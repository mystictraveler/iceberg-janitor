package janitor

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// stitchedPartition is the per-partition output of the parallel
// stitch phase. The CompactHot batched commit collects N of these
// and stages them in a single Transaction.
type stitchedPartition struct {
	partitionKey string
	anchor       string
	bootstrap    bool
	smallCount   int
	oldPaths     []string
	newFilePath  string
	rowsWritten  int64
	durationMs   int64
}

// stitchPartitionToFile is the stitch-only helper that the parallel
// CompactHot worker calls. It mirrors the stitch+fallback logic of
// executeStitchAndCommit's first half (lines ~360-477) but stops
// before the ReplaceDataFiles + master check + commit. The caller
// (CompactHot) collects N stitchedPartition results and stages them
// all in one Transaction.
//
// On success returns the new file path and rows written. On failure
// removes any partial output and returns the error.
func stitchPartitionToFile(ctx context.Context, tbl *icebergtable.Table, fs icebergio.IO, oldPaths []string, expectedRows int64) (string, int64, error) {
	icebergSchema := tbl.Metadata().CurrentSchema()
	locProv, err := tbl.LocationProvider()
	if err != nil {
		return "", 0, fmt.Errorf("getting location provider: %w", err)
	}
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return "", 0, fmt.Errorf("filesystem does not support writes")
	}
	newFileName := fmt.Sprintf("janitor-%s.parquet", uuid.New().String())
	newFilePath := locProv.NewDataLocation(newFileName)

	writeSchema, err := icebergtable.SchemaToArrowSchema(icebergSchema, nil, false, false)
	if err != nil {
		return "", 0, fmt.Errorf("building arrow write schema: %w", err)
	}

	out, err := wfs.Create(newFilePath)
	if err != nil {
		return "", 0, fmt.Errorf("creating output file %s: %w", newFilePath, err)
	}

	// Fast path: parquet-go byte-copy stitch.
	rowsWritten, stitchErr := stitchParquetFiles(ctx, fs, oldPaths, out)
	usedStitch := stitchErr == nil && rowsWritten == expectedRows
	if !usedStitch {
		// Fallback to pqarrow decode/encode path.
		_ = out.Close()
		_ = wfs.Remove(newFilePath)
		out, err = wfs.Create(newFilePath)
		if err != nil {
			return "", 0, fmt.Errorf("creating output file %s for fallback: %w", newFilePath, err)
		}
		rowsWritten = 0
		pqProps := parquet.NewWriterProperties(parquet.WithStats(true))
		pqWriter, perr := pqarrow.NewFileWriter(writeSchema, out, pqProps, pqarrow.DefaultWriterProps())
		if perr != nil {
			_ = wfs.Remove(newFilePath)
			return "", 0, fmt.Errorf("creating parquet writer: %w", perr)
		}
		mem := memory.DefaultAllocator
		var pqWriterMu sync.Mutex
		g2, gctx2 := errgroup.WithContext(ctx)
		g2.SetLimit(parquetReadConcurrency)
		for _, fpath := range oldPaths {
			fpath := fpath
			g2.Go(func() error {
				if gctx2.Err() != nil {
					return gctx2.Err()
				}
				batches, n, rerr := readParquetFileBatches(gctx2, fs, fpath, writeSchema, mem)
				if rerr != nil {
					return fmt.Errorf("copying %s: %w", fpath, rerr)
				}
				pqWriterMu.Lock()
				defer pqWriterMu.Unlock()
				for i, b := range batches {
					if werr := pqWriter.Write(b); werr != nil {
						for j := i; j < len(batches); j++ {
							batches[j].Release()
						}
						return fmt.Errorf("write %s: %w", fpath, werr)
					}
					b.Release()
				}
				atomic.AddInt64(&rowsWritten, n)
				return nil
			})
		}
		if err := g2.Wait(); err != nil {
			_ = pqWriter.Close()
			_ = wfs.Remove(newFilePath)
			return "", 0, err
		}
		if err := pqWriter.Close(); err != nil {
			_ = wfs.Remove(newFilePath)
			return "", 0, fmt.Errorf("closing parquet writer: %w", err)
		}
	} else {
		if cerr := out.Close(); cerr != nil {
			_ = wfs.Remove(newFilePath)
			return "", 0, fmt.Errorf("closing stitched output %s: %w", newFilePath, cerr)
		}
	}

	if rowsWritten != expectedRows {
		_ = wfs.Remove(newFilePath)
		return "", 0, fmt.Errorf("read/manifest row mismatch: read %d rows but expected %d", rowsWritten, expectedRows)
	}

	// Post-stitch row group merge check. Byte-copy stitch preserves
	// row group boundaries: N source files → N row groups in one
	// output file. Query engines like Athena/Trino process each row
	// group as a separate scan unit, so many tiny row groups are
	// WORSE than fewer files with many row groups. Run 19 proved
	// this: 2,500 row groups (50 files × 50 RGs) was 46-111% slower
	// on Athena than 200 uncompacted files with 1 RG each.
	//
	// If the stitched output has > maxRowGroupsPerFile row groups,
	// re-read it via pqarrow and rewrite with merged row groups.
	// This is the decode/encode path (slower, ~3-10× more CPU) but
	// produces query-optimal output. The cost is paid once per
	// compaction; every query benefits for the lifetime of the file.
	if usedStitch {
		mergedPath, mergedRows, merr := maybeMergeRowGroups(ctx, tbl, fs, wfs, newFilePath, rowsWritten, writeSchema, locProv)
		if merr == nil && mergedPath != "" {
			// Merge succeeded — swap the output path.
			_ = wfs.Remove(newFilePath)
			newFilePath = mergedPath
			rowsWritten = mergedRows
		}
		// If merge failed or wasn't needed, keep the stitched output.
	}

	return newFilePath, rowsWritten, nil
}

// defaultHotPartitionConcurrency is the default fan-out for the
// per-partition stitch workers. It is intentionally modest because
// each worker nests into the stitch file-open loop, which is itself
// parallel (see pkg/janitor/stitch.go). The total concurrent parquet
// opens is defaultHotPartitionConcurrency × stitch.openConcurrency,
// and that product lands on the HTTP transport as simultaneous S3
// GetObject range reads. Run MinIO-bench #3 saturated the client
// connection pool at 16 × 32 = 512 in-flight opens and parked every
// stitch goroutine in net/http/transport.go for 6+ minutes. With
// 4 × 8 = 32 the same workload finishes cleanly.
//
// Operators on beefy S3 can raise this via CompactHotOptions.
// PartitionConcurrency.
const defaultHotPartitionConcurrency = 4

// CompactHotOptions configures the hot-loop entry point.
type CompactHotOptions struct {
	// SmallFileThresholdBytes — files <= this byte count are eligible
	// for inclusion in a delta-stitch round.
	SmallFileThresholdBytes int64

	// MaintainInterval — how frequently the hot loop is invoked.
	// Used as the rotation period for the time-based round-robin
	// anchor selection: epoch_minute / interval_minutes determines
	// the rotation index, and (rotation_index + hash(partition_key))
	// % len(large_files) picks the anchor for this round. The result
	// is that successive hot rounds touch different large files in
	// the same partition, distributing wear and avoiding the case
	// where one giant file keeps absorbing the small-file tail while
	// other large files in the partition stay static.
	MaintainInterval time.Duration

	// HotWindowSnapshots — only partitions whose
	// MaxSequenceNumber is within the last N snapshot sequence
	// numbers are touched. Cold partitions are skipped (cold loop
	// handles them).
	HotWindowSnapshots int

	// MinSmallFiles — skip partitions with fewer small files than
	// this. Default 2 (no point stitching one file).
	MinSmallFiles int

	// PartitionConcurrency — max number of partitions stitched in
	// parallel. Each worker nests into stitch.go's parallel file-open
	// loop, so the effective concurrent S3 GetObject count is
	// PartitionConcurrency × stitch inner open limit. Default 4.
	// Raise on beefy S3 backends with large connection pools.
	PartitionConcurrency int

	// DryRun, when true, runs the full partition analysis and builds
	// the stitch work list, then STOPS before any stitch I/O or
	// commit. The result reports per-partition projections and a
	// top-level ContentionDetected flag computed by reloading the
	// table after planning. See CompactOptions.DryRun for the
	// rationale.
	DryRun bool
}

func (o *CompactHotOptions) defaults() {
	if o.SmallFileThresholdBytes <= 0 {
		o.SmallFileThresholdBytes = 64 * 1024 * 1024
	}
	if o.MaintainInterval <= 0 {
		o.MaintainInterval = 5 * time.Minute
	}
	if o.HotWindowSnapshots <= 0 {
		o.HotWindowSnapshots = 5
	}
	if o.MinSmallFiles <= 0 {
		o.MinSmallFiles = 2
	}
	if o.PartitionConcurrency <= 0 {
		o.PartitionConcurrency = defaultHotPartitionConcurrency
	}
}

// CompactHotResult is the per-partition outcome of a hot-loop round.
type CompactHotResult struct {
	Identifier         icebergtable.Identifier `json:"identifier"`
	PartitionsAnalyzed int                     `json:"partitions_analyzed"`
	PartitionsHot      int                     `json:"partitions_hot"`
	PartitionsStitched int                     `json:"partitions_stitched"`
	PartitionsSkipped  int                     `json:"partitions_skipped"`
	PartitionsFailed   int                     `json:"partitions_failed"`
	TotalDurationMs    int64                   `json:"total_duration_ms"`
	Partitions         []HotPartitionResult    `json:"partitions"`

	// DryRun, ContentionDetected, PartitionsPlanned: set only when
	// CompactHotOptions.DryRun is true. PartitionsPlanned counts the
	// partitions that would have been stitched (same set the Partitions
	// slice describes, with Stitched=false and DryRun=true on each
	// entry). ContentionDetected reports whether the table's current
	// snapshot id advanced during the planning phase.
	DryRun             bool  `json:"dry_run,omitempty"`
	ContentionDetected bool  `json:"contention_detected,omitempty"`
	PartitionsPlanned  int   `json:"partitions_planned,omitempty"`
	ObservedSnapshotID int64 `json:"observed_snapshot_id,omitempty"`
}

type HotPartitionResult struct {
	PartitionKey  string `json:"partition_key"`
	Anchor        string `json:"anchor"`
	SmallCount    int    `json:"small_count"`
	Bootstrap     bool   `json:"bootstrap"` // true if no large files existed
	Stitched      bool   `json:"stitched"`
	Skipped       bool   `json:"skipped,omitempty"`
	SkippedReason string `json:"skipped_reason,omitempty"`
	Error         string `json:"error,omitempty"`
	DurationMs    int64  `json:"duration_ms"`
	BeforeFiles   int    `json:"before_files"`
	AfterFiles    int    `json:"after_files"`
	// DryRun: true when this partition was part of a dry-run plan —
	// the stitch input files were selected but nothing was written
	// or committed.
	DryRun bool `json:"dry_run,omitempty"`
}

// CompactHot runs the hot-loop maintenance pass on the table:
//
//  1. Analyze partitions to find the hot ones (those whose
//     MaxSequenceNumber falls inside the hot window).
//  2. For each hot partition with at least MinSmallFiles small files:
//     a. Pick an "anchor" file via time-based round-robin over the
//        partition's large files. If no large files exist, use the
//        biggest small file as the anchor (bootstrap).
//     b. Build the stitch input set: anchor + all small files in the
//        partition (excluding the anchor itself if it was a small).
//     c. Call Compact with OldPathsOverride set to that input set.
//        compactOnce's fast path skips the manifest walk and stitches
//        the given files directly.
//
// Each partition is processed independently. A failure on one partition
// does not stop the others — the result aggregates per-partition
// outcomes. The function returns a non-nil error only if the analyzer
// itself fails.
func CompactHot(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactHotOptions) (*CompactHotResult, error) {
	opts.defaults()
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "CompactHot")
	span.SetAttributes(observe.Table(ident[0], ident[1]))
	defer span.End()

	started := time.Now()
	result := &CompactHotResult{Identifier: ident}

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return result, fmt.Errorf("loading table %v: %w", ident, err)
	}

	parts, err := analyzer.AnalyzePartitions(ctx, tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: opts.SmallFileThresholdBytes,
		HotWindowSnapshots:      opts.HotWindowSnapshots,
	})
	if err != nil {
		return result, fmt.Errorf("analyzing partitions: %w", err)
	}
	result.PartitionsAnalyzed = len(parts)

	// Time-based rotation index. epoch_minutes / interval_minutes
	// gives a rotation that advances each maintain cycle and is
	// stable across multiple invocations within the same window.
	intervalMin := int64(opts.MaintainInterval / time.Minute)
	if intervalMin < 1 {
		intervalMin = 1
	}
	rotationIndex := time.Now().Unix() / 60 / intervalMin

	// Build the parallel work list. Each entry is a (partition,
	// stitchInputs, expectedRows) tuple. The planning phase (which
	// only reads PartitionHealth and is cheap) is sequential; the
	// stitch phase (parquet read + write) is parallel; the commit
	// phase (one Transaction with all partition replacements) is
	// sequential at the end.
	type stitchWork struct {
		p             analyzer.PartitionHealth
		anchor        analyzer.FileInfo
		bootstrap     bool
		inputs        []string
		expectedRows  int64
	}
	work := make([]stitchWork, 0, len(parts))
	for _, p := range parts {
		if !p.IsHot {
			continue
		}
		result.PartitionsHot++

		if p.SmallFileCount < opts.MinSmallFiles {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				SmallCount:    p.SmallFileCount,
				Skipped:       true,
				SkippedReason: fmt.Sprintf("only %d small files (min %d)", p.SmallFileCount, opts.MinSmallFiles),
			})
			continue
		}

		// Pick the anchor.
		anchor, bootstrap := pickAnchor(p, rotationIndex)
		if anchor.Path == "" {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				SmallCount:    p.SmallFileCount,
				Skipped:       true,
				SkippedReason: "no anchor candidate",
			})
			continue
		}

		// Build the stitch input: anchor + all small files (excluding
		// anchor if it was already in the small set during bootstrap).
		stitchInputs := []string{anchor.Path}
		for _, sf := range p.SmallFiles {
			if sf.Path == anchor.Path {
				continue
			}
			stitchInputs = append(stitchInputs, sf.Path)
		}
		if len(stitchInputs) < 2 {
			result.PartitionsSkipped++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey:  p.PartitionKey,
				Anchor:        anchor.Path,
				SmallCount:    p.SmallFileCount,
				Bootstrap:     bootstrap,
				Skipped:       true,
				SkippedReason: "stitch set has fewer than 2 inputs",
			})
			continue
		}

		// Compute expected total rows for this partition's stitch
		// input set from the analyzer's per-file row counts. Used
		// by stitchPartitionToFile's pre-flight row check.
		var expectedRows int64
		for _, fi := range p.Files {
			for _, sp := range stitchInputs {
				if fi.Path == sp {
					expectedRows += fi.Rows
					break
				}
			}
		}

		work = append(work, stitchWork{
			p:            p,
			anchor:       anchor,
			bootstrap:    bootstrap,
			inputs:       stitchInputs,
			expectedRows: expectedRows,
		})
	}

	// Dry-run cut point. All of the analysis work is done: we have the
	// full partition health, the hot filter, and the per-partition
	// stitch plan. Stop here before any file I/O. Emit one
	// HotPartitionResult per planned partition with DryRun=true and
	// Stitched=false, then probe for contention by reloading the
	// table. See CompactOptions.DryRun for the rationale.
	if opts.DryRun {
		result.DryRun = true
		result.PartitionsPlanned = len(work)
		for _, w := range work {
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey: w.p.PartitionKey,
				Anchor:       w.anchor.Path,
				SmallCount:   w.p.SmallFileCount,
				Bootstrap:    w.bootstrap,
				BeforeFiles:  len(w.inputs),
				AfterFiles:   1,
				DryRun:       true,
			})
		}
		if snap := tbl.CurrentSnapshot(); snap != nil {
			result.ObservedSnapshotID = snap.SnapshotID
		}
		if probe, perr := cat.LoadTable(ctx, ident); perr == nil {
			if cur := probe.CurrentSnapshot(); cur != nil && cur.SnapshotID != result.ObservedSnapshotID {
				result.ContentionDetected = true
			}
		}
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, nil
	}

	// === Parallel partition stitch (no commit) ===
	//
	// Each worker stitches ONE partition's source files into a new
	// parquet file. No commit, no CAS, no master check yet. The
	// parallel speedup comes from overlapping the stitch I/O
	// (read sources, write new file) across partitions, which on
	// S3 is the dominant cost. With PartitionConcurrency=4 (default)
	// and ~50 partitions, the wall time goes from 50 × stitch_time to
	// roughly ceil(50/4) × stitch_time ≈ 12-13 × stitch_time.
	// Operators on beefy S3 can raise PartitionConcurrency.
	//
	// Per-worker output is a stitchedPartition with the new file
	// path, the old paths it consumed, and the row count. The
	// commit phase below assembles all of these into a single
	// Transaction and commits ONCE.
	fs, err := tbl.FS(ctx)
	if err != nil {
		return result, fmt.Errorf("getting fs: %w", err)
	}
	stitched := make([]stitchedPartition, len(work))
	stitchErrs := make([]error, len(work))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.PartitionConcurrency)
	for i, w := range work {
		i, w := i, w
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			partStarted := time.Now()
			newFilePath, rowsWritten, serr := stitchPartitionToFile(gctx, tbl, fs, w.inputs, w.expectedRows)
			stitched[i] = stitchedPartition{
				partitionKey: w.p.PartitionKey,
				anchor:       w.anchor.Path,
				bootstrap:    w.bootstrap,
				smallCount:   w.p.SmallFileCount,
				oldPaths:     w.inputs,
				newFilePath:  newFilePath,
				rowsWritten:  rowsWritten,
				durationMs:   time.Since(partStarted).Milliseconds(),
			}
			if serr != nil {
				stitchErrs[i] = serr
			}
			return nil // never bubble per-partition stitch errors — record per-partition
		})
	}
	if err := g.Wait(); err != nil && !isContextCancelled(err) {
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, fmt.Errorf("CompactHot stitch worker: %w", err)
	}

	// Collect successful stitches into the batched-commit input.
	// Per-partition stitch failures are recorded in the result and
	// excluded from the commit. A worker that produced a new file
	// AND had a stitchErr is impossible (the helper either returns
	// (path, rows, nil) or (\"\", 0, err)) so the path-empty check
	// is sufficient.
	var batchOldPaths []string
	var batchNewPaths []string
	var batchRows int64
	for i, sp := range stitched {
		if stitchErrs[i] != nil || sp.newFilePath == "" {
			result.PartitionsFailed++
			result.Partitions = append(result.Partitions, HotPartitionResult{
				PartitionKey: sp.partitionKey,
				Anchor:       sp.anchor,
				SmallCount:   sp.smallCount,
				Bootstrap:    sp.bootstrap,
				DurationMs:   sp.durationMs,
				Error:        errString(stitchErrs[i]),
			})
			continue
		}
		batchOldPaths = append(batchOldPaths, sp.oldPaths...)
		batchNewPaths = append(batchNewPaths, sp.newFilePath)
		batchRows += sp.rowsWritten
	}

	if len(batchNewPaths) == 0 {
		// Nothing to commit — every partition was skipped or
		// failed. Done.
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, nil
	}

	// === Batched commit ===
	//
	// Build ONE Transaction with ALL partition replacements,
	// run the master check ONCE on the staged table, then CAS
	// commit ONCE. On CAS conflict (a foreign writer commited
	// between our LoadTable and our commit), reload the table
	// and retry — but DO NOT re-stitch. The new files are already
	// on disk and idempotent at the file level.
	//
	// 15 retry attempts with exponential backoff (capped at 5s)
	// matches the existing single-partition Compact() retry
	// shape. Persistent CAS conflict beyond that bubbles up as a
	// hard failure.
	const maxAttempts = 15
	const initialBackoff = 100 * time.Millisecond
	const maxBackoff = 5 * time.Second
	commitStarted := time.Now()
	backoff := initialBackoff
	var commitErr error
	var verification *safety.Verification
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		freshTbl, lerr := cat.LoadTable(ctx, ident)
		if lerr != nil {
			commitErr = fmt.Errorf("reloading table for batched commit: %w", lerr)
			break
		}
		tx := freshTbl.NewTransaction()
		if rerr := tx.ReplaceDataFiles(ctx, batchOldPaths, batchNewPaths, nil); rerr != nil {
			commitErr = fmt.Errorf("staging batched ReplaceDataFiles (%d old, %d new): %w",
				len(batchOldPaths), len(batchNewPaths), rerr)
			break
		}
		staged, serr := tx.StagedTable()
		if serr != nil {
			commitErr = fmt.Errorf("getting staged table for batched commit: %w", serr)
			break
		}
		v, verr := safety.VerifyCompactionConsistency(ctx, freshTbl, staged, cat.Props())
		verification = v
		if verr != nil {
			commitErr = fmt.Errorf("master check failed on batched stage: %w", verr)
			break
		}
		_, cerr := tx.Commit(ctx)
		if cerr == nil {
			commitErr = nil
			break
		}
		if !IsRetryableConcurrencyError(cerr) {
			commitErr = cerr
			break
		}
		// Retryable: sleep + try again
		commitErr = cerr
		select {
		case <-ctx.Done():
			commitErr = ctx.Err()
			break
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
	commitElapsedMs := time.Since(commitStarted).Milliseconds()

	// Record per-partition results based on the commit outcome.
	// All partitions in the batch share the same commit result —
	// either all succeeded together or all failed together.
	for _, sp := range stitched {
		if sp.newFilePath == "" {
			continue // already recorded as failed above
		}
		pr := HotPartitionResult{
			PartitionKey: sp.partitionKey,
			Anchor:       sp.anchor,
			SmallCount:   sp.smallCount,
			Bootstrap:    sp.bootstrap,
			DurationMs:   sp.durationMs + commitElapsedMs/int64(len(batchNewPaths)), // amortized
		}
		if commitErr != nil {
			result.PartitionsFailed++
			pr.Error = commitErr.Error()
		} else {
			result.PartitionsStitched++
			pr.Stitched = true
		}
		result.Partitions = append(result.Partitions, pr)
	}

	if commitErr != nil {
		// Commit failed. Best-effort cleanup of the orphan stitched
		// files — orphan-removal will catch any we miss.
		wfs, ok := fs.(icebergio.WriteFileIO)
		if ok {
			for _, np := range batchNewPaths {
				_ = wfs.Remove(np)
			}
		}
		result.TotalDurationMs = time.Since(started).Milliseconds()
		return result, fmt.Errorf("batched commit failed after retries: %w (master check verification: %+v)", commitErr, verification)
	}

	result.TotalDurationMs = time.Since(started).Milliseconds()
	return result, nil
}

// errString returns err.Error() or empty string if err is nil. Used
// by the result aggregation to keep the per-partition Error field
// empty when stitch+commit succeeded.
func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// isContextCancelled reports whether err is a benign context
// cancellation (Cancelled or DeadlineExceeded) versus a real error.
// Used by CompactHot's errgroup wait to distinguish "we were
// asked to stop" from "a worker hit something unexpected".
func isContextCancelled(err error) bool {
	if err == nil {
		return false
	}
	return err == context.Canceled || err == context.DeadlineExceeded
}

// pickAnchor selects the anchor file for a partition's delta-stitch
// round. Strategy:
//
//   - If the partition has any large files, time-based round-robin
//     picks one of them: large_files[(rotationIndex + hash(key)) % N].
//     This distributes the stitch wear across all large files in the
//     partition rather than always growing the same one.
//
//   - If the partition has NO large files (typical of a freshly-seeded
//     partition or one whose janitor has never run), bootstrap by
//     picking the largest small file as the anchor. After the first
//     stitch round, the result becomes the partition's first large
//     file and the round-robin path engages from then on.
//
// Returns the picked file and a bootstrap flag for telemetry.
func pickAnchor(p analyzer.PartitionHealth, rotationIndex int64) (analyzer.FileInfo, bool) {
	if len(p.LargeFiles) > 0 {
		// Stable sort by path so the rotation is deterministic across
		// runs even if the analyzer's manifest walk produces different
		// orderings.
		large := append([]analyzer.FileInfo(nil), p.LargeFiles...)
		sort.Slice(large, func(i, j int) bool {
			return large[i].Path < large[j].Path
		})
		idx := (rotationIndex + hashKey(p.PartitionKey)) % int64(len(large))
		if idx < 0 {
			idx += int64(len(large))
		}
		return large[idx], false
	}
	if len(p.SmallFiles) == 0 {
		return analyzer.FileInfo{}, false
	}
	// Bootstrap: pick the biggest small file.
	biggest := p.SmallFiles[0]
	for _, f := range p.SmallFiles[1:] {
		if f.Bytes > biggest.Bytes {
			biggest = f
		}
	}
	return biggest, true
}

func hashKey(s string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return int64(h.Sum64() & 0x7fffffffffffffff)
}
