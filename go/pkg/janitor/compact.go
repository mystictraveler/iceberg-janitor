package janitor

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergconfig "github.com/apache/iceberg-go/config"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// applyIcebergMaxWorkersOverride sets iceberg-go's package-level
// config.EnvConfig.MaxWorkers from JANITOR_ICEBERG_MAX_WORKERS, if set.
// This is the diagnostic switch for github.com/mystictraveler/iceberg-janitor#4
// (master check fails under concurrent writes — suspected
// partitionedFanoutWriter race). Setting this to 1 forces single-worker
// fanout. If the master check failures vanish at MaxWorkers=1 but
// reappear at MaxWorkers=5 (the iceberg-go default), the writer race
// is confirmed and we file upstream against apache/iceberg-go.
//
// EnvConfig is a package-level var with no exported setter, so we
// mutate it directly. We do this once per process under a sync.Once
// because EnvConfig has no concurrent-write protection of its own.
var icebergMaxWorkersOnce sync.Once

func applyIcebergMaxWorkersOverride() {
	icebergMaxWorkersOnce.Do(func() {
		v := os.Getenv("JANITOR_ICEBERG_MAX_WORKERS")
		if v == "" {
			return
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return
		}
		icebergconfig.EnvConfig.MaxWorkers = n
	})
}

// IsRetryableConcurrencyError reports whether `err` represents an
// optimistic-concurrency conflict that the caller should respond to by
// reloading the table and retrying from scratch (rather than treating
// as a hard failure).
//
// There are TWO layers at which a janitor commit can lose a
// concurrent-write race, and both should drive a retry:
//
//  1. Our directory catalog's per-key conditional write fails with
//     catalog.ErrCASConflict because another writer committed
//     v(N+1).metadata.json before us. This is the case our retry loop
//     was originally designed for.
//
//  2. iceberg-go's Transaction.Commit calls Requirement.Validate
//     against the current metadata BEFORE our DirectoryCatalog.
//     CommitTable is even invoked. If the streamer commits between our
//     stage and our commit, the staged transaction's branch-snapshot
//     requirement no longer matches and validation fails with a
//     "requirement failed: branch main has changed: expected id X,
//     found Y" error (and several similar variants for other
//     requirement types — schema id changed, last assigned field id
//     changed, default spec id changed, etc).
//
// The iceberg-go errors are plain fmt.Errorf strings with no
// exported sentinel, so we string-match on the consistent
// "has changed" substring. All six requirement-validation failure
// messages in iceberg-go's table/requirements.go contain it (case
// varies, so we lowercase first). The match is intentionally narrow:
// we only retry on concurrency-conflict errors, NOT on other
// requirement failures like assertTableUuid mismatches (which would
// indicate a programming bug, not a race).
//
// This helper is the fix for github.com/mystictraveler/iceberg-janitor#1.
func IsRetryableConcurrencyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, catalog.ErrCASConflict) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "has changed") &&
		(strings.Contains(msg, "requirement failed") || strings.Contains(msg, "requirement validation failed")) {
		return true
	}
	return false
}

// CompactOptions configure a single Compact call.
type CompactOptions struct {
	// MaxAttempts caps the CAS-conflict retry loop. Default 5.
	MaxAttempts int
	// InitialBackoff is the wait between the first failed attempt and
	// the second. Doubles each time. Default 100ms.
	InitialBackoff time.Duration
	// RowFilter, if non-nil, scopes the compaction to only the rows
	// (and therefore only the data files) matching the predicate.
	// This is the smaller-scope-per-compaction primitive for the
	// writer-fight pathology: instead of compacting the whole table
	// in one big atomic transaction (which races the streamer for
	// every file the streamer wrote since we started planning), we
	// compact one partition's worth of files in a smaller transaction
	// that only conflicts with the streamer's commits to that one
	// partition.
	//
	// The filter MUST align with a partition predicate of the table
	// — e.g. iceberg.EqualTo(iceberg.Reference("ss_store_bucket"), 5).
	// Iceberg's overwrite-with-filter semantics:
	//   - Files where ALL rows match the filter are completely deleted
	//   - Files where SOME rows match are rewritten to keep only
	//     non-matching rows
	//   - Files where NO rows match are kept unchanged
	// For partition predicates, all rows in a file have the same
	// partition value, so each file is either fully matched or fully
	// non-matched and the rewrite-partial path doesn't fire.
	//
	// nil means "compact the whole table" (the previous behavior).
	RowFilter icebergpkg.BooleanExpression

	// PartitionTuple is the parquet-go-path equivalent of RowFilter.
	// The CLI sets BOTH from a single --partition col=value flag:
	// RowFilter is the iceberg.BooleanExpression form (used by the
	// legacy compactOnce path that still calls iceberg-go's scan
	// API), PartitionTuple is the bare map form (used by
	// compactOnceReplace, which walks manifests directly without
	// going through iceberg-go's scan / classifier and so doesn't
	// want a BooleanExpression to walk). Both must agree if both are
	// set; both being nil means "whole table".
	//
	// Map key is the schema column name (e.g. "ss_store_sk"); value
	// is the typed Go value (int64 for the int columns we currently
	// support). compactOnceReplace looks up the column's iceberg
	// field id from the table schema, then matches against
	// DataFile.Partition()[partition_field_id].
	PartitionTuple map[string]any
}

func (o *CompactOptions) defaults() {
	if o.MaxAttempts <= 0 {
		// 15 attempts at exponential backoff (100ms doubling) gives
		// us ~50s of patience under writer-fight, which is enough
		// for the streamer to leave a quiet window between commit
		// bursts on the partition we're touching. Was 5 originally;
		// bumped after the parquet-go-direct compaction path
		// eliminated the row-loss bug and surfaced retry exhaustion
		// as the next-largest failure mode under streaming load.
		o.MaxAttempts = 15
	}
	if o.InitialBackoff <= 0 {
		o.InitialBackoff = 100 * time.Millisecond
	}
}

// CompactResult is the structured outcome of a successful Compact call.
// On failure the function returns an error and a partial result with
// whatever was filled in before the failure.
type CompactResult struct {
	Identifier  icebergtable.Identifier `json:"identifier"`
	Attempts    int                     `json:"attempts"`

	BeforeSnapshotID int64 `json:"before_snapshot_id"`
	BeforeFiles      int   `json:"before_files"`
	BeforeBytes      int64 `json:"before_bytes"`
	BeforeRows      int64 `json:"before_rows"`

	AfterSnapshotID int64 `json:"after_snapshot_id"`
	AfterFiles      int   `json:"after_files"`
	AfterBytes      int64 `json:"after_bytes"`
	AfterRows      int64 `json:"after_rows"`

	Verification *safety.Verification `json:"verification"`
	DurationMs   int64                `json:"duration_ms"`
}

// Compact reads all data from the table, rewrites it via iceberg-go's
// transactional Overwrite, runs the mandatory pre-commit master check
// (I1 row count, I2 schema, I7 manifest references), and commits via
// the directory catalog's atomic CAS.
//
// On a CAS conflict (another writer beat us to v(N+1).metadata.json),
// Compact reloads the table and retries up to MaxAttempts times with
// exponential backoff. This is the optimistic-concurrency model the
// design assumes.
//
// This is the shared implementation called by both cmd/janitor-cli
// (in-process) and cmd/janitor-server (HTTP handler). Both binaries
// reuse the same code path so the master check, CAS retry, and
// reporting are identical regardless of how the request arrived.
//
// MVP scope: full read + full overwrite via streaming RecordReader.
// No partition scoping, no incremental work, no stitching binpack.
// True stitching (column-chunk byte copy) lands later.
func Compact(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions) (*CompactResult, error) {
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "Compact")
	span.SetAttributes(observe.Table(ident[0], ident[1]))
	defer span.End()

	opts.defaults()
	applyIcebergMaxWorkersOverride()
	started := time.Now()
	result := &CompactResult{Identifier: ident}

	// Dispatch: JANITOR_COMPACT_USE_REPLACE=1 selects the architectural
	// fix path (compactOnceReplace) for #4 / apache/iceberg-go#860.
	// Default still uses the legacy compactOnce path so existing
	// callers see no change until the new path is proven under load.
	useReplace := os.Getenv("JANITOR_COMPACT_USE_REPLACE") == "1"
	once := compactOnce
	if useReplace {
		once = compactOnceReplace
	}

	backoff := opts.InitialBackoff
	for attempt := 1; attempt <= opts.MaxAttempts; attempt++ {
		result.Attempts = attempt
		err := once(ctx, cat, ident, opts, result)
		if err == nil {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, nil
		}
		// Retry on either layer of optimistic-concurrency conflict:
		// our directory catalog's CAS or iceberg-go's requirement
		// validation. See IsRetryableConcurrencyError for details.
		if !IsRetryableConcurrencyError(err) {
			result.DurationMs = time.Since(started).Milliseconds()
			return result, err
		}
		select {
		case <-ctx.Done():
			result.DurationMs = time.Since(started).Milliseconds()
			return result, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	result.DurationMs = time.Since(started).Milliseconds()
	return result, fmt.Errorf("compaction failed: exceeded %d concurrency-retry attempts", opts.MaxAttempts)
}

// compactOnce performs a single compaction attempt. It loads the table
// fresh, opens a transaction, stages a streaming overwrite, runs the
// master check, and commits. The before/after counts are written into
// `result` so the caller has structured stats even on failure.
//
// If opts.RowFilter is non-nil, the scan and the overwrite both use it
// — the staging set is restricted to rows / files matching the filter.
// This is the partition-scoped compaction code path.
func compactOnce(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions, result *CompactResult) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	// Guard: row-filter scoping is only safe on partitioned tables.
	// Iceberg's overwrite-with-filter semantics rely on each data file
	// being either fully matched or fully non-matched by the filter,
	// which holds for partition predicates but NOT for row predicates
	// against an unpartitioned table. On an unpartitioned table, every
	// file would contain a mix of matching and non-matching rows, so
	// iceberg-go would fall into the partial-rewrite path and rewrite
	// EVERY file in the table — strictly worse than a plain whole-table
	// compaction (we'd pay the I/O of rewriting non-matching rows for
	// no benefit). The master check would still pass — it's not a
	// correctness bug, it's a wasted-work bug — but the whole point
	// of partition-scoped compaction is to do LESS work, so reject
	// the request loudly rather than silently doing more.
	//
	// We don't try to validate that the filter column actually aligns
	// with a partition field. That's a janitor-internal interface;
	// the operator who passes --partition is expected to know the
	// table's partition spec. The unpartitioned check catches the
	// most common footgun (forgetting that the table isn't partitioned
	// at all).
	if opts.RowFilter != nil {
		if spec := tbl.Spec(); spec.NumFields() == 0 {
			return fmt.Errorf("partition-scoped compaction requested but table %v is unpartitioned; pass no --partition filter to compact the whole table", ident)
		}
	}

	if snap := tbl.CurrentSnapshot(); snap != nil {
		result.BeforeSnapshotID = snap.SnapshotID
	}
	result.BeforeFiles, result.BeforeBytes, result.BeforeRows = SnapshotFileStats(ctx, tbl)

	// Build the scan, applying the row filter if one was provided.
	// iceberg-go's Scan handles partition pruning at PlanFiles time
	// — files whose partition values don't satisfy the predicate are
	// not even read.
	var scanOpts []icebergtable.ScanOption
	if opts.RowFilter != nil {
		scanOpts = append(scanOpts, icebergtable.WithRowFilter(opts.RowFilter))
	}
	scan := tbl.Scan(scanOpts...)
	scanSchema, recordIter, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return fmt.Errorf("opening scan: %w", err)
	}
	reader := newStreamingRecordReader(scanSchema, recordIter)
	defer reader.Release()

	// Build the overwrite, applying the same row filter so iceberg-go's
	// commit machinery deletes only the files in the matching partition
	// rather than every data file in the table.
	var owOpts []icebergtable.OverwriteOption
	if opts.RowFilter != nil {
		owOpts = append(owOpts, icebergtable.WithOverwriteFilter(opts.RowFilter))
	}
	tx := tbl.NewTransaction()
	if err := tx.Overwrite(ctx, reader, nil, owOpts...); err != nil {
		return fmt.Errorf("staging streaming overwrite: %w", err)
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("scan iterator error: %w", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		return fmt.Errorf("getting staged table: %w", err)
	}

	// Mandatory pre-commit master check. Non-bypassable.
	verification, err := safety.VerifyCompactionConsistency(ctx, tbl, staged, cat.Props())
	result.Verification = verification
	if err != nil {
		return err
	}

	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return err
	}

	if snap := newTbl.CurrentSnapshot(); snap != nil {
		result.AfterSnapshotID = snap.SnapshotID
	}
	result.AfterFiles, result.AfterBytes, result.AfterRows = SnapshotFileStats(ctx, newTbl)
	if result.AfterRows != result.BeforeRows {
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", result.BeforeRows, result.AfterRows)
	}
	return nil
}

// SnapshotFileStats walks the current snapshot's manifests and returns
// (data file count, total bytes, total rows). Exported because both
// the CLI and the server need it for reporting.
func SnapshotFileStats(ctx context.Context, tbl *icebergtable.Table) (files int, bytes int64, rows int64) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, 0
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return 0, 0, 0
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, 0, 0
	}
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			continue
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			continue
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			files++
			bytes += df.FileSizeBytes()
			rows += df.Count()
		}
	}
	return files, bytes, rows
}

// streamingRecordReader adapts an iter.Seq2[arrow.RecordBatch, error]
// (the streaming scan iterator iceberg-go returns from ToArrowRecords)
// into the array.RecordReader interface that Transaction.Overwrite
// expects. It pulls one batch at a time via iter.Pull2 — no buffering,
// no whole-table-in-memory.
//
// Moved here from cmd/janitor-cli so both the CLI and the server use
// the same memory-bounded streaming compaction code path.
type streamingRecordReader struct {
	schema  *arrow.Schema
	next    func() (arrow.RecordBatch, error, bool)
	stop    func()
	current arrow.RecordBatch
	err     error
	refs    int64
}

func newStreamingRecordReader(schema *arrow.Schema, seq iter.Seq2[arrow.RecordBatch, error]) *streamingRecordReader {
	next, stop := iter.Pull2(seq)
	return &streamingRecordReader{schema: schema, next: next, stop: stop, refs: 1}
}

func (r *streamingRecordReader) Schema() *arrow.Schema      { return r.schema }
func (r *streamingRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *streamingRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *streamingRecordReader) Err() error                     { return r.err }

func (r *streamingRecordReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	rec, err, ok := r.next()
	if !ok {
		return false
	}
	if err != nil {
		r.err = err
		return false
	}
	r.current = rec
	return true
}

func (r *streamingRecordReader) Retain() { r.refs++ }

func (r *streamingRecordReader) Release() {
	r.refs--
	if r.refs > 0 {
		return
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.stop != nil {
		r.stop()
		r.stop = nil
	}
}
