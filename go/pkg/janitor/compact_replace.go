package janitor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"go.opentelemetry.io/otel/attribute"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/observe"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// manifestReadConcurrency is the bounded-fan-out limit for parallel
// manifest avro reads in compactOnce. 32 was chosen as a safe default
// for HTTP/2 multiplexing: gocloud's S3 backend keeps a connection
// pool that can serve hundreds of concurrent Range GETs over a small
// number of TCP connections without exhausting file descriptors.
// Higher values are worth benchmarking on AWS-S3-with-public-network
// latency profiles, but on local MinIO 32 already gets the per-attempt
// manifest-walk wall time down to <100ms for the hottest table in the
// bench. See issue #6 for the analysis.
const manifestReadConcurrency = 32

// parquetReadConcurrency is the bounded-fan-out limit for parallel
// parquet input file reads in compactOnce's data-copy step. Same
// rationale as manifestReadConcurrency: HTTP/2 multiplexing makes
// the cost of 32 concurrent Range GETs roughly equal to one, while
// reducing total wall time from O(N×L) to ~max(L, ⌈N/32⌉×L) for N
// input files.
//
// The memory bound is parquetReadConcurrency × max-batches-per-file.
// On the bench's typical 240 KB-per-file workload (each file decoded
// to ~1 MB Arrow), 32 workers cap working memory at ~32 MB — fine
// for Lambda's smallest tier. On wider workloads (large parquet
// files with many row groups) the bound scales linearly; if it ever
// becomes a problem, drop the concurrency or switch to the
// stitching-binpack column-chunk byte-copy path that doesn't
// materialize Arrow at all.
const parquetReadConcurrency = 32

// compactOnce is the parquet-go-direct compaction path. It is the
// only compaction implementation; the legacy iceberg-go scan/overwrite
// path was deleted in the cut-over commit that promoted this function
// to be the default.
//
// Why parquet-go directly: the original path (iceberg-go's
// tx.Overwrite → classifier + partitionedFanoutWriter) silently loses
// rows under concurrent producer load. See
// github.com/mystictraveler/iceberg-janitor#4 and
// apache/iceberg-go#860 for the upstream report. The bench surfaced
// a downstream symptom: even iceberg-go's Scan.PlanFiles is
// non-deterministic on the same *Scan when a foreign writer is
// committing in parallel — two PlanFiles calls return different file
// lists despite WithSnapshotID being set, AND scan.ToArrowRecords
// returns yet a third row count even when the file list is stable.
// The instrumented bench showed the file count drifting up by exactly
// one file (= one streamer commit) between the two PlanFiles calls.
// So iceberg-go's scan path cannot be used safely for compaction
// against a hot table.
//
// This implementation BYPASSES iceberg-go's scan entirely. It:
//
//  1. Walks tbl.CurrentSnapshot().Manifests(fs) directly to get an
//     immutable list of manifest entries. Each manifest entry holds
//     a DataFile record with the file path, row count, and partition
//     tuple. This walk reads only files whose paths come from the
//     in-memory snapshot record, so it cannot drift even if a
//     foreign writer commits.
//  2. Filters the entries by partition tuple if PartitionTuple is
//     set. Match is structural: DataFile.Partition() returns a
//     map[int]any keyed by partition field id; we look up the
//     target column's iceberg field id from the schema, find the
//     partition field with that source id, and compare values.
//  3. Reads each matching file via parquet-go's pqarrow.NewFileReader
//     directly, with no iceberg-go scan, no row filter, no
//     partitionedFanoutWriter. Each file's rows are streamed as
//     Arrow record batches.
//  4. Writes those batches into a single new Parquet file via
//     pqarrow.NewFileWriter using a fieldless arrow schema (so that
//     iceberg-go's filesToDataFiles will accept the file when we
//     register it — see table/arrow_utils.go:1229).
//  5. Pre-flight checks rowsWritten == sum(DataFile.Count()). The
//     iceberg-go writer guarantees that each data file's manifest
//     entry has the exact row count of the file at write time, so
//     this is a hard equality. A mismatch means the parquet read
//     dropped rows (which would be a parquet-go bug, not an
//     iceberg-go race) and we abort BEFORE staging.
//  6. Calls tx.ReplaceDataFiles(ctx, oldPaths, [newPath], nil) which
//     takes EXPLICIT path lists with no row-filter classification
//     and no fanout writer.
//  7. Master check (existing safety.VerifyCompactionConsistency).
//  8. Commit.
//
// Identity-transform partition values are auto-inferred by
// filesToDataFiles from the Parquet footer's lower=upper bounds.
// Non-linear transforms (bucket / truncate / time-bucket) would
// fail there — but the streamer uses identity transforms specifically
// for this reason and the unpartitioned-table guard rejects
// --partition on tables with no partition spec.
func compactOnce(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions, result *CompactResult) error {
	tr := observe.Tracer("janitor.compact")
	ctx, span := tr.Start(ctx, "compactOnce")
	span.SetAttributes(observe.Table(ident[0], ident[1]), observe.Attempt(result.Attempts))
	defer span.End()

	ctx, loadSpan := tr.Start(ctx, "load_table")
	tbl, err := cat.LoadTable(ctx, ident)
	loadSpan.End()
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	if opts.PartitionTuple != nil {
		if spec := tbl.Spec(); spec.NumFields() == 0 {
			return fmt.Errorf("partition-scoped compaction requested but table %v is unpartitioned", ident)
		}
	}

	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil
	}
	result.BeforeSnapshotID = snap.SnapshotID
	result.BeforeFiles, result.BeforeBytes, result.BeforeRows = SnapshotFileStats(ctx, tbl)

	fs, err := tbl.FS(ctx)
	if err != nil {
		return fmt.Errorf("getting fs: %w", err)
	}

	// Step 1: walk the snapshot's manifest list and collect every
	// data-file entry. Build the typed partition predicates once.
	//
	// buildPartitionLiterals parses the user's raw `--partition col=value`
	// strings into typed iceberg.Literal values keyed by partition
	// field id. The two predicates below — matchPart (per-data-file
	// equality) and matchManifest (per-manifest bound check, used
	// to prune the GET storm before reading any manifests) — both
	// take this same typed binding slice and operate at different
	// layers. See compact_partition_types.go for the type-aware
	// parsing and comparison primitives.
	icebergSchema := tbl.Metadata().CurrentSchema()
	partitionBindings, err := buildPartitionLiterals(opts.PartitionTuple, icebergSchema, tbl.Spec())
	if err != nil {
		return fmt.Errorf("parsing partition tuple: %w", err)
	}
	matchPart := buildPartitionMatcher(partitionBindings)
	matchManifest := buildManifestPredicate(partitionBindings)

	ctx, walkSpan := tr.Start(ctx, "manifest_walk")
	manifests, err := snap.Manifests(fs)
	if err != nil {
		walkSpan.End()
		return fmt.Errorf("listing manifests: %w", err)
	}
	walkSpan.SetAttributes(observe.Manifests(len(manifests)))

	// Bounded-concurrency manifest read. Sequential reads of N
	// manifests cost N round-trips, which on S3 (~20 ms each) becomes
	// the dominant cost on hot streaming tables and causes the
	// compactor to lose the writer-fight CAS race against fast
	// streamers. Parallelizing across 32 workers drops the wall time
	// to ~max(L, ⌈N/32⌉ × L), which is small enough that the compact
	// finishes between writer commits even on the hottest table in
	// the bench (store_sales at 60 cpm). See issue #6 for the full
	// analysis and bench numbers.
	type manifestResult struct {
		paths []string
		rows  int64
	}
	results := make([]manifestResult, len(manifests))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(manifestReadConcurrency)
	for i, m := range manifests {
		i, m := i, m
		// Manifest pruning: if the manifest's per-partition-column
		// bound summaries can prove that the target partition value
		// CANNOT be present in this manifest's data files, skip the
		// GET entirely. This is the load-bearing optimization for
		// the writer-fight pathology — see buildManifestPredicate.
		// On a hot streaming table this typically eliminates 90+%
		// of manifest reads.
		if matchManifest != nil && !matchManifest(m) {
			continue
		}
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			mf, err := fs.Open(m.FilePath())
			if err != nil {
				return fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
			}
			entries, err := icebergpkg.ReadManifest(m, mf, true)
			mf.Close()
			if err != nil {
				return fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
			}
			var local manifestResult
			for _, e := range entries {
				df := e.DataFile()
				if df == nil || df.ContentType() != icebergpkg.EntryContentData {
					continue
				}
				if matchPart != nil && !matchPart(df) {
					continue
				}
				// Pattern B (skip-already-large-files): if a target
				// file size is configured, files at or above the
				// threshold are kept in the snapshot unchanged. They
				// are NOT added to oldPaths (so ReplaceDataFiles will
				// not remove them) and NOT read or rewritten. Only
				// the small-file tail is rewritten. See the doc on
				// CompactOptions.TargetFileSizeBytes for the
				// architectural rationale and bench numbers.
				if opts.TargetFileSizeBytes > 0 && df.FileSizeBytes() >= opts.TargetFileSizeBytes {
					continue
				}
				local.paths = append(local.paths, df.FilePath())
				local.rows += df.Count()
			}
			results[i] = local
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Flatten results in manifest-list order. Order matters because
	// downstream filesToDataFiles inference (and any future debugging
	// of which-file-came-from-where) is much easier to reason about
	// when the input order is deterministic and matches the snapshot's
	// own manifest ordering.
	var oldPaths []string
	var expectedRows int64
	for _, r := range results {
		oldPaths = append(oldPaths, r.paths...)
		expectedRows += r.rows
	}
	walkSpan.SetAttributes(observe.Files(len(oldPaths)), observe.Rows(expectedRows))
	walkSpan.End()

	if len(oldPaths) == 0 {
		return nil // nothing to compact
	}

	// Step 2: open the destination Parquet file. The location
	// provider gives us <table>/data/<filename>.
	locProv, err := tbl.LocationProvider()
	if err != nil {
		return fmt.Errorf("getting location provider: %w", err)
	}
	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return fmt.Errorf("filesystem does not support writes")
	}
	newFileName := fmt.Sprintf("janitor-%s.parquet", uuid.New().String())
	newFilePath := locProv.NewDataLocation(newFileName)

	// Build a fieldless arrow schema. iceberg-go's filesToDataFiles
	// rejects files that carry arrow field ids; we map column names
	// to iceberg field ids via the schema's name mapping at register
	// time (see filesToDataFiles in table/arrow_utils.go:1199).
	writeSchema, err := icebergtable.SchemaToArrowSchema(icebergSchema, nil, false, false)
	if err != nil {
		return fmt.Errorf("building arrow write schema: %w", err)
	}

	out, err := wfs.Create(newFilePath)
	if err != nil {
		return fmt.Errorf("creating output file %s: %w", newFilePath, err)
	}

	// Step 3: stitching binpack v1 (fast path) — read each input
	// parquet file via parquet-go (NOT arrow-go's pqarrow) and use
	// parquet-go's GenericWriter.WriteRowGroup to copy each input row
	// group into the output. This skips Arrow decoding entirely and
	// uses parquet-go's CopyRows fast path through RowWriterTo, which
	// keeps values in their parquet-native typed-value form across
	// the read→write boundary. Per design plan decision #13.
	//
	// On the bench's hot tables (store_sales etc), the inputs are all
	// from the streamer with identical writer config so the fast path
	// fires reliably. If parquet-go returns a schema-mismatch error
	// (which shouldn't happen on uniform inputs but could for
	// real-world workloads with mixed writers), we fall through to
	// the legacy pqarrow decode/encode read path below.
	rowsWritten, stitchErr := stitchParquetFiles(ctx, fs, oldPaths, out)
	usedStitch := stitchErr == nil && rowsWritten == expectedRows
	if !usedStitch {
		// Stitching failed or row count mismatched. Close + delete
		// the partial output and fall through to the pqarrow path
		// which we know works for any input shape.
		_ = out.Close()
		_ = wfs.Remove(newFilePath)
		out, err = wfs.Create(newFilePath)
		if err != nil {
			return fmt.Errorf("creating output file %s for fallback: %w", newFilePath, err)
		}
		rowsWritten = 0

		ctx, stitchSpan := tr.Start(ctx, "stitch_write_fallback")
		stitchSpan.SetAttributes(observe.Files(len(oldPaths)))

		pqProps := parquet.NewWriterProperties(parquet.WithStats(true))
		pqWriter, perr := pqarrow.NewFileWriter(writeSchema, out, pqProps, pqarrow.DefaultWriterProps())
		if perr != nil {
			stitchSpan.End()
			_ = wfs.Remove(newFilePath)
			return fmt.Errorf("creating parquet writer: %w", perr)
		}
		cleanup := func() {
			_ = pqWriter.Close()
			_ = wfs.Remove(newFilePath)
		}
		mem := memory.DefaultAllocator

		// Fallback: read via pqarrow into Arrow batches, write
		// serially through pqarrow.FileWriter under a mutex. Same
		// shape as the path that issue #6 parallelized — workers
		// read in parallel, writes are serialized.
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
			stitchSpan.End()
			cleanup()
			return err
		}
		if err := pqWriter.Close(); err != nil {
			stitchSpan.End()
			_ = wfs.Remove(newFilePath)
			return fmt.Errorf("closing parquet writer: %w", err)
		}
		stitchSpan.SetAttributes(observe.Rows(rowsWritten))
		stitchSpan.End()
	} else {
		// Stitching path succeeded. Close the output writer (the
		// stitch helper itself doesn't close it because it doesn't
		// own the lifecycle).
		if cerr := out.Close(); cerr != nil {
			_ = wfs.Remove(newFilePath)
			return fmt.Errorf("closing stitched output %s: %w", newFilePath, cerr)
		}
	}

	// Step 4: pre-flight check. The iceberg-go writer's manifest
	// entries record the exact row count of each data file. If
	// rowsWritten != expectedRows, parquet-go's reader dropped or
	// added rows during file copy — abort and clean up.
	if rowsWritten != expectedRows {
		_ = wfs.Remove(newFilePath)
		return fmt.Errorf("read/manifest row mismatch: read %d rows but manifest entries sum to %d", rowsWritten, expectedRows)
	}

	// Step 5: stage the replace. ReplaceDataFiles takes explicit
	// path lists, walks the transaction's pinned snapshot to convert
	// oldPaths to DataFile records, and calls filesToDataFiles on the
	// new path to compute stats and infer partition values.
	tx := tbl.NewTransaction()
	if err := tx.ReplaceDataFiles(ctx, oldPaths, []string{newFilePath}, nil); err != nil {
		_ = wfs.Remove(newFilePath)
		return fmt.Errorf("staging replace: %w", err)
	}

	staged, err := tx.StagedTable()
	if err != nil {
		return fmt.Errorf("getting staged table: %w", err)
	}

	// Step 6: master check (mandatory, unchanged).
	ctx, verifySpan := tr.Start(ctx, "master_check")
	verification, err := safety.VerifyCompactionConsistency(ctx, tbl, staged, cat.Props())
	result.Verification = verification
	if err != nil {
		verifySpan.RecordError(err)
		verifySpan.End()
		return err
	}
	verifySpan.SetAttributes(attribute.String("result", verification.Overall))
	verifySpan.End()

	// Step 7: commit.
	_, commitSpan := tr.Start(ctx, "cas_commit")
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		commitSpan.RecordError(err)
		commitSpan.End()
		return err
	}
	commitSpan.End()

	if newSnap := newTbl.CurrentSnapshot(); newSnap != nil {
		result.AfterSnapshotID = newSnap.SnapshotID
	}
	result.AfterFiles, result.AfterBytes, result.AfterRows = SnapshotFileStats(ctx, newTbl)
	if result.AfterRows != result.BeforeRows {
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", result.BeforeRows, result.AfterRows)
	}
	return nil
}

// buildManifestPredicate returns a fast in-memory check that decides
// whether a given manifest is worth opening for a partition-scoped
// compaction. The check looks at the manifest's per-partition-column
// lower/upper bound summaries (which were written into the
// manifest_list avro file by whoever produced the snapshot, so they
// are already in memory after `snap.Manifests(fs)` returns) and
// returns false if those bounds prove the requested partition value
// CANNOT be present in the manifest's data files.
//
// Returns nil (== "include every manifest") when:
//   - bindings is empty (whole-table compaction; nothing to prune)
//   - the binding's result type isn't comparable via compareLiterals
//     (currently only DecimalType — see compact_partition_types.go)
//
// **Why this matters and how it composes:**
//
// On a hot streaming table the manifest list grows by one or two
// entries per writer commit. Without pruning, every partition-scoped
// compact attempt walks ALL manifests (even though almost all of
// them don't contain files from the target partition), paying one S3
// round-trip per manifest. The bench's worst case (run 9 / run 10)
// was store_sales partition compact → 13449 data files → ~150 manifests
// → 222 s wall because the writer kept committing during the walk.
//
// Manifest pruning skips manifests whose bounds don't include the
// target partition value, BEFORE paying the GET round-trip. For a
// 50-partition table where each commit lands in only 1-3 partitions
// the win is roughly 10-30×; for time-partitioned tables where each
// commit lands in one date partition the win is closer to 50-100×.
//
// **Composition with other optimizations:**
//
//   - Pattern B (skip-already-large-files via opts.TargetFileSizeBytes)
//     filters which files INSIDE a manifest get rewritten. This
//     pruning filters which MANIFESTS get opened in the first place.
//     Both are independent and stack: pruning first to skip the GET,
//     then Pattern B to skip the per-file rewrite work on the
//     manifests we did open.
//
//   - The future on-commit dispatcher (issue #3, "Pattern C") will
//     fire compactOnce per writer-commit-event, scoped to the
//     partition the event touched. The dispatcher passes the same
//     PartitionTuple to compactOnce, so this predicate works
//     unchanged from the dispatcher path. Pattern C does NOT need a
//     special code path here; it's "polling vs event-driven trigger"
//     wrapping the same compactOnce.
//
//   - Manifest rewrite (a future maintenance op, tracked separately)
//     is the missing additive-better step. Today's compactions write
//     better-organized DATA files but the historical micro-manifests
//     from individual streamer commits remain in the snapshot.
//     Manifest rewrite would consolidate them into fewer larger
//     manifests, after which this pruning predicate skips even more
//     manifests per attempt. Pruning is useful right now even
//     without manifest rewrite, but the two compound: better
//     manifests → tighter bounds → more skips.
//
// SCD type 1 / type 2 tables and pure streaming tables both benefit
// from this — the predicate doesn't care about the workload pattern,
// only about the partition spec and the in-memory bounds.
//
// **Spec-evolution caveat:** the predicate is built against the
// current partition spec. If the table's spec was evolved (e.g. a
// partition column was added), older manifests may have a shorter
// Partitions() slice than the current spec expects. We detect this
// case (specPosition >= len(summaries)) and conservatively include
// the manifest. Manifest pruning becomes less effective on tables
// whose spec has evolved, but never wrong.
func buildManifestPredicate(bindings []partitionLiteralBinding) func(icebergpkg.ManifestFile) bool {
	if len(bindings) == 0 {
		return nil
	}
	return func(m icebergpkg.ManifestFile) bool {
		summaries := m.Partitions()
		for _, b := range bindings {
			if b.SpecPosition >= len(summaries) {
				// Manifest's partition summary list is shorter than
				// the spec — this can happen for manifests written
				// against an older spec, before a new partition
				// column was added. Safest is to include the
				// manifest (we can't prove the value is absent).
				return true
			}
			s := summaries[b.SpecPosition]
			if s.LowerBound == nil || s.UpperBound == nil {
				// No bounds recorded — can't decide. Include.
				return true
			}
			lo, err := icebergpkg.LiteralFromBytes(b.ResultType, *s.LowerBound)
			if err != nil {
				return true
			}
			hi, err := icebergpkg.LiteralFromBytes(b.ResultType, *s.UpperBound)
			if err != nil {
				return true
			}
			cmpLo, ok := compareLiterals(b.Want, lo, b.ResultType)
			if !ok {
				return true
			}
			cmpHi, ok := compareLiterals(b.Want, hi, b.ResultType)
			if !ok {
				return true
			}
			// Include the manifest iff lo <= want <= hi.
			if cmpLo < 0 || cmpHi > 0 {
				return false
			}
		}
		return true
	}
}

// buildPartitionMatcher returns a predicate over DataFile that
// returns true iff the file's Partition() tuple matches the
// requested partition value bindings. Returns nil ("match
// everything") when bindings is empty.
//
// The matcher uses Literal.Equals via valueToLiteral to convert
// each data file's raw partition value into a typed Literal of the
// right iceberg type, then compares against the user-supplied
// Want literal. This routes all type-specific equality through
// iceberg-go's Literal implementations (one place to add support
// for new types — see compact_partition_types.go).
//
// The matcher is the per-FILE filter that runs INSIDE manifest
// entries (after a manifest has been opened and read). The
// manifest predicate (buildManifestPredicate above) is the
// per-MANIFEST filter that runs BEFORE the manifest GET. They
// operate on disjoint layers and stack additively.
func buildPartitionMatcher(bindings []partitionLiteralBinding) func(icebergpkg.DataFile) bool {
	if len(bindings) == 0 {
		return nil
	}
	return func(df icebergpkg.DataFile) bool {
		p := df.Partition()
		for _, b := range bindings {
			got, ok := p[b.PartFieldID]
			if !ok {
				return false
			}
			gotLit, ok := valueToLiteral(got, b.ResultType)
			if !ok {
				// Don't know how to wrap the value as a Literal of
				// this type. Conservative: this file does not match.
				// (False positives in the matcher mean we read more
				// than necessary; false negatives in the matcher
				// mean we drop rows. Returning false here is the
				// false-positive direction — we drop a file we
				// might have wanted, which means we under-compact,
				// which is harmless. Returning true would risk
				// over-compacting a file from a different partition,
				// which is correctness — much worse.)
				return false
			}
			if !b.Want.Equals(gotLit) {
				return false
			}
		}
		return true
	}
}

// readParquetFileBatches reads `path` via parquet-go directly and
// returns every row group's records as a slice of arrow.RecordBatch
// rewrapped under `writeSchema` (the fieldless target schema), plus
// the total row count. The caller is responsible for Release()'ing
// each returned batch after use (the bench's compact path does this
// under the writer mutex right after pqWriter.Write).
//
// This is the READ-ONLY half of what was once `copyParquetFile`. The
// split is what lets us parallelize: many workers can call this
// function concurrently against different files (each one's network
// I/O multiplexes over gocloud's HTTP/2 connection pool), and a
// single mutex-guarded writer goroutine drains the batches into the
// shared pqarrow.FileWriter. The writer is the only serialization
// point.
//
// We use parquet-go's pqarrow reader rather than iceberg-go's scan
// because the bug we're working around (apache/iceberg-go#860) is
// in iceberg-go's scan path. parquet-go is a layer below iceberg-go
// — it has no knowledge of snapshots, manifests, or row filters,
// and so cannot exhibit the snapshot-pinning races that the scan
// path does.
func readParquetFileBatches(ctx context.Context, fs icebergio.IO, path string, writeSchema *arrow.Schema, mem memory.Allocator) ([]arrow.RecordBatch, int64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	pqf, err := file.NewParquetReader(f)
	if err != nil {
		return nil, 0, fmt.Errorf("parquet open: %w", err)
	}
	defer pqf.Close()

	arrReader, err := pqarrow.NewFileReader(pqf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, 0, fmt.Errorf("pqarrow open: %w", err)
	}

	// nil columns + nil row groups → read all columns and all row
	// groups.
	rec, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("get record reader: %w", err)
	}
	defer rec.Release()

	var (
		batches []arrow.RecordBatch
		rows    int64
	)
	for rec.Next() {
		batch := rec.RecordBatch()
		converted, cerr := rebatchWithSchema(batch, writeSchema)
		if cerr != nil {
			// Release any batches we already accumulated.
			for _, b := range batches {
				b.Release()
			}
			return nil, 0, fmt.Errorf("rebatch: %w", cerr)
		}
		batches = append(batches, converted)
		rows += converted.NumRows()
	}
	if err := rec.Err(); err != nil {
		for _, b := range batches {
			b.Release()
		}
		return nil, 0, fmt.Errorf("read: %w", err)
	}
	return batches, rows, nil
}

// rebatchWithSchema returns a copy of `batch` whose schema is
// `target`. Records read from parquet-go carry the schema metadata
// from the source file (which may include arrow field ids).
// pqarrow.FileWriter.Write requires the input record schema to
// equal the writer's schema by arrow.Schema.Equal — field-id
// metadata makes the two schemas non-equal even when column names
// and types match. We rewrap the columns under the target schema;
// nothing about the column data changes.
func rebatchWithSchema(batch arrow.RecordBatch, target *arrow.Schema) (arrow.RecordBatch, error) {
	if batch.Schema().Equal(target) {
		batch.Retain()
		return batch, nil
	}
	if int(batch.NumCols()) != len(target.Fields()) {
		return nil, fmt.Errorf("column count mismatch: batch=%d target=%d", batch.NumCols(), len(target.Fields()))
	}
	cols := make([]arrow.Array, batch.NumCols())
	for i := range cols {
		cols[i] = batch.Column(i)
	}
	return array.NewRecord(target, cols, batch.NumRows()), nil
}
