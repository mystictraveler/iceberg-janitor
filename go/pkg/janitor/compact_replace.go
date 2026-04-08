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

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
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
	tbl, err := cat.LoadTable(ctx, ident)
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
	// data-file entry. Build the partition-match predicate once.
	icebergSchema := tbl.Metadata().CurrentSchema()
	matchPart, err := buildPartitionMatcher(opts.PartitionTuple, tbl.Spec(), icebergSchema)
	if err != nil {
		return fmt.Errorf("building partition matcher: %w", err)
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return fmt.Errorf("listing manifests: %w", err)
	}

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

	pqProps := parquet.NewWriterProperties(parquet.WithStats(true))
	pqWriter, err := pqarrow.NewFileWriter(writeSchema, out, pqProps, pqarrow.DefaultWriterProps())
	if err != nil {
		_ = wfs.Remove(newFilePath)
		return fmt.Errorf("creating parquet writer: %w", err)
	}
	cleanup := func() {
		_ = pqWriter.Close()
		_ = wfs.Remove(newFilePath)
	}

	mem := memory.DefaultAllocator

	// Step 3: read each input file via parquet-go directly and copy
	// its rows into the output writer. We parallelize the READ side
	// (workers each open one source parquet, decode it to Arrow batches,
	// rebatch under the writer schema), but the WRITE side stays
	// single-threaded behind a mutex because pqarrow.FileWriter is
	// not safe for concurrent Write calls. Each worker gathers its
	// file's batches in a local slice, then under the writer mutex
	// flushes them and releases the references. This bounds working
	// memory to parquetReadConcurrency × max-batches-per-file rather
	// than holding all decoded data at once.
	//
	// On the TPC-DS streaming bench, this drops store_sales partition
	// compact from ~33 s/attempt (sequential) to <2 s/attempt — under
	// the writer's commit interval, so the CAS retry race is winnable
	// after one attempt instead of thirteen. See issue #6.
	var rowsWritten int64
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
					// Release the batches we're about to drop on the floor.
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
		cleanup()
		return err
	}

	if err := pqWriter.Close(); err != nil {
		_ = wfs.Remove(newFilePath)
		return fmt.Errorf("closing parquet writer: %w", err)
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
	verification, err := safety.VerifyCompactionConsistency(ctx, tbl, staged, cat.Props())
	result.Verification = verification
	if err != nil {
		return err
	}

	// Step 7: commit.
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return err
	}

	if newSnap := newTbl.CurrentSnapshot(); newSnap != nil {
		result.AfterSnapshotID = newSnap.SnapshotID
	}
	result.AfterFiles, result.AfterBytes, result.AfterRows = SnapshotFileStats(ctx, newTbl)
	if result.AfterRows != result.BeforeRows {
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", result.BeforeRows, result.AfterRows)
	}
	return nil
}

// buildPartitionMatcher returns a predicate over DataFile that
// returns true iff the file's Partition() tuple matches the
// requested column=value. Returns nil (== "match everything") if
// PartitionTuple is empty.
//
// The lookup is structural:
//   - resolve column name → iceberg schema field id
//   - resolve schema field id → partition field id (where the
//     partition field's SourceID == schema field id)
//   - DataFile.Partition() is keyed by partition field id; compare
//     values
func buildPartitionMatcher(partTuple map[string]any, spec icebergpkg.PartitionSpec, sc *icebergpkg.Schema) (func(icebergpkg.DataFile) bool, error) {
	if len(partTuple) == 0 {
		return nil, nil
	}
	type binding struct {
		partFieldID int
		want        any
	}
	var bindings []binding
	for col, val := range partTuple {
		schemaField, ok := sc.FindFieldByName(col)
		if !ok {
			return nil, fmt.Errorf("partition column %q not found in schema", col)
		}
		var partFieldID = -1
		for f := range spec.Fields() {
			if f.SourceID == schemaField.ID {
				partFieldID = f.FieldID
				break
			}
		}
		if partFieldID == -1 {
			return nil, fmt.Errorf("schema column %q (id=%d) is not a partition source", col, schemaField.ID)
		}
		bindings = append(bindings, binding{partFieldID: partFieldID, want: val})
	}
	return func(df icebergpkg.DataFile) bool {
		p := df.Partition()
		for _, b := range bindings {
			got, ok := p[b.partFieldID]
			if !ok {
				return false
			}
			if !partitionValueEqual(got, b.want) {
				return false
			}
		}
		return true
	}, nil
}

// partitionValueEqual compares two partition values for equality
// allowing for the integer-type lattice that comes out of iceberg
// metadata vs the parsed CLI flag (which lands as int64). The
// streamer's identity-partitioned columns are int32 in the schema
// but stored as int32 in the partition tuple, while the CLI parses
// the value as int64. Normalize both sides to int64 before
// comparing.
func partitionValueEqual(a, b any) bool {
	ai, aok := toInt64(a)
	bi, bok := toInt64(b)
	if aok && bok {
		return ai == bi
	}
	return a == b
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	}
	return 0, false
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
