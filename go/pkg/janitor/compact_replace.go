package janitor

import (
	"context"
	"fmt"

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

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

// compactOnceReplace is the architectural fix for
// github.com/mystictraveler/iceberg-janitor#4 / apache/iceberg-go#860.
//
// The original path (compactOnce → tx.Overwrite → iceberg-go's
// classifier + partitionedFanoutWriter) silently loses rows under
// concurrent producer load. The bench surfaced a downstream symptom:
// even iceberg-go's Scan.PlanFiles is non-deterministic on the same
// *Scan when a foreign writer is committing in parallel — two
// PlanFiles calls return different file lists despite WithSnapshotID
// being set, AND scan.ToArrowRecords returns yet a third row count
// even when the file list is stable. The instrumented bench showed
// the file count drifting up by exactly one file (= one streamer
// commit) between the two PlanFiles calls. So iceberg-go's scan
// path cannot be used safely for compaction against a hot table.
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
//
// Gated behind JANITOR_COMPACT_USE_REPLACE=1 in compact.go's Compact
// dispatch.
func compactOnceReplace(ctx context.Context, cat *catalog.DirectoryCatalog, ident icebergtable.Identifier, opts CompactOptions, result *CompactResult) error {
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("loading table %v: %w", ident, err)
	}

	if opts.PartitionTuple != nil || opts.RowFilter != nil {
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

	var oldPaths []string
	var expectedRows int64
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			return fmt.Errorf("opening manifest %s: %w", m.FilePath(), err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			return fmt.Errorf("reading manifest %s: %w", m.FilePath(), err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			if matchPart != nil && !matchPart(df) {
				continue
			}
			oldPaths = append(oldPaths, df.FilePath())
			expectedRows += df.Count()
		}
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
	// its rows into the output writer.
	var rowsWritten int64
	for _, fpath := range oldPaths {
		n, rerr := copyParquetFile(ctx, fs, fpath, pqWriter, writeSchema, mem)
		if rerr != nil {
			cleanup()
			return fmt.Errorf("copying %s: %w", fpath, rerr)
		}
		rowsWritten += n
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

// copyParquetFile reads `path` via parquet-go directly and writes
// every row group's records into `dst`, rewrapped under
// `writeSchema` (the fieldless target schema). Returns the number
// of rows written.
//
// We use parquet-go's pqarrow reader rather than iceberg-go's scan
// because the bug we're working around (apache/iceberg-go#860) is
// in iceberg-go's scan path. parquet-go is a layer below iceberg-go
// — it has no knowledge of snapshots, manifests, or row filters,
// and so cannot exhibit the snapshot-pinning races that the scan
// path does.
func copyParquetFile(ctx context.Context, fs icebergio.IO, path string, dst *pqarrow.FileWriter, writeSchema *arrow.Schema, mem memory.Allocator) (int64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	pqf, err := file.NewParquetReader(f)
	if err != nil {
		return 0, fmt.Errorf("parquet open: %w", err)
	}
	defer pqf.Close()

	arrReader, err := pqarrow.NewFileReader(pqf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return 0, fmt.Errorf("pqarrow open: %w", err)
	}

	// nil columns + nil row groups → read all columns and all row
	// groups.
	rec, err := arrReader.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, fmt.Errorf("get record reader: %w", err)
	}
	defer rec.Release()

	var rows int64
	for rec.Next() {
		batch := rec.RecordBatch()
		converted, cerr := rebatchWithSchema(batch, writeSchema)
		if cerr != nil {
			return rows, fmt.Errorf("rebatch: %w", cerr)
		}
		if werr := dst.Write(converted); werr != nil {
			converted.Release()
			return rows, fmt.Errorf("write: %w", werr)
		}
		rows += converted.NumRows()
		converted.Release()
	}
	if err := rec.Err(); err != nil {
		return rows, fmt.Errorf("read: %w", err)
	}
	return rows, nil
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
