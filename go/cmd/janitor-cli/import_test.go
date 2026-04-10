package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "gocloud.dev/blob/fileblob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
)

// writeTestParquetFile writes a small parquet file with the given
// rows to disk and returns the number of rows written.
func writeTestParquetFile(t *testing.T, path string, id int32, region string, n int) int {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "region", Type: arrow.BinaryTypes.String},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	mem := memory.DefaultAllocator
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()
	for i := 0; i < n; i++ {
		bldr.Field(0).(*array.Int32Builder).Append(id*1000 + int32(i))
		bldr.Field(1).(*array.StringBuilder).Append(region)
		bldr.Field(2).(*array.Float64Builder).Append(float64(i) * 1.5)
	}
	rec := bldr.NewRecord()
	defer rec.Release()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	w, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("pqarrow writer: %v", err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	// pqarrow.FileWriter.Close() closes the underlying file; don't double-close.
	return n
}

// TestImportUnpartitioned verifies the import flow for a flat
// directory of parquet files with no hive-style partitioning.
func TestImportUnpartitioned(t *testing.T) {
	dir := t.TempDir()
	tableDir := filepath.Join(dir, "mydb.db", "events")
	dataDir := filepath.Join(tableDir, "data")

	// Write 3 small parquet files.
	writeTestParquetFile(t, filepath.Join(dataDir, "file1.parquet"), 1, "us", 100)
	writeTestParquetFile(t, filepath.Join(dataDir, "file2.parquet"), 2, "eu", 150)
	writeTestParquetFile(t, filepath.Join(dataDir, "file3.parquet"), 3, "ap", 50)

	ctx := context.Background()
	warehouseURL := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", warehouseURL, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	defer cat.Close()

	// Read schema from first file.
	schema, err := readParquetSchema(ctx, cat.Bucket(), "mydb.db/events/data/file1.parquet")
	if err != nil {
		t.Fatalf("readParquetSchema: %v", err)
	}
	if len(schema.Fields()) != 3 {
		t.Fatalf("schema fields: got %d want 3", len(schema.Fields()))
	}

	// Convert to iceberg schema.
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIceberg: %v", err)
	}

	// Create table.
	ident := icebergtable.Identifier{"mydb", "events"}
	tbl, err := cat.CreateTable(ctx, ident, icebergSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if tbl.Metadata().TableUUID().String() == "" {
		t.Fatal("table UUID is empty")
	}

	// Register files.
	files := []string{
		"file://" + filepath.Join(dataDir, "file1.parquet"),
		"file://" + filepath.Join(dataDir, "file2.parquet"),
		"file://" + filepath.Join(dataDir, "file3.parquet"),
	}
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(ctx, files, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify.
	fileCount, totalBytes, totalRows := janitor.SnapshotFileStats(ctx, newTbl)
	if fileCount != 3 {
		t.Errorf("file count: got %d want 3", fileCount)
	}
	if totalRows != 300 {
		t.Errorf("total rows: got %d want 300", totalRows)
	}
	if totalBytes <= 0 {
		t.Errorf("total bytes should be positive, got %d", totalBytes)
	}

	// Verify the table can be reloaded from disk.
	reloaded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable after import: %v", err)
	}
	f2, _, r2 := janitor.SnapshotFileStats(ctx, reloaded)
	if f2 != 3 || r2 != 300 {
		t.Errorf("reloaded stats: files=%d rows=%d, want 3/300", f2, r2)
	}
}

// TestImportWithCompact verifies that --compact stitches many small
// files into fewer large ones. We write 10 small files (50 rows each)
// into a single partition, import them, then compact. Pattern B with
// the 128 MB default target treats all of them as "small" and merges
// them into one.
func TestImportWithCompact(t *testing.T) {
	dir := t.TempDir()
	tableDir := filepath.Join(dir, "mydb.db", "logs")
	dataDir := filepath.Join(tableDir, "data")

	// Write 10 small files — total 500 rows.
	totalRows := 0
	for i := 0; i < 10; i++ {
		path := filepath.Join(dataDir, fmt.Sprintf("chunk-%03d.parquet", i))
		totalRows += writeTestParquetFile(t, path, int32(i), "us", 50)
	}

	ctx := context.Background()
	warehouseURL := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", warehouseURL, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	defer cat.Close()

	// Schema + create.
	schema, err := readParquetSchema(ctx, cat.Bucket(), "mydb.db/logs/data/chunk-000.parquet")
	if err != nil {
		t.Fatalf("readParquetSchema: %v", err)
	}
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIceberg: %v", err)
	}
	ident := icebergtable.Identifier{"mydb", "logs"}
	tbl, err := cat.CreateTable(ctx, ident, icebergSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Register all 10 files.
	var absFiles []string
	for i := 0; i < 10; i++ {
		absFiles = append(absFiles, "file://"+filepath.Join(dataDir, fmt.Sprintf("chunk-%03d.parquet", i)))
	}
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(ctx, absFiles, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	afterImport, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	beforeFiles, _, beforeRows := janitor.SnapshotFileStats(ctx, afterImport)
	if beforeFiles != 10 {
		t.Errorf("before compact: files=%d want 10", beforeFiles)
	}
	if beforeRows != int64(totalRows) {
		t.Errorf("before compact: rows=%d want %d", beforeRows, totalRows)
	}

	// Compact — should stitch 10 small files into 1.
	compactResult, err := janitor.CompactTable(ctx, cat, ident, janitor.CompactOptions{})
	if err != nil {
		t.Fatalf("CompactTable: %v", err)
	}
	if compactResult.PartitionsSucceeded == 0 && compactResult.PartitionsFound > 0 {
		t.Errorf("no partitions compacted: found=%d succeeded=%d failed=%d",
			compactResult.PartitionsFound, compactResult.PartitionsSucceeded, compactResult.PartitionsFailed)
	}

	// Reload and verify.
	afterCompact, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable after compact: %v", err)
	}
	afterFiles, _, afterRows := janitor.SnapshotFileStats(ctx, afterCompact)
	if afterFiles >= beforeFiles {
		t.Errorf("compact did not reduce files: before=%d after=%d", beforeFiles, afterFiles)
	}
	if afterRows != beforeRows {
		t.Errorf("row count changed: before=%d after=%d", beforeRows, afterRows)
	}
	t.Logf("import+compact: %d files → %d files, %d rows preserved", beforeFiles, afterFiles, afterRows)
}

// TestImportPartitioned verifies import with hive-style partitioning
// (region=us/, region=eu/, etc.) and that the partition spec is
// correctly inferred from the directory structure.
func TestImportPartitioned(t *testing.T) {
	dir := t.TempDir()
	tableDir := filepath.Join(dir, "mydb.db", "events")

	// Write files in hive-style partition dirs.
	writeTestParquetFile(t, filepath.Join(tableDir, "data", "region=us", "f1.parquet"), 1, "us", 80)
	writeTestParquetFile(t, filepath.Join(tableDir, "data", "region=us", "f2.parquet"), 2, "us", 70)
	writeTestParquetFile(t, filepath.Join(tableDir, "data", "region=eu", "f3.parquet"), 3, "eu", 60)
	writeTestParquetFile(t, filepath.Join(tableDir, "data", "region=ap", "f4.parquet"), 4, "ap", 40)

	ctx := context.Background()
	warehouseURL := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", warehouseURL, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	defer cat.Close()

	// Read schema.
	schema, err := readParquetSchema(ctx, cat.Bucket(), "mydb.db/events/data/region=us/f1.parquet")
	if err != nil {
		t.Fatalf("readParquetSchema: %v", err)
	}
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIceberg: %v", err)
	}

	// Infer partition spec from file paths.
	filePaths := []string{
		"mydb.db/events/data/region=us/f1.parquet",
		"mydb.db/events/data/region=us/f2.parquet",
		"mydb.db/events/data/region=eu/f3.parquet",
		"mydb.db/events/data/region=ap/f4.parquet",
	}
	spec := inferPartitionSpec(filePaths, "mydb.db/events", icebergSchema)
	if spec == nil {
		t.Fatal("inferPartitionSpec returned nil, expected partition on 'region'")
	}
	if spec.NumFields() != 1 {
		t.Fatalf("partition spec fields: got %d want 1", spec.NumFields())
	}

	// Verify the partition field name.
	var partFieldName string
	for f := range spec.Fields() {
		partFieldName = f.Name
		break
	}
	if partFieldName != "region" {
		t.Errorf("partition field name: got %q want %q", partFieldName, "region")
	}

	// Create table with partition spec.
	ident := icebergtable.Identifier{"mydb", "events"}
	tbl, err := cat.CreateTable(ctx, ident, icebergSchema,
		icebergcat.WithPartitionSpec(spec))
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Register files.
	absFiles := make([]string, len(filePaths))
	for i, key := range filePaths {
		absFiles[i] = "file://" + filepath.Join(dir, key)
	}
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(ctx, absFiles, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify.
	fileCount, _, totalRows := janitor.SnapshotFileStats(ctx, newTbl)
	if fileCount != 4 {
		t.Errorf("file count: got %d want 4", fileCount)
	}
	if totalRows != 250 {
		t.Errorf("total rows: got %d want 250 (80+70+60+40)", totalRows)
	}

	// Verify the table is partitioned by checking metadata.
	reloaded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}
	reloadedSpec := reloaded.Spec()
	if reloadedSpec.NumFields() != 1 {
		t.Errorf("reloaded spec fields: got %d want 1", reloadedSpec.NumFields())
	}
}

// TestInferPartitionSpec checks the path-based inference logic
// directly without needing a full import cycle.
func TestInferPartitionSpec(t *testing.T) {
	schema := icebergpkg.NewSchema(0,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.PrimitiveTypes.Int32, Required: true},
		icebergpkg.NestedField{ID: 2, Name: "date", Type: icebergpkg.PrimitiveTypes.Date, Required: false},
		icebergpkg.NestedField{ID: 3, Name: "region", Type: icebergpkg.PrimitiveTypes.String, Required: false},
		icebergpkg.NestedField{ID: 4, Name: "value", Type: icebergpkg.PrimitiveTypes.Float64, Required: false},
	)

	cases := []struct {
		name       string
		files      []string
		prefix     string
		wantFields int
		wantNames  []string
	}{
		{
			name:       "single partition column",
			files:      []string{"tbl/data/date=2026-01-01/f.parquet", "tbl/data/date=2026-01-02/f.parquet"},
			prefix:     "tbl",
			wantFields: 1,
			wantNames:  []string{"date"},
		},
		{
			name:       "two partition columns",
			files:      []string{"tbl/data/region=us/date=2026-01-01/f.parquet"},
			prefix:     "tbl",
			wantFields: 2,
			wantNames:  []string{"region", "date"},
		},
		{
			name:       "no partitioning",
			files:      []string{"tbl/data/f1.parquet", "tbl/data/f2.parquet"},
			prefix:     "tbl",
			wantFields: 0,
		},
		{
			name:       "unknown column in path ignored",
			files:      []string{"tbl/data/unknown_col=x/f.parquet"},
			prefix:     "tbl",
			wantFields: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := inferPartitionSpec(tc.files, tc.prefix, schema)
			if tc.wantFields == 0 {
				if spec != nil {
					t.Errorf("expected nil spec, got %d fields", spec.NumFields())
				}
				return
			}
			if spec == nil {
				t.Fatalf("expected %d fields, got nil spec", tc.wantFields)
			}
			if spec.NumFields() != tc.wantFields {
				t.Errorf("fields: got %d want %d", spec.NumFields(), tc.wantFields)
			}
			i := 0
			for f := range spec.Fields() {
				if i < len(tc.wantNames) && f.Name != tc.wantNames[i] {
					t.Errorf("field %d name: got %q want %q", i, f.Name, tc.wantNames[i])
				}
				i++
			}
		})
	}
}

// TestBuildPartitionSpecFromCols verifies explicit partition spec
// construction from column names.
func TestBuildPartitionSpecFromCols(t *testing.T) {
	schema := icebergpkg.NewSchema(0,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.PrimitiveTypes.Int32, Required: true},
		icebergpkg.NestedField{ID: 2, Name: "region", Type: icebergpkg.PrimitiveTypes.String, Required: false},
		icebergpkg.NestedField{ID: 3, Name: "date", Type: icebergpkg.PrimitiveTypes.Date, Required: false},
	)

	spec, err := buildPartitionSpecFromCols("region,date", schema)
	if err != nil {
		t.Fatalf("buildPartitionSpecFromCols: %v", err)
	}
	if spec.NumFields() != 2 {
		t.Fatalf("fields: got %d want 2", spec.NumFields())
	}

	_, err = buildPartitionSpecFromCols("nonexistent", schema)
	if err == nil {
		t.Error("expected error for nonexistent column")
	}
}

// Silence unused import warnings — bytes is used by readParquetSchema
// which is tested indirectly via TestImportUnpartitioned.
var _ = bytes.NewReader
