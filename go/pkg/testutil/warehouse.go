// Package testutil provides a fileblob-backed warehouse fixture for
// integration tests. Tests that need a real Iceberg table with real
// parquet files can call NewWarehouse + SeedFactTable instead of
// hand-rolling the full CreateTable + AddFiles dance.
//
// The fixture is intentionally minimal — it is NOT a parity fixture
// for TPC-DS or the streamer. It exists to let pkg/janitor,
// pkg/maintenance, and pkg/safety exercise their end-to-end paths
// against a real Iceberg table without requiring docker or the bench
// harness.
//
// All helpers take testing.TB so they can be called from both
// *testing.T (integration tests) and *testing.B (benchmarks).
package testutil

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	_ "gocloud.dev/blob/fileblob"

	"github.com/parquet-go/parquet-go"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
)

// SimpleFactRow is the in-memory row shape written by SeedFactTable.
// Field names match SimpleFactSchema so that iceberg-go's AddFiles
// recognizes the parquet footer columns and maps them to schema
// field ids 1, 2, 3.
type SimpleFactRow struct {
	ID     int64  `parquet:"id"`
	Value  int64  `parquet:"value"`
	Region string `parquet:"region"`
}

// Warehouse is a throwaway fileblob-backed warehouse rooted at a
// per-test temporary directory. The catalog is closed automatically
// via t.Cleanup when NewWarehouse returns.
type Warehouse struct {
	Dir string
	URL string
	Cat *catalog.DirectoryCatalog
}

// NewWarehouse creates a fresh file:// warehouse at t.TempDir() and
// opens a DirectoryCatalog on it. The catalog is registered with
// t.Cleanup so tests do not need to close it.
func NewWarehouse(t testing.TB) *Warehouse {
	t.Helper()
	dir := t.TempDir()
	url := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(context.Background(), "test", url, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	return &Warehouse{Dir: dir, URL: url, Cat: cat}
}

// SimpleFactSchema returns an iceberg schema compatible with
// SimpleFactRow. Field ids are 1, 2, 3 and match the parquet column
// names via struct tags.
func SimpleFactSchema() *icebergpkg.Schema {
	return icebergpkg.NewSchema(1,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.Int64Type{}, Required: true},
		icebergpkg.NestedField{ID: 2, Name: "value", Type: icebergpkg.Int64Type{}},
		icebergpkg.NestedField{ID: 3, Name: "region", Type: icebergpkg.StringType{}},
	)
}

// CreateTable wraps DirectoryCatalog.CreateTable with t.Fatal on
// failure so the test reads linearly.
func (w *Warehouse) CreateTable(t testing.TB, ns, name string, schema *icebergpkg.Schema, opts ...icebergcat.CreateTableOpt) *icebergtable.Table {
	t.Helper()
	ident := icebergtable.Identifier{ns, name}
	tbl, err := w.Cat.CreateTable(context.Background(), ident, schema, opts...)
	if err != nil {
		t.Fatalf("CreateTable(%s.%s): %v", ns, name, err)
	}
	return tbl
}

// LoadTable loads an existing table by (namespace, name). Useful after
// a Compact/Expire/RewriteManifests call that mutates the table —
// callers reload to see the post-op state.
func (w *Warehouse) LoadTable(t testing.TB, ns, name string) *icebergtable.Table {
	t.Helper()
	ident := icebergtable.Identifier{ns, name}
	tbl, err := w.Cat.LoadTable(context.Background(), ident)
	if err != nil {
		t.Fatalf("LoadTable(%s.%s): %v", ns, name, err)
	}
	return tbl
}

// WriteParquetFile writes a parquet file containing the given rows
// at the given bucket-relative key. Returns the absolute file://
// URL the file lives at, which AddFiles can consume directly.
func (w *Warehouse) WriteParquetFile(t testing.TB, key string, rows []SimpleFactRow) string {
	t.Helper()
	absPath := filepath.Join(w.Dir, key)
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	f, err := os.Create(absPath)
	if err != nil {
		t.Fatalf("create %s: %v", absPath, err)
	}
	defer f.Close()

	pw := parquet.NewGenericWriter[SimpleFactRow](f)
	if _, err := pw.Write(rows); err != nil {
		t.Fatalf("parquet write %s: %v", absPath, err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("parquet close %s: %v", absPath, err)
	}
	return "file://" + absPath
}

// SeedPartitionedFactTable creates a table partitioned by `region`
// with `numPartitions` partitions, each containing
// `filesPerPartition` parquet files of `rowsPerFile` rows. Used by
// tests that need to exercise parallel multi-partition compaction.
//
// Each partition value is a synthetic region name ("r000", "r001",
// ...). The parquet files for partition rNNN live under
// `<warehouse>/<ns>.db/<name>/data/region=rNNN/part-NNNNN.parquet`.
// AddFiles infers the partition value from the path on read, so
// partition=region=rNNN files end up in the rNNN partition.
//
// Total row count = numPartitions × filesPerPartition × rowsPerFile.
func (w *Warehouse) SeedPartitionedFactTable(t testing.TB, ns, name string, numPartitions, filesPerPartition, rowsPerFile int) (*icebergtable.Table, []string) {
	t.Helper()
	schema := SimpleFactSchema()
	regionField, ok := schema.FindFieldByName("region")
	if !ok {
		t.Fatal("region field not found in SimpleFactSchema")
	}
	spec := icebergpkg.NewPartitionSpec(icebergpkg.PartitionField{
		SourceID:  regionField.ID,
		FieldID:   1000,
		Name:      "region",
		Transform: icebergpkg.IdentityTransform{},
	})
	tbl := w.CreateTable(t, ns, name, schema, icebergcat.WithPartitionSpec(&spec))

	absFiles := make([]string, 0, numPartitions*filesPerPartition)
	keys := make([]string, 0, numPartitions*filesPerPartition)
	rowID := int64(0)
	for p := 0; p < numPartitions; p++ {
		region := fmt.Sprintf("r%03d", p)
		for f := 0; f < filesPerPartition; f++ {
			key := fmt.Sprintf("%s.db/%s/data/region=%s/part-%05d.parquet", ns, name, region, f)
			rows := make([]SimpleFactRow, rowsPerFile)
			for j := 0; j < rowsPerFile; j++ {
				rows[j] = SimpleFactRow{
					ID:     rowID,
					Value:  int64(j),
					Region: region,
				}
				rowID++
			}
			abs := w.WriteParquetFile(t, key, rows)
			absFiles = append(absFiles, abs)
			keys = append(keys, key)
		}
	}

	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), absFiles, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	newTbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return newTbl, keys
}

// SeedFactTable creates a table with numFiles small parquet files,
// each containing rowsPerFile rows with monotonically increasing
// ids. Returns the committed table (after AddFiles) and the list
// of bucket-relative keys of the parquet files. Total row count =
// numFiles × rowsPerFile.
func (w *Warehouse) SeedFactTable(t testing.TB, ns, name string, numFiles, rowsPerFile int) (*icebergtable.Table, []string) {
	t.Helper()
	tbl := w.CreateTable(t, ns, name, SimpleFactSchema())

	absFiles := make([]string, 0, numFiles)
	keys := make([]string, 0, numFiles)
	rowID := int64(0)
	for i := 0; i < numFiles; i++ {
		key := fmt.Sprintf("%s.db/%s/data/part-%05d.parquet", ns, name, i)
		rows := make([]SimpleFactRow, rowsPerFile)
		for j := 0; j < rowsPerFile; j++ {
			rows[j] = SimpleFactRow{
				ID:     rowID,
				Value:  int64(j),
				Region: "us-east-1",
			}
			rowID++
		}
		abs := w.WriteParquetFile(t, key, rows)
		absFiles = append(absFiles, abs)
		keys = append(keys, key)
	}

	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), absFiles, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	newTbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return newTbl, keys
}
