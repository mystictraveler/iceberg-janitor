package catalog_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

func TestNewDirectoryCatalog_Fileblob(t *testing.T) {
	dir := t.TempDir()
	url := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(context.Background(), "test", url, nil)
	if err != nil {
		t.Fatalf("NewDirectoryCatalog: %v", err)
	}
	defer cat.Close()

	if cat.Bucket() == nil {
		t.Error("Bucket() is nil")
	}
	if cat.WarehouseURL() != url {
		t.Errorf("WarehouseURL() = %q, want %q", cat.WarehouseURL(), url)
	}
	if cat.CatalogType() != "directory" {
		t.Errorf("CatalogType() = %q, want directory", cat.CatalogType())
	}
	props := cat.Props()
	if props == nil {
		t.Error("Props() is nil, want empty map")
	}
}

func TestLoadTable_NotFound(t *testing.T) {
	dir := t.TempDir()
	url := "file://" + dir
	cat, err := catalog.NewDirectoryCatalog(context.Background(), "test", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()

	_, err = cat.LoadTable(context.Background(), icebergtable.Identifier{"ns", "nonexistent"})
	if err == nil {
		t.Fatal("expected error loading nonexistent table")
	}
}

func TestCreateTable_LoadTable_RoundTrip(t *testing.T) {
	w := testutil.NewWarehouse(t)
	schema := testutil.SimpleFactSchema()

	tbl := w.CreateTable(t, "rt", "events", schema)
	if tbl == nil {
		t.Fatal("CreateTable returned nil")
	}

	loaded := w.LoadTable(t, "rt", "events")
	if loaded == nil {
		t.Fatal("LoadTable returned nil")
	}
	if loaded.Metadata().CurrentSchema().ID != schema.ID {
		t.Errorf("loaded schema ID = %d, want %d", loaded.Metadata().CurrentSchema().ID, schema.ID)
	}
}

func TestCreateTable_CommitTable_LoadTable(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "commit", "data", 3, 10)

	snap := tbl.CurrentSnapshot()
	if snap == nil {
		t.Fatal("no current snapshot after SeedFactTable")
	}
	if snap.SnapshotID == 0 {
		t.Error("snapshot ID is 0")
	}

	// Reload and verify the snapshot persisted.
	loaded := w.LoadTable(t, "commit", "data")
	loadedSnap := loaded.CurrentSnapshot()
	if loadedSnap == nil {
		t.Fatal("reloaded table has no current snapshot")
	}
	if loadedSnap.SnapshotID != snap.SnapshotID {
		t.Errorf("reloaded snapshot ID = %d, want %d", loadedSnap.SnapshotID, snap.SnapshotID)
	}
}

func TestListTables(t *testing.T) {
	w := testutil.NewWarehouse(t)
	w.CreateTable(t, "list", "t1", testutil.SimpleFactSchema())
	w.CreateTable(t, "list", "t2", testutil.SimpleFactSchema())

	count := 0
	for ident, err := range w.Cat.ListTables(context.Background(), icebergtable.Identifier{"list.db"}) {
		if err != nil {
			t.Fatalf("ListTables iteration: %v", err)
		}
		_ = ident
		count++
	}
	if count < 2 {
		t.Errorf("ListTables returned %d tables, want >= 2", count)
	}
}

func TestMetadataLocation_AfterCommit(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "ml", "data", 2, 5)

	loc := tbl.MetadataLocation()
	if loc == "" {
		t.Error("MetadataLocation is empty after SeedFactTable")
	}

	// Add more data and commit — metadata location should change.
	abs := w.WriteParquetFile(t, "ml.db/data/data/extra.parquet",
		[]testutil.SimpleFactRow{{ID: 100, Value: 1, Region: "us"}})
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), []string{abs}, nil, false); err != nil {
		t.Fatal(err)
	}
	tbl2, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	loc2 := tbl2.MetadataLocation()
	if loc2 == "" {
		t.Error("MetadataLocation is empty after second commit")
	}
	if loc2 == loc {
		t.Error("MetadataLocation didn't change after commit — should point to new version")
	}
}
