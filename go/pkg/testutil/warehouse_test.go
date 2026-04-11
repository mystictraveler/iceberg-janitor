package testutil

import (
	"context"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
)

// TestSeedFactTableBasic is a smoke test for the fixture itself:
// after seeding, the table must be loadable via the catalog and
// report the expected file count + row count in its current snapshot.
func TestSeedFactTableBasic(t *testing.T) {
	w := NewWarehouse(t)
	const numFiles = 5
	const rowsPerFile = 10

	tbl, keys := w.SeedFactTable(t, "testns", "events", numFiles, rowsPerFile)
	if tbl == nil {
		t.Fatal("SeedFactTable returned nil table")
	}
	if len(keys) != numFiles {
		t.Errorf("keys count = %d, want %d", len(keys), numFiles)
	}

	// Reload via the catalog and walk the current snapshot to verify
	// numFiles data files and (numFiles × rowsPerFile) rows are
	// visible.
	reloaded := w.LoadTable(t, "testns", "events")
	snap := reloaded.CurrentSnapshot()
	if snap == nil {
		t.Fatal("reloaded table has no current snapshot")
	}

	ctx := context.Background()
	fs, err := reloaded.FS(ctx)
	if err != nil {
		t.Fatalf("FS: %v", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatalf("Manifests: %v", err)
	}

	totalFiles := 0
	totalRows := int64(0)
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			t.Fatalf("open manifest: %v", err)
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			t.Fatalf("read manifest: %v", err)
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil {
				continue
			}
			totalFiles++
			totalRows += df.Count()
		}
	}

	if totalFiles != numFiles {
		t.Errorf("snapshot data file count = %d, want %d", totalFiles, numFiles)
	}
	if totalRows != int64(numFiles*rowsPerFile) {
		t.Errorf("snapshot row count = %d, want %d", totalRows, int64(numFiles*rowsPerFile))
	}
}
