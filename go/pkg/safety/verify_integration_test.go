package safety_test

import (
	"context"
	"io"
	"testing"
	"time"

	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

func readManifestEntries(m icebergpkg.ManifestFile, r io.ReadCloser) ([]icebergpkg.ManifestEntry, error) {
	return icebergpkg.ReadManifest(m, r, true)
}

// TestVerifyCompactionConsistency_RowCountPreserved is the golden-path
// test for the master check: compact a table, stage the transaction,
// and verify I1 (row count) + I2 (schema) + I7 (manifest refs) pass.
func TestVerifyCompactionConsistency_RowCountPreserved(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "mc", "compact", 4, 10)

	// Build a compaction transaction: replace all 4 files with 1.
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		t.Fatal("no current snapshot")
	}
	fs, err := tbl.FS(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Collect all data file paths from the manifest.
	manifests, err := snap.Manifests(fs)
	if err != nil {
		t.Fatal(err)
	}
	var oldPaths []string
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			t.Fatal(err)
		}
		entries, erra := readManifestEntries(m, mf)
		mf.Close()
		if erra != nil {
			t.Fatal(erra)
		}
		for _, e := range entries {
			if df := e.DataFile(); df != nil {
				oldPaths = append(oldPaths, df.FilePath())
			}
		}
	}
	if len(oldPaths) != 4 {
		t.Fatalf("expected 4 data files, got %d", len(oldPaths))
	}

	// Write a new parquet file containing ALL the rows.
	allRows := make([]testutil.SimpleFactRow, 0, 40)
	for i := int64(0); i < 40; i++ {
		allRows = append(allRows, testutil.SimpleFactRow{ID: i, Value: i % 10, Region: "us-east-1"})
	}
	newPath := w.WriteParquetFile(t, "mc.db/compact/data/compacted.parquet", allRows)

	tx := tbl.NewTransaction()
	if err := tx.ReplaceDataFiles(context.Background(), oldPaths, []string{newPath}, nil); err != nil {
		t.Fatalf("ReplaceDataFiles: %v", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatalf("StagedTable: %v", err)
	}

	v, err := safety.VerifyCompactionConsistency(context.Background(), tbl, staged, w.Cat.Props())
	if err != nil {
		t.Fatalf("VerifyCompactionConsistency: %v", err)
	}
	if v.Overall != "pass" {
		t.Errorf("Overall = %s, want pass. I1=%s I2=%s I7=%s",
			v.Overall, v.I1RowCount.Result, v.I2Schema.Result, v.I7ManifestRefs.Result)
	}
	if v.I1RowCount.In != 40 || v.I1RowCount.Out != 40 {
		t.Errorf("I1 row count: in=%d out=%d, want 40/40", v.I1RowCount.In, v.I1RowCount.Out)
	}
}

// TestVerifyCompactionConsistency_RowCountMismatchFails verifies that
// the master check FAILS when the staged output has a different row
// count than the input. This is the I1 invariant: compaction must
// not lose or gain rows.
func TestVerifyCompactionConsistency_RowCountMismatchFails(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "mc", "mismatch", 3, 10)

	snap := tbl.CurrentSnapshot()
	fs, _ := tbl.FS(context.Background())
	manifests, _ := snap.Manifests(fs)
	var oldPaths []string
	for _, m := range manifests {
		mf, _ := fs.Open(m.FilePath())
		entries, _ := readManifestEntries(m, mf)
		mf.Close()
		for _, e := range entries {
			if df := e.DataFile(); df != nil {
				oldPaths = append(oldPaths, df.FilePath())
			}
		}
	}

	// Write a new file with FEWER rows than the input (20 instead of 30).
	shortRows := make([]testutil.SimpleFactRow, 20)
	for i := range shortRows {
		shortRows[i] = testutil.SimpleFactRow{ID: int64(i), Value: 0, Region: "us"}
	}
	newPath := w.WriteParquetFile(t, "mc.db/mismatch/data/short.parquet", shortRows)

	tx := tbl.NewTransaction()
	if err := tx.ReplaceDataFiles(context.Background(), oldPaths, []string{newPath}, nil); err != nil {
		t.Fatalf("ReplaceDataFiles: %v", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatalf("StagedTable: %v", err)
	}

	v, err := safety.VerifyCompactionConsistency(context.Background(), tbl, staged, w.Cat.Props())
	if err == nil {
		t.Fatal("expected master check to FAIL on row count mismatch, got nil error")
	}
	if v.I1RowCount.Result != "fail" {
		t.Errorf("I1 result = %s, want fail", v.I1RowCount.Result)
	}
	if v.Overall != "fail" {
		t.Errorf("Overall = %s, want fail", v.Overall)
	}
}

// TestVerifyExpireConsistency_SingleSnapshot verifies that expiring a
// table with a single snapshot is a no-op that passes all checks.
func TestVerifyExpireConsistency_SingleSnapshot(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "mc", "expire1", 2, 5)

	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(
		icebergtable.WithRetainLast(1),
		icebergtable.WithOlderThan(time.Hour),
	); err != nil {
		t.Fatalf("ExpireSnapshots: %v", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		t.Fatalf("StagedTable: %v", err)
	}

	v, err := safety.VerifyExpireConsistency(context.Background(), tbl, staged, w.Cat.Props())
	if err != nil {
		t.Fatalf("VerifyExpireConsistency: %v", err)
	}
	if v.Overall != "pass" {
		t.Errorf("Overall = %s, want pass. I1=%s I2=%s SnapshotRetain=%s",
			v.Overall, v.I1RowCount.Result, v.I2Schema.Result, v.SnapshotRetain.Result)
	}
}

// TestVerifyExpireConsistency_AfterMultipleCommits verifies that the
// expire master check passes after dropping an old snapshot. The
// current snapshot's row count must be unchanged.
func TestVerifyExpireConsistency_AfterMultipleCommits(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "mc", "expire2", 2, 10)

	// Add a second commit.
	abs := w.WriteParquetFile(t, "mc.db/expire2/data/extra.parquet",
		[]testutil.SimpleFactRow{{ID: 100, Value: 1, Region: "us"}, {ID: 101, Value: 2, Region: "us"}})
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), []string{abs}, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	tbl2, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Expire with KeepLast=1.
	tx2 := tbl2.NewTransaction()
	if err := tx2.ExpireSnapshots(
		icebergtable.WithRetainLast(1),
		icebergtable.WithOlderThan(time.Nanosecond),
	); err != nil {
		t.Fatalf("ExpireSnapshots: %v", err)
	}
	staged, err := tx2.StagedTable()
	if err != nil {
		t.Fatalf("StagedTable: %v", err)
	}

	v, err := safety.VerifyExpireConsistency(context.Background(), tbl2, staged, w.Cat.Props())
	if err != nil {
		t.Fatalf("VerifyExpireConsistency: %v", err)
	}
	if v.Overall != "pass" {
		t.Errorf("Overall = %s, want pass", v.Overall)
	}
	if v.I1RowCount.Result != "pass" {
		t.Errorf("I1 = %s (%s)", v.I1RowCount.Result, v.I1RowCount.Reason)
	}
	if v.SnapshotRetain.Result != "pass" {
		t.Errorf("SnapshotRetain = %s (%s)", v.SnapshotRetain.Result, v.SnapshotRetain.Reason)
	}
	if v.SnapshotRetain.Removed < 1 {
		t.Errorf("SnapshotRetain.Removed = %d, want >= 1", v.SnapshotRetain.Removed)
	}
}

// TestVerifyCompactionConsistency_NilInputsFail is the defensive
// guard: passing nil before or staged must return an error.
func TestVerifyCompactionConsistency_NilInputsFail(t *testing.T) {
	_, err := safety.VerifyCompactionConsistency(context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil inputs")
	}
}

func TestVerifyExpireConsistency_NilInputsFail(t *testing.T) {
	_, err := safety.VerifyExpireConsistency(context.Background(), nil, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil inputs")
	}
}
