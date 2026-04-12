package maintenance_test

import (
	"context"
	"testing"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestExpire_DryRun verifies that a dry-run expire stages the
// snapshot removal plan, runs the master check, populates the
// projected After* fields + RemovedSnapshotIDs, but does NOT commit.
// The table's metadata.json generation must be unchanged after the
// call.
func TestExpire_DryRun(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "dry", "expire", 2, 10)

	// Add a second commit so there's actually a snapshot to drop.
	second := w.WriteParquetFile(t, "dry.db/expire/data/part-10000.parquet",
		[]testutil.SimpleFactRow{
			{ID: 100, Value: 1, Region: "us"},
			{ID: 101, Value: 2, Region: "us"},
		})
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), []string{second}, nil, false); err != nil {
		t.Fatalf("second AddFiles: %v", err)
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	tblBefore := w.LoadTable(t, "dry", "expire")
	beforeSnapID := tblBefore.CurrentSnapshot().SnapshotID
	beforeSnapCount := len(tblBefore.Metadata().Snapshots())

	res, err := maintenance.Expire(context.Background(), w.Cat,
		icebergtable.Identifier{"dry", "expire"},
		maintenance.ExpireOptions{
			KeepLast:   1,
			KeepWithin: time.Nanosecond,
			DryRun:     true,
		})
	if err != nil {
		t.Fatalf("Expire dry-run: %v", err)
	}
	if !res.DryRun {
		t.Error("result.DryRun = false, want true")
	}
	if res.BeforeSnapshots != beforeSnapCount {
		t.Errorf("BeforeSnapshots = %d, want %d", res.BeforeSnapshots, beforeSnapCount)
	}
	// Dry run should still compute which snapshots WOULD be removed.
	if len(res.RemovedSnapshotIDs) == 0 {
		t.Error("RemovedSnapshotIDs is empty on dry-run — expected at least one projected removal")
	}
	if res.AfterSnapshots >= res.BeforeSnapshots {
		t.Errorf("AfterSnapshots (%d) >= BeforeSnapshots (%d) — dry-run should reflect projected shrink",
			res.AfterSnapshots, res.BeforeSnapshots)
	}

	// Reload the table — current snapshot id MUST be unchanged and
	// the actual snapshot count must match beforeSnapCount (no
	// commit happened).
	tblAfter := w.LoadTable(t, "dry", "expire")
	if got := tblAfter.CurrentSnapshot().SnapshotID; got != beforeSnapID {
		t.Errorf("post dry-run current snapshot id = %d, want %d (no commit must have happened)", got, beforeSnapID)
	}
	if got := len(tblAfter.Metadata().Snapshots()); got != beforeSnapCount {
		t.Errorf("post dry-run snapshot count = %d, want %d", got, beforeSnapCount)
	}
}

// TestRewriteManifests_DryRun verifies that a dry-run rewrite walks
// the full manifest list, computes the projected consolidated
// count, but writes no new manifest files and does not call
// CommitTable.
func TestRewriteManifests_DryRun(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "dry", "rewrite", 2, 10)

	// Add several more batches so the manifest list is large enough
	// to trip MinManifestsToTrigger. Each AddFiles call creates one
	// new manifest on the snapshot's manifest list.
	for i := 0; i < 5; i++ {
		key := "dry.db/rewrite/data/part-1000" + string(rune('0'+i)) + ".parquet"
		abs := w.WriteParquetFile(t, key,
			[]testutil.SimpleFactRow{
				{ID: int64(100 + i*2), Value: 1, Region: "us"},
				{ID: int64(101 + i*2), Value: 2, Region: "us"},
			})
		tx := tbl.NewTransaction()
		if err := tx.AddFiles(context.Background(), []string{abs}, nil, false); err != nil {
			t.Fatalf("AddFiles i=%d: %v", i, err)
		}
		newTbl, err := tx.Commit(context.Background())
		if err != nil {
			t.Fatalf("Commit i=%d: %v", i, err)
		}
		tbl = newTbl
	}

	tblBefore := w.LoadTable(t, "dry", "rewrite")
	beforeSnapID := tblBefore.CurrentSnapshot().SnapshotID
	beforeFiles, beforeRows := janitor.SnapshotFileStatsFast(context.Background(), tblBefore)

	res, err := maintenance.RewriteManifests(context.Background(), w.Cat,
		icebergtable.Identifier{"dry", "rewrite"},
		maintenance.RewriteManifestsOptions{
			MinManifestsToTrigger: 2,
			DryRun:                true,
		})
	if err != nil {
		t.Fatalf("RewriteManifests dry-run: %v", err)
	}
	if !res.DryRun {
		t.Error("result.DryRun = false, want true")
	}
	if res.BeforeManifests < 2 {
		t.Errorf("BeforeManifests = %d, want >= 2", res.BeforeManifests)
	}
	if res.AfterManifests == 0 {
		t.Error("AfterManifests = 0 on a dry-run that should project consolidation")
	}
	if res.BeforeDataFiles == 0 {
		t.Error("BeforeDataFiles = 0 — the dry-run must have walked manifest entries")
	}
	if res.AfterDataFiles != res.BeforeDataFiles {
		t.Errorf("AfterDataFiles (%d) != BeforeDataFiles (%d) — rewrite preserves file set", res.AfterDataFiles, res.BeforeDataFiles)
	}
	if res.BeforeRows != res.AfterRows {
		t.Errorf("row count drifted in dry-run: before=%d after=%d", res.BeforeRows, res.AfterRows)
	}

	// Reload and confirm no commit happened.
	tblAfter := w.LoadTable(t, "dry", "rewrite")
	if got := tblAfter.CurrentSnapshot().SnapshotID; got != beforeSnapID {
		t.Errorf("post dry-run snapshot id = %d, want %d (no commit must have happened)", got, beforeSnapID)
	}
	afterFiles, afterRows := janitor.SnapshotFileStatsFast(context.Background(), tblAfter)
	if afterFiles != beforeFiles {
		t.Errorf("post dry-run file count = %d, want %d", afterFiles, beforeFiles)
	}
	if afterRows != beforeRows {
		t.Errorf("post dry-run row count = %d, want %d", afterRows, beforeRows)
	}
}
