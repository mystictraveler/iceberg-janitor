package janitor_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestCompact_DryRun verifies that a dry-run Compact walks the
// manifest list to produce oldPaths + expectedRows, then stops
// before writing a new parquet file or committing. The projected
// AfterFiles count equals BeforeFiles - PlannedOldFiles + 1 (one
// new stitched output per call).
func TestCompact_DryRun(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numFiles = 6
	const rowsPerFile = 5
	_, _ = w.SeedFactTable(t, "dry", "compact", numFiles, rowsPerFile)

	tblBefore := w.LoadTable(t, "dry", "compact")
	beforeSnapID := tblBefore.CurrentSnapshot().SnapshotID
	beforeFiles, beforeRows := janitor.SnapshotFileStatsFast(context.Background(), tblBefore)

	res, err := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"dry", "compact"},
		janitor.CompactOptions{
			// No TargetFileSizeBytes — this forces Pattern A (rewrite
			// every file in the partition), which is the clearest
			// dry-run case: BeforeFiles → 1.
			DryRun: true,
		})
	if err != nil {
		t.Fatalf("Compact dry-run: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}

	if !res.DryRun {
		t.Error("result.DryRun = false, want true")
	}
	if res.BeforeFiles != beforeFiles {
		t.Errorf("BeforeFiles = %d, want %d", res.BeforeFiles, beforeFiles)
	}
	if res.BeforeRows != beforeRows {
		t.Errorf("BeforeRows = %d, want %d", res.BeforeRows, beforeRows)
	}
	if res.PlannedOldFiles == 0 {
		t.Error("PlannedOldFiles = 0 on dry-run — manifest walk should have found the seeded files")
	}
	if res.PlannedNewFiles != 1 {
		t.Errorf("PlannedNewFiles = %d, want 1", res.PlannedNewFiles)
	}
	wantAfterFiles := res.BeforeFiles - res.PlannedOldFiles + 1
	if res.AfterFiles != wantAfterFiles {
		t.Errorf("AfterFiles = %d, want %d (before - planned_old + 1)", res.AfterFiles, wantAfterFiles)
	}
	if res.AfterRows != res.BeforeRows {
		t.Errorf("AfterRows (%d) != BeforeRows (%d) — dry-run must not project row drift", res.AfterRows, res.BeforeRows)
	}
	if res.AfterSnapshotID != res.BeforeSnapshotID {
		t.Errorf("AfterSnapshotID (%d) != BeforeSnapshotID (%d) — dry-run must not advance the snapshot id",
			res.AfterSnapshotID, res.BeforeSnapshotID)
	}
	if res.ContentionDetected {
		t.Error("ContentionDetected = true on an idle table")
	}

	// Reload: confirm no commit happened and file set is untouched.
	tblAfter := w.LoadTable(t, "dry", "compact")
	if got := tblAfter.CurrentSnapshot().SnapshotID; got != beforeSnapID {
		t.Errorf("post dry-run current snapshot id = %d, want %d", got, beforeSnapID)
	}
	afterFiles, afterRows := janitor.SnapshotFileStatsFast(context.Background(), tblAfter)
	if afterFiles != beforeFiles {
		t.Errorf("post dry-run file count = %d, want %d", afterFiles, beforeFiles)
	}
	if afterRows != beforeRows {
		t.Errorf("post dry-run row count = %d, want %d", afterRows, beforeRows)
	}
}
