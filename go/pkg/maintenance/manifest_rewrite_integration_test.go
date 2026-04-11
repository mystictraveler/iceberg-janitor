package maintenance_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

func TestRewriteManifests_PreservesDataFilesAndRows(t *testing.T) {
	// Seed a table with several files. Rewrite-manifests must
	// preserve every data file path and every row — it only
	// reorganizes the manifest layer.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "rm", "rewrite_basic", 8, 15)

	res, err := maintenance.RewriteManifests(context.Background(), w.Cat,
		icebergtable.Identifier{"rm", "rewrite_basic"},
		maintenance.RewriteManifestsOptions{MinManifestsToTrigger: 0})
	if err != nil {
		t.Fatalf("RewriteManifests: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if res.BeforeRows != 120 {
		t.Errorf("BeforeRows = %d, want 120", res.BeforeRows)
	}
	if res.AfterRows != res.BeforeRows {
		t.Errorf("AfterRows (%d) != BeforeRows (%d) — master check should have blocked this", res.AfterRows, res.BeforeRows)
	}
	if res.BeforeDataFiles != res.AfterDataFiles {
		t.Errorf("rewrite-manifests must preserve data file count: Before=%d After=%d", res.BeforeDataFiles, res.AfterDataFiles)
	}
	if res.BeforeDataFiles != 8 {
		t.Errorf("BeforeDataFiles = %d, want 8", res.BeforeDataFiles)
	}
}

func TestRewriteManifests_BelowTriggerThresholdNoop(t *testing.T) {
	// MinManifestsToTrigger guard: if the table has fewer manifests
	// than the threshold, rewrite-manifests should early-exit
	// without making a new commit.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "rm", "below_trigger", 2, 5)

	// Threshold of 100 manifests is way above what seed produces
	// (1 manifest from the single AddFiles commit). Should be a no-op.
	res, err := maintenance.RewriteManifests(context.Background(), w.Cat,
		icebergtable.Identifier{"rm", "below_trigger"},
		maintenance.RewriteManifestsOptions{MinManifestsToTrigger: 100})
	if err != nil {
		t.Fatalf("RewriteManifests: %v", err)
	}
	if res.BeforeSnapshotID != res.AfterSnapshotID {
		t.Errorf("expected no-op but snapshot changed: Before=%d After=%d",
			res.BeforeSnapshotID, res.AfterSnapshotID)
	}
}
