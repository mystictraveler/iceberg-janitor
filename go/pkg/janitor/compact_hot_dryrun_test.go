package janitor_test

import (
	"context"
	"strings"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestCompactHot_DryRun_NoSideEffects verifies that a dry-run
// CompactHot does NOT write any new parquet files and does NOT
// advance the table's current snapshot. The planning phase must
// still run end-to-end — result.PartitionsPlanned and per-partition
// details are populated, and every partition entry carries
// DryRun=true.
func TestCompactHot_DryRun_NoSideEffects(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 4
	const filesPerPartition = 3
	const rowsPerFile = 5

	_, _ = w.SeedPartitionedFactTable(t, "dry", "events", numPartitions, filesPerPartition, rowsPerFile)

	tblBefore := w.LoadTable(t, "dry", "events")
	beforeSnapID := tblBefore.CurrentSnapshot().SnapshotID
	beforeFiles, beforeRows := janitor.SnapshotFileStatsFast(context.Background(), tblBefore)

	res, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"dry", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000,
			MinSmallFiles:           2,
			DryRun:                  true,
		})
	if err != nil {
		t.Fatalf("CompactHot dry-run: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}

	if !res.DryRun {
		t.Error("result.DryRun = false, want true")
	}
	if res.PartitionsPlanned != numPartitions {
		t.Errorf("PartitionsPlanned = %d, want %d", res.PartitionsPlanned, numPartitions)
	}
	if res.PartitionsStitched != 0 {
		t.Errorf("PartitionsStitched = %d, want 0 (dry-run must not stitch)", res.PartitionsStitched)
	}
	if res.PartitionsFailed != 0 {
		t.Errorf("PartitionsFailed = %d, want 0", res.PartitionsFailed)
	}
	if res.ObservedSnapshotID != beforeSnapID {
		t.Errorf("ObservedSnapshotID = %d, want %d (pre-walk load should pin the id we saw)", res.ObservedSnapshotID, beforeSnapID)
	}
	if res.ContentionDetected {
		t.Error("ContentionDetected = true on an idle table; nothing is committing concurrently")
	}
	if len(res.Partitions) != numPartitions {
		t.Errorf("Partitions slice has %d entries, want %d", len(res.Partitions), numPartitions)
	}
	for i, p := range res.Partitions {
		if !p.DryRun {
			t.Errorf("Partitions[%d].DryRun = false, want true", i)
		}
		if p.Stitched {
			t.Errorf("Partitions[%d].Stitched = true on dry-run, want false", i)
		}
		if p.BeforeFiles < 2 {
			t.Errorf("Partitions[%d].BeforeFiles = %d, want >= 2", i, p.BeforeFiles)
		}
		if p.AfterFiles != 1 {
			t.Errorf("Partitions[%d].AfterFiles = %d, want 1 (projected stitched output)", i, p.AfterFiles)
		}
	}

	// The underlying table's current snapshot must be unchanged —
	// no commit happened, so reloading must yield the same id.
	tblAfter := w.LoadTable(t, "dry", "events")
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

	// No new parquet files should have been created in the table's
	// data directory — we asserted this indirectly via snapshot id,
	// but also sanity check that the plan's anchor paths are the
	// ones the seed wrote (each should have `part-` in it).
	for _, p := range res.Partitions {
		if p.Anchor == "" {
			t.Errorf("partition %s has empty anchor on dry-run — plan should still pick an anchor", p.PartitionKey)
			continue
		}
		if !strings.Contains(p.Anchor, "part-") {
			t.Errorf("partition %s anchor = %s, looks like a NEW file name (dry-run shouldn't create anything)", p.PartitionKey, p.Anchor)
		}
	}
}

// TestCompactHot_DryRun_RealRunAfterMatchesPlan is the belt-and-
// suspenders check: run CompactHot in dry-run mode, capture
// PartitionsPlanned, then run it for real and confirm
// PartitionsStitched matches the dry-run plan. This is the
// invariant operators rely on: "dry run says N partitions, real
// run commits N partitions".
func TestCompactHot_DryRun_RealRunAfterMatchesPlan(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 5
	const filesPerPartition = 3
	const rowsPerFile = 4

	_, _ = w.SeedPartitionedFactTable(t, "plan", "events", numPartitions, filesPerPartition, rowsPerFile)

	opts := janitor.CompactHotOptions{
		SmallFileThresholdBytes: 128 * 1024 * 1024,
		HotWindowSnapshots:      1000,
		MinSmallFiles:           2,
	}

	dryOpts := opts
	dryOpts.DryRun = true
	dry, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"plan", "events"}, dryOpts)
	if err != nil {
		t.Fatalf("dry CompactHot: %v", err)
	}
	if dry.PartitionsPlanned != numPartitions {
		t.Fatalf("dry PartitionsPlanned = %d, want %d", dry.PartitionsPlanned, numPartitions)
	}

	real, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"plan", "events"}, opts)
	if err != nil {
		t.Fatalf("real CompactHot: %v", err)
	}
	if real.PartitionsStitched != dry.PartitionsPlanned {
		t.Errorf("real PartitionsStitched = %d, dry PartitionsPlanned = %d (plan vs real mismatch)",
			real.PartitionsStitched, dry.PartitionsPlanned)
	}
	if real.PartitionsFailed != 0 {
		t.Errorf("real PartitionsFailed = %d, want 0", real.PartitionsFailed)
	}
}
