package janitor_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestCompactHot_ParallelStitch_AllPartitionsCompacted is the
// load-bearing test for the parallel partition loop in
// CompactHot. It seeds a partitioned fileblob table with N
// partitions, each containing M small files, then runs CompactHot
// and asserts:
//
//  1. CompactHot returns no top-level error
//  2. PartitionsHot reports all N partitions
//  3. PartitionsStitched reports all N partitions (no failures)
//  4. PartitionsFailed is zero
//  5. The total row count after compaction equals N × M × rowsPerFile
//     (master check would have caught any row loss; this is a
//     belt-and-suspenders top-level invariant)
//  6. The reload-from-catalog file count is < the initial file
//     count (something actually got compacted)
//
// Run with `go test -race` to validate the parallel work-list
// construction + result aggregation under contention.
func TestCompactHot_ParallelStitch_AllPartitionsCompacted(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 8
	const filesPerPartition = 4
	const rowsPerFile = 5
	expectedRows := int64(numPartitions * filesPerPartition * rowsPerFile)

	tbl, _ := w.SeedPartitionedFactTable(t, "hot", "events", numPartitions, filesPerPartition, rowsPerFile)
	if tbl == nil {
		t.Fatal("seed returned nil table")
	}

	res, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"hot", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000, // make every partition "hot"
			MinSmallFiles:           2,
		})
	if err != nil {
		t.Fatalf("CompactHot: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}

	if res.PartitionsAnalyzed != numPartitions {
		t.Errorf("PartitionsAnalyzed = %d, want %d", res.PartitionsAnalyzed, numPartitions)
	}
	if res.PartitionsHot != numPartitions {
		t.Errorf("PartitionsHot = %d, want %d (HotWindowSnapshots=1000 should make every partition hot)",
			res.PartitionsHot, numPartitions)
	}
	if res.PartitionsStitched != numPartitions {
		t.Errorf("PartitionsStitched = %d, want %d", res.PartitionsStitched, numPartitions)
	}
	if res.PartitionsFailed != 0 {
		t.Errorf("PartitionsFailed = %d, want 0", res.PartitionsFailed)
	}
	if res.PartitionsSkipped != 0 {
		t.Errorf("PartitionsSkipped = %d, want 0", res.PartitionsSkipped)
	}
	if len(res.Partitions) != numPartitions {
		t.Errorf("Partitions slice has %d entries, want %d", len(res.Partitions), numPartitions)
	}

	// Reload the table and verify the data layout post-compaction.
	reloaded := w.LoadTable(t, "hot", "events")
	files, rows := janitor.SnapshotFileStatsFast(context.Background(), reloaded)
	if int64(rows) != expectedRows {
		t.Errorf("post-compact rows = %d, want %d (master check should have blocked row loss)",
			rows, expectedRows)
	}
	// Initial file count was numPartitions × filesPerPartition = 32.
	// After delta-stitch each partition collapses to ~1-2 files
	// (anchor + new stitched output, depending on whether the
	// anchor stays as a separate file). Either way, the total
	// should be < 32.
	initialFiles := numPartitions * filesPerPartition
	if files >= initialFiles {
		t.Errorf("post-compact file count = %d, want < %d (no compaction happened?)",
			files, initialFiles)
	}
}

// TestCompactHot_PartialFailureRecorded verifies that the parallel
// loop correctly records per-partition failures. This is a
// regression test for the result-aggregation mutex: if the mutex
// is missed, concurrent goroutines updating PartitionsFailed and
// the Partitions slice will race or lose updates.
//
// We can't easily induce a real partition failure inline (it
// would require fault injection into the catalog or the parquet
// stitch). Instead, this test sanity-checks the success-only
// case under -race: the result counters and slice must be
// internally consistent regardless of concurrent updates.
func TestCompactHot_ResultCountersUnderRace(t *testing.T) {
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedPartitionedFactTable(t, "race", "events", 16, 3, 5)
	if tbl == nil {
		t.Fatal("seed returned nil table")
	}

	res, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"race", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000,
			MinSmallFiles:           2,
		})
	if err != nil {
		t.Fatalf("CompactHot: %v", err)
	}

	// Counter consistency: stitched + failed + skipped == hot.
	total := res.PartitionsStitched + res.PartitionsFailed + res.PartitionsSkipped
	if total != res.PartitionsHot {
		t.Errorf("counter mismatch: stitched(%d)+failed(%d)+skipped(%d)=%d, want PartitionsHot=%d",
			res.PartitionsStitched, res.PartitionsFailed, res.PartitionsSkipped, total, res.PartitionsHot)
	}
	if len(res.Partitions) != res.PartitionsHot {
		t.Errorf("Partitions slice has %d entries, want %d (PartitionsHot)",
			len(res.Partitions), res.PartitionsHot)
	}
}
