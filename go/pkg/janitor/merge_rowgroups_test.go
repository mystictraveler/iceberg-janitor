package janitor_test

import (
	"bytes"
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"
	pqgo "github.com/parquet-go/parquet-go"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestMergeRowGroups_StitchThenMerge verifies that CompactHot's
// post-stitch row group merge fires when the stitched output has
// more than maxRowGroupsPerFile (4) row groups. Seeds a table with
// 8 files (→ 8 row groups after stitch), runs CompactHot, then
// verifies the committed output has ≤ 4 row groups.
func TestMergeRowGroups_StitchThenMerge(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 2
	const filesPerPartition = 8 // > maxRowGroupsPerFile (4), triggers merge
	const rowsPerFile = 5
	expectedRows := int64(numPartitions * filesPerPartition * rowsPerFile)

	_, _ = w.SeedPartitionedFactTable(t, "mrg", "events", numPartitions, filesPerPartition, rowsPerFile)

	res, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"mrg", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000,
			MinSmallFiles:           2,
		})
	if err != nil {
		t.Fatalf("CompactHot: %v", err)
	}
	if res.PartitionsStitched != numPartitions {
		t.Errorf("PartitionsStitched = %d, want %d", res.PartitionsStitched, numPartitions)
	}
	if res.PartitionsFailed != 0 {
		t.Errorf("PartitionsFailed = %d, want 0", res.PartitionsFailed)
	}

	// Reload table and verify row count preserved.
	reloaded := w.LoadTable(t, "mrg", "events")
	_, rows := janitor.SnapshotFileStatsFast(context.Background(), reloaded)
	if rows != expectedRows {
		t.Errorf("post-compact rows = %d, want %d", rows, expectedRows)
	}

	// Read the output files and check row group counts.
	// After merge, each output file should have ≤ 4 row groups
	// (was 8 before merge = filesPerPartition).
	snap := reloaded.CurrentSnapshot()
	fs, _ := reloaded.FS(context.Background())
	paths := manifestDataFilePaths(t, snap, fs)

	for _, p := range paths {
		f, err := fs.Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		info, _ := f.Stat()
		data := make([]byte, info.Size())
		f.ReadAt(data, 0)
		f.Close()

		pf, err := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		rgCount := len(pf.Metadata().RowGroups)
		if rgCount > 4 {
			t.Errorf("file %s has %d row groups, want ≤ 4 (merge should have fired for files with > 4 RGs)", p, rgCount)
		}
		t.Logf("file %s: %d row groups, %d rows", p, rgCount, func() int64 {
			var n int64
			for _, rg := range pf.Metadata().RowGroups {
				n += rg.NumRows
			}
			return n
		}())
	}
}

// TestMergeRowGroups_SkipsSmallFiles verifies that files with
// ≤ maxRowGroupsPerFile row groups are NOT re-encoded. This
// protects the hot-path performance: a stitch of 2-3 files should
// stay as byte-copy, not trigger an expensive decode/encode.
func TestMergeRowGroups_SkipsSmallFiles(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 2
	const filesPerPartition = 3 // ≤ 4, should NOT trigger merge
	const rowsPerFile = 5

	_, _ = w.SeedPartitionedFactTable(t, "nomrg", "events", numPartitions, filesPerPartition, rowsPerFile)

	res, err := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"nomrg", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000,
			MinSmallFiles:           2,
		})
	if err != nil {
		t.Fatalf("CompactHot: %v", err)
	}
	if res.PartitionsStitched != numPartitions {
		t.Errorf("PartitionsStitched = %d, want %d", res.PartitionsStitched, numPartitions)
	}

	// Verify the output files have exactly filesPerPartition row
	// groups (stitch preserved them, merge didn't fire).
	reloaded := w.LoadTable(t, "nomrg", "events")
	snap := reloaded.CurrentSnapshot()
	fs, _ := reloaded.FS(context.Background())
	paths := manifestDataFilePaths(t, snap, fs)

	for _, p := range paths {
		f, err := fs.Open(p)
		if err != nil {
			t.Fatalf("open %s: %v", p, err)
		}
		info, _ := f.Stat()
		data := make([]byte, info.Size())
		f.ReadAt(data, 0)
		f.Close()

		pf, err := pqgo.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("parse %s: %v", p, err)
		}
		rgCount := len(pf.Metadata().RowGroups)
		// With 3 files per partition stitched, expect 3 row groups
		// (merge should NOT have fired since 3 ≤ 4).
		if rgCount != filesPerPartition {
			t.Errorf("file %s has %d row groups, want %d (merge should NOT fire for ≤ 4 RGs)", p, rgCount, filesPerPartition)
		}
	}
}
