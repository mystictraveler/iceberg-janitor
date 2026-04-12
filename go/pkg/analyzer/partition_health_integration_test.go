package analyzer_test

import (
	"context"
	"testing"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestAnalyzePartitions_UnpartitionedTable verifies that an
// unpartitioned table produces a single PartitionHealth record
// with all files lumped together.
func TestAnalyzePartitions_UnpartitionedTable(t *testing.T) {
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "ap", "unpart", 5, 10)
	tbl := w.LoadTable(t, "ap", "unpart")

	parts, err := analyzer.AnalyzePartitions(context.Background(), tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: 128 * 1024 * 1024,
		HotWindowSnapshots:      1000,
	})
	if err != nil {
		t.Fatalf("AnalyzePartitions: %v", err)
	}
	if len(parts) != 1 {
		t.Errorf("expected 1 partition group for unpartitioned table, got %d", len(parts))
	}
	if parts[0].FileCount != 5 {
		t.Errorf("FileCount = %d, want 5", parts[0].FileCount)
	}
	if parts[0].SmallFileCount != 5 {
		t.Errorf("SmallFileCount = %d, want 5 (all files are small)", parts[0].SmallFileCount)
	}
	if parts[0].TotalRows != 50 {
		t.Errorf("TotalRows = %d, want 50", parts[0].TotalRows)
	}
}

// TestAnalyzePartitions_PartitionedTable verifies that a partitioned
// table produces one PartitionHealth per partition, with the correct
// file count per partition.
func TestAnalyzePartitions_PartitionedTable(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numPartitions = 6
	const filesPerPartition = 3
	const rowsPerFile = 4
	_, _ = w.SeedPartitionedFactTable(t, "ap", "parts", numPartitions, filesPerPartition, rowsPerFile)
	tbl := w.LoadTable(t, "ap", "parts")

	parts, err := analyzer.AnalyzePartitions(context.Background(), tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: 128 * 1024 * 1024,
		HotWindowSnapshots:      1000,
	})
	if err != nil {
		t.Fatalf("AnalyzePartitions: %v", err)
	}
	if len(parts) != numPartitions {
		t.Errorf("got %d partitions, want %d", len(parts), numPartitions)
	}
	for _, p := range parts {
		if p.FileCount != filesPerPartition {
			t.Errorf("partition %s: FileCount = %d, want %d", p.PartitionKey, p.FileCount, filesPerPartition)
		}
		if p.SmallFileCount != filesPerPartition {
			t.Errorf("partition %s: SmallFileCount = %d, want %d", p.PartitionKey, p.SmallFileCount, filesPerPartition)
		}
		expectedRows := int64(filesPerPartition * rowsPerFile)
		if p.TotalRows != expectedRows {
			t.Errorf("partition %s: TotalRows = %d, want %d", p.PartitionKey, p.TotalRows, expectedRows)
		}
	}
}

// TestAnalyzePartitions_HotWindowAll verifies that with a large
// HotWindowSnapshots, all partitions are marked as hot.
func TestAnalyzePartitions_HotWindowAll(t *testing.T) {
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedPartitionedFactTable(t, "ap", "hot", 4, 2, 5)
	tbl := w.LoadTable(t, "ap", "hot")

	parts, err := analyzer.AnalyzePartitions(context.Background(), tbl, analyzer.PartitionHealthOptions{
		SmallFileThresholdBytes: 128 * 1024 * 1024,
		HotWindowSnapshots:      10000,
	})
	if err != nil {
		t.Fatal(err)
	}
	hot := 0
	for _, p := range parts {
		if p.IsHot {
			hot++
		}
	}
	if hot != len(parts) {
		t.Errorf("%d/%d partitions hot, want all hot with HotWindowSnapshots=10000", hot, len(parts))
	}
}

// TestAnalyzePartitions_NilSnapshot handles a table with no
// current snapshot (freshly created, no data).
func TestAnalyzePartitions_NilSnapshot(t *testing.T) {
	w := testutil.NewWarehouse(t)
	w.CreateTable(t, "ap", "empty", testutil.SimpleFactSchema())
	tbl := w.LoadTable(t, "ap", "empty")

	parts, err := analyzer.AnalyzePartitions(context.Background(), tbl, analyzer.PartitionHealthOptions{})
	if err != nil {
		t.Fatalf("AnalyzePartitions on empty table: %v", err)
	}
	if parts != nil {
		t.Errorf("expected nil partitions for empty table, got %d", len(parts))
	}
}
