package janitor

import (
	"testing"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
)

// TestPickAnchor_RoundRobinLargeFiles verifies that successive rotation
// indices pick different large files for the same partition. The
// round-robin distributes stitch wear across all large files rather
// than always growing the same one.
func TestPickAnchor_RoundRobinLargeFiles(t *testing.T) {
	p := analyzer.PartitionHealth{
		PartitionKey: "ss_store_sk=5",
		LargeFiles: []analyzer.FileInfo{
			{Path: "data/a.parquet", Bytes: 100 << 20},
			{Path: "data/b.parquet", Bytes: 100 << 20},
			{Path: "data/c.parquet", Bytes: 100 << 20},
		},
		SmallFiles: []analyzer.FileInfo{
			{Path: "data/small1.parquet", Bytes: 1 << 10},
		},
	}

	seen := map[string]bool{}
	for i := int64(0); i < 10; i++ {
		f, boot := pickAnchor(p, i)
		if boot {
			t.Errorf("rotation %d: expected bootstrap=false (large files exist)", i)
		}
		if f.Path == "" {
			t.Errorf("rotation %d: expected non-empty anchor", i)
		}
		seen[f.Path] = true
	}

	// Over 10 rotations we should hit all 3 large files (a, b, c).
	if len(seen) != 3 {
		t.Errorf("expected to see all 3 large files over 10 rotations, saw %d: %v", len(seen), seen)
	}
}

// TestPickAnchor_Deterministic verifies that the same inputs produce
// the same output (no hidden state, no randomness).
func TestPickAnchor_Deterministic(t *testing.T) {
	p := analyzer.PartitionHealth{
		PartitionKey: "event_date=2026-04-10",
		LargeFiles: []analyzer.FileInfo{
			{Path: "data/a.parquet", Bytes: 100 << 20},
			{Path: "data/b.parquet", Bytes: 100 << 20},
		},
	}
	first, _ := pickAnchor(p, 42)
	for i := 0; i < 5; i++ {
		f, _ := pickAnchor(p, 42)
		if f.Path != first.Path {
			t.Errorf("non-deterministic: run %d got %s, want %s", i, f.Path, first.Path)
		}
	}
}

// TestPickAnchor_UnsortedInput verifies that input order does not
// affect the picked anchor — the function sorts internally for
// deterministic rotation.
func TestPickAnchor_UnsortedInput(t *testing.T) {
	sortedInput := analyzer.PartitionHealth{
		PartitionKey: "x=1",
		LargeFiles: []analyzer.FileInfo{
			{Path: "a.parquet", Bytes: 100 << 20},
			{Path: "b.parquet", Bytes: 100 << 20},
			{Path: "c.parquet", Bytes: 100 << 20},
		},
	}
	reversedInput := analyzer.PartitionHealth{
		PartitionKey: "x=1",
		LargeFiles: []analyzer.FileInfo{
			{Path: "c.parquet", Bytes: 100 << 20},
			{Path: "b.parquet", Bytes: 100 << 20},
			{Path: "a.parquet", Bytes: 100 << 20},
		},
	}
	for i := int64(0); i < 5; i++ {
		s, _ := pickAnchor(sortedInput, i)
		r, _ := pickAnchor(reversedInput, i)
		if s.Path != r.Path {
			t.Errorf("rotation %d: sorted=%s, reversed=%s (expected equal)", i, s.Path, r.Path)
		}
	}
}

// TestPickAnchor_Bootstrap verifies that when no large files exist,
// the function picks the biggest small file as the anchor and returns
// bootstrap=true.
func TestPickAnchor_Bootstrap(t *testing.T) {
	p := analyzer.PartitionHealth{
		PartitionKey: "x=1",
		SmallFiles: []analyzer.FileInfo{
			{Path: "small_1kb.parquet", Bytes: 1 << 10},
			{Path: "small_100kb.parquet", Bytes: 100 << 10},
			{Path: "small_10kb.parquet", Bytes: 10 << 10},
		},
	}
	f, boot := pickAnchor(p, 0)
	if !boot {
		t.Error("expected bootstrap=true when no large files exist")
	}
	if f.Path != "small_100kb.parquet" {
		t.Errorf("expected biggest small file (100kb), got %s", f.Path)
	}
}

// TestPickAnchor_Empty verifies the empty-partition edge case.
func TestPickAnchor_Empty(t *testing.T) {
	p := analyzer.PartitionHealth{PartitionKey: "x=1"}
	f, boot := pickAnchor(p, 0)
	if f.Path != "" {
		t.Errorf("expected empty anchor, got %s", f.Path)
	}
	if boot {
		t.Error("expected bootstrap=false for empty partition")
	}
}

// TestHashKey_Deterministic verifies hashKey is stable across calls.
func TestHashKey_Deterministic(t *testing.T) {
	inputs := []string{"", "a", "ss_store_sk=5", "event_date=2026-04-10/region=US"}
	for _, in := range inputs {
		h1 := hashKey(in)
		h2 := hashKey(in)
		if h1 != h2 {
			t.Errorf("hashKey(%q) non-deterministic: %d vs %d", in, h1, h2)
		}
		if h1 < 0 {
			t.Errorf("hashKey(%q) returned negative: %d", in, h1)
		}
	}
}

// TestHashKey_Distributes verifies distinct inputs produce distinct
// hashes (best-effort — hash collisions are theoretically possible).
func TestHashKey_Distributes(t *testing.T) {
	inputs := []string{
		"ss_store_sk=1", "ss_store_sk=2", "ss_store_sk=3",
		"ss_store_sk=4", "ss_store_sk=5",
	}
	seen := map[int64]bool{}
	for _, in := range inputs {
		h := hashKey(in)
		if seen[h] {
			t.Errorf("hash collision on inputs: %v", inputs)
		}
		seen[h] = true
	}
}
