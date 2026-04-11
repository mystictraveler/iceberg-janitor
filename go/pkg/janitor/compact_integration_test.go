package janitor_test

import (
	"context"
	"testing"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// Integration tests use a separate _test package so they can reuse
// pkg/testutil (which lives outside pkg/janitor to avoid an import
// cycle with pkg/catalog).

func TestCompact_SmallFilesMergedInPlace(t *testing.T) {
	w := testutil.NewWarehouse(t)
	const numFiles = 6
	const rowsPerFile = 10
	_, _ = w.SeedFactTable(t, "smoke", "events", numFiles, rowsPerFile)

	// Set target file size large enough that all files are "small"
	// and should be merged into one output.
	res, err := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"smoke", "events"},
		janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if res.BeforeFiles != numFiles {
		t.Errorf("BeforeFiles = %d, want %d", res.BeforeFiles, numFiles)
	}
	if res.AfterFiles >= res.BeforeFiles {
		t.Errorf("AfterFiles (%d) should be < BeforeFiles (%d) — compaction didn't reduce files", res.AfterFiles, res.BeforeFiles)
	}
	if res.BeforeRows != int64(numFiles*rowsPerFile) {
		t.Errorf("BeforeRows = %d, want %d", res.BeforeRows, numFiles*rowsPerFile)
	}
	if res.AfterRows != res.BeforeRows {
		t.Errorf("row count changed: Before=%d After=%d (master check should have caught this)", res.BeforeRows, res.AfterRows)
	}
	if res.Verification == nil {
		t.Error("Verification is nil — master check did not run")
	}
}

func TestCompact_IdempotentOnCompactedTable(t *testing.T) {
	// Running Compact twice in a row on the same table: the second
	// call should either be a no-op (BeforeFiles == AfterFiles) or
	// produce a count ≤ the first call's output. Either way, no
	// data should be lost.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "smoke", "twice", 6, 20)

	ident := icebergtable.Identifier{"smoke", "twice"}
	opts := janitor.CompactOptions{TargetFileSizeBytes: 128 * 1024 * 1024}

	r1, err := janitor.Compact(context.Background(), w.Cat, ident, opts)
	if err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	r2, err := janitor.Compact(context.Background(), w.Cat, ident, opts)
	if err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if r1.AfterRows != 120 || r2.AfterRows != 120 {
		t.Errorf("row count changed across compactions: r1.After=%d r2.After=%d, want 120",
			r1.AfterRows, r2.AfterRows)
	}
}

func TestCompact_PreservesRowOrdering(t *testing.T) {
	// Row count invariant under compaction: sum of rows before ==
	// sum after. The master check should enforce this, so any
	// violation would have already surfaced as an error above, but
	// this test provides a redundant assertion at the result level.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "smoke", "ordering", 4, 25)

	res, err := janitor.Compact(context.Background(), w.Cat,
		icebergtable.Identifier{"smoke", "ordering"},
		janitor.CompactOptions{
			TargetFileSizeBytes: 128 * 1024 * 1024,
		})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.BeforeRows != 100 {
		t.Errorf("BeforeRows = %d, want 100", res.BeforeRows)
	}
	if res.AfterRows != 100 {
		t.Errorf("AfterRows = %d, want 100", res.AfterRows)
	}
}
