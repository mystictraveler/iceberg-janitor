package maintenance_test

import (
	"context"
	"testing"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

func TestExpire_KeepLastOne_NoOldSnapshots(t *testing.T) {
	// A freshly-seeded table has exactly ONE snapshot (the commit
	// that AddFiles made). KeepLast=1 should retain it and
	// NothingToDo the expiration.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "exp", "one_snap", 3, 5)

	res, err := maintenance.Expire(context.Background(), w.Cat,
		icebergtable.Identifier{"exp", "one_snap"},
		maintenance.ExpireOptions{KeepLast: 1, KeepWithin: time.Hour})
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
	if res.BeforeSnapshots != 1 {
		t.Errorf("BeforeSnapshots = %d, want 1", res.BeforeSnapshots)
	}
	if res.AfterSnapshots != 1 {
		t.Errorf("AfterSnapshots = %d, want 1 (single snapshot must be retained)", res.AfterSnapshots)
	}
	if res.BeforeRows != 15 {
		t.Errorf("BeforeRows = %d, want 15", res.BeforeRows)
	}
}

func TestExpire_AfterMultipleCommits(t *testing.T) {
	// Seed a table, then make another commit via SeedFactTable's
	// secondary path: we'll manually add more files to the existing
	// table. After two commits there should be two snapshots in the
	// parent chain. Expire with KeepLast=1 should retain only the
	// current one — but the row count (current snapshot only) is
	// unchanged.
	w := testutil.NewWarehouse(t)
	tbl, _ := w.SeedFactTable(t, "exp", "multi", 2, 10)

	// Add a second batch of files via a manual Transaction.AddFiles
	// call, which creates a second snapshot on the main branch.
	secondKey := "exp.db/multi/data/part-10000.parquet"
	abs := w.WriteParquetFile(t, secondKey, []testutil.SimpleFactRow{
		{ID: 100, Value: 1, Region: "us"},
		{ID: 101, Value: 2, Region: "us"},
	})
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), []string{abs}, nil, false); err != nil {
		t.Fatalf("second AddFiles: %v", err)
	}
	if _, err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("second Commit: %v", err)
	}

	// Now the table has 2 snapshots. Expire with KeepLast=1,
	// KeepWithin=0 should drop the first one.
	res, err := maintenance.Expire(context.Background(), w.Cat,
		icebergtable.Identifier{"exp", "multi"},
		maintenance.ExpireOptions{KeepLast: 1, KeepWithin: time.Nanosecond})
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if res.BeforeSnapshots < 2 {
		t.Errorf("BeforeSnapshots = %d, want >= 2", res.BeforeSnapshots)
	}
	// AfterSnapshots must retain at least 1 (the current one).
	if res.AfterSnapshots < 1 {
		t.Errorf("AfterSnapshots = %d, want >= 1", res.AfterSnapshots)
	}
	if res.AfterSnapshots > res.BeforeSnapshots {
		t.Errorf("AfterSnapshots (%d) > BeforeSnapshots (%d) — expire must not add snapshots", res.AfterSnapshots, res.BeforeSnapshots)
	}
}

func TestExpire_RowCountInvariant(t *testing.T) {
	// The master check for Expire asserts the CURRENT snapshot's
	// row count is unchanged. Verify this holds end-to-end.
	w := testutil.NewWarehouse(t)
	_, _ = w.SeedFactTable(t, "exp", "rows", 4, 10)

	res, err := maintenance.Expire(context.Background(), w.Cat,
		icebergtable.Identifier{"exp", "rows"},
		maintenance.ExpireOptions{KeepLast: 1})
	if err != nil {
		t.Fatalf("Expire: %v", err)
	}
	if res.BeforeRows != 40 {
		t.Errorf("BeforeRows = %d, want 40", res.BeforeRows)
	}
	if res.AfterRows != res.BeforeRows {
		t.Errorf("AfterRows (%d) != BeforeRows (%d) — master check should have blocked this", res.AfterRows, res.BeforeRows)
	}
}
