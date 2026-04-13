package janitor_test

import (
	"bytes"
	"context"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	pqgo "github.com/parquet-go/parquet-go"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/testutil"
)

// TestMergeRowGroups_SortedByDefaultSortOrder verifies that when a
// table has a default sort order defined, the row group merge sorts
// the output rows by those columns. This produces tighter min/max
// column stats for better predicate pushdown.
func TestMergeRowGroups_SortedByDefaultSortOrder(t *testing.T) {
	w := testutil.NewWarehouse(t)

	// Create a table with a sort order on the "id" column (ascending).
	schema := testutil.SimpleFactSchema()
	idField, ok := schema.FindFieldByName("id")
	if !ok {
		t.Fatal("id field not found")
	}
	sortOrder, err := icebergtable.NewSortOrder(
		1,
		[]icebergtable.SortField{{
			SourceID:  idField.ID,
			Transform: icebergpkg.IdentityTransform{},
			Direction: icebergtable.SortASC,
			NullOrder: icebergtable.NullsFirst,
		}},
	)
	if err != nil {
		t.Fatalf("NewSortOrder: %v", err)
	}

	tbl := w.CreateTable(t, "sort", "events", schema,
		icebergcat.WithSortOrder(sortOrder))

	// Seed with files that have OUT-OF-ORDER ids across files.
	// File 0: ids 40-47 (high)
	// File 1: ids 0-7 (low)
	// File 2: ids 24-31 (mid)
	// File 3: ids 8-15 (low-mid)
	// File 4: ids 32-39 (high-mid)
	// File 5: ids 16-23 (mid-low)
	batches := []struct {
		start int64
		count int
	}{
		{40, 8}, {0, 8}, {24, 8}, {8, 8}, {32, 8}, {16, 8},
	}
	var allPaths []string
	for i, batch := range batches {
		key := "sort.db/events/data/part-" + string(rune('A'+i)) + ".parquet"
		rows := make([]testutil.SimpleFactRow, batch.count)
		for j := 0; j < batch.count; j++ {
			rows[j] = testutil.SimpleFactRow{
				ID:     batch.start + int64(j),
				Value:  int64(j),
				Region: "us-east-1",
			}
		}
		abs := w.WriteParquetFile(t, key, rows)
		allPaths = append(allPaths, abs)
	}
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(context.Background(), allPaths, nil, false); err != nil {
		t.Fatalf("AddFiles: %v", err)
	}
	tbl, err = tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Run CompactHot — should stitch 6 files → 1, then merge
	// (6 > maxRowGroupsPerFile=4), then sort by id ASC.
	res, cerr := janitor.CompactHot(context.Background(), w.Cat,
		icebergtable.Identifier{"sort", "events"},
		janitor.CompactHotOptions{
			SmallFileThresholdBytes: 128 * 1024 * 1024,
			HotWindowSnapshots:      1000,
			MinSmallFiles:           2,
		})
	if cerr != nil {
		t.Fatalf("CompactHot: %v", cerr)
	}
	if res.PartitionsStitched == 0 {
		t.Fatal("nothing stitched")
	}

	// Read the output file and verify rows are sorted by id ASC.
	reloaded := w.LoadTable(t, "sort", "events")
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

		// Check row group count (should be 1 after merge).
		rgCount := len(pf.Metadata().RowGroups)
		if rgCount > maxRowGroupsForTest {
			t.Errorf("file %s has %d row groups, want ≤ %d", p, rgCount, maxRowGroupsForTest)
		}

		// Read rows and verify sorted by id ASC.
		reader := pqgo.NewGenericReader[testutil.SimpleFactRow](pf)
		var rows []testutil.SimpleFactRow
		buf := make([]testutil.SimpleFactRow, 256)
		for {
			n, err := reader.Read(buf)
			rows = append(rows, buf[:n]...)
			if err != nil {
				break
			}
		}

		if len(rows) != 48 {
			t.Errorf("expected 48 rows, got %d", len(rows))
			continue
		}

		// Verify monotonically increasing ids (sorted).
		for i := 1; i < len(rows); i++ {
			if rows[i].ID < rows[i-1].ID {
				t.Errorf("rows NOT sorted by id ASC: row[%d].ID=%d < row[%d].ID=%d",
					i, rows[i].ID, i-1, rows[i-1].ID)
				break
			}
		}

		// Verify the FIRST id is 0 and the LAST is 47.
		if rows[0].ID != 0 {
			t.Errorf("first row ID = %d, want 0", rows[0].ID)
		}
		if rows[len(rows)-1].ID != 47 {
			t.Errorf("last row ID = %d, want 47", rows[len(rows)-1].ID)
		}

		t.Logf("file %s: %d row groups, %d rows, sorted id range [%d, %d]",
			p, rgCount, len(rows), rows[0].ID, rows[len(rows)-1].ID)
	}
}

const maxRowGroupsForTest = 4
