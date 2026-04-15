package testutil

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
)

// MergeOnReadTableProps returns the Iceberg table properties required
// for a table that will receive V2 position delete files via
// iceberg-go's tx.Delete API. Two knobs matter:
//
//   - format-version=2 — merge-on-read is not supported on V1.
//   - write.delete.mode=merge-on-read — opts into position-delete
//     writes (the default is copy-on-write, which rewrites data files
//     instead of emitting deletes).
//
// Tests that want to exercise the janitor's V2 delete handling MUST
// create tables with these properties; otherwise tx.Delete will
// silently rewrite the data and no delete files will ever appear in
// the manifest tree.
func MergeOnReadTableProps() icebergpkg.Properties {
	return icebergpkg.Properties{
		icebergtable.PropertyFormatVersion: "2",
		icebergtable.WriteDeleteModeKey:    icebergtable.WriteModeMergeOnRead,
	}
}

// CreateMergeOnReadTable creates an unpartitioned V2 table with the
// merge-on-read delete mode preset, so tests can fire position
// deletes via tx.Delete without extra setup.
func (w *Warehouse) CreateMergeOnReadTable(t testing.TB, ns, name string, schema *icebergpkg.Schema) *icebergtable.Table {
	t.Helper()
	return w.CreateTable(t, ns, name, schema, icebergcat.WithProperties(MergeOnReadTableProps()))
}

// FirePositionDelete issues a tx.Delete against the table using the
// provided iceberg BooleanExpression filter. On a V2 + merge-on-read
// table this writes a position delete file (not a data-file rewrite).
// The returned table is the post-commit handle.
//
// This is the primary fixture path for exercising the janitor's V2
// delete read-through code. Usage pattern:
//
//	tbl := w.CreateMergeOnReadTable(t, "db", "events", schema)
//	tbl = w.SeedFactRowsIntoTable(t, tbl, rows)
//	tbl = testutil.FirePositionDelete(t, tbl,
//	    icebergpkg.EqualTo(icebergpkg.Reference("id"), int64(5)))
// AppendSimpleFactRows seeds a table with the given SimpleFactRow
// values using iceberg-go's Arrow writer path (tx.Append). This
// populates all per-column stats in the way iceberg-go expects for
// downstream operations like tx.Delete's metrics-based file
// classification. Returns the post-commit table handle.
//
// Use this instead of the raw WriteParquetFile + AddFiles combo
// whenever the test later plans to call tx.Delete — AddFiles'
// inferred stats may not be sufficient for the metrics evaluators
// that drive iceberg-go's merge-on-read delete classification.
func AppendSimpleFactRows(t testing.TB, tbl *icebergtable.Table, rows []SimpleFactRow) *icebergtable.Table {
	t.Helper()
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil)

	idB := array.NewInt64Builder(mem)
	valB := array.NewInt64Builder(mem)
	regB := array.NewStringBuilder(mem)
	for _, r := range rows {
		idB.Append(r.ID)
		valB.Append(r.Value)
		regB.Append(r.Region)
	}
	idArr := idB.NewArray()
	defer idArr.Release()
	valArr := valB.NewArray()
	defer valArr.Release()
	regArr := regB.NewArray()
	defer regArr.Release()

	rec := array.NewRecord(schema, []arrow.Array{idArr, valArr, regArr}, int64(len(rows)))
	defer rec.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()

	tx := tbl.NewTransaction()
	if err := tx.Append(context.Background(), rdr, nil); err != nil {
		t.Fatalf("tx.Append: %v", err)
	}
	newTbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit append: %v", err)
	}
	return newTbl
}

func FirePositionDelete(t testing.TB, tbl *icebergtable.Table, filter icebergpkg.BooleanExpression) *icebergtable.Table {
	t.Helper()
	var beforeID int64
	if s := tbl.CurrentSnapshot(); s != nil {
		beforeID = s.SnapshotID
	}
	tx := tbl.NewTransaction()
	if err := tx.Delete(context.Background(), filter, nil); err != nil {
		t.Fatalf("tx.Delete: %v", err)
	}
	newTbl, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatalf("commit delete: %v", err)
	}
	var afterID int64
	if s := newTbl.CurrentSnapshot(); s != nil {
		afterID = s.SnapshotID
	}
	t.Logf("FirePositionDelete: before snap %d → after snap %d (filter=%s)", beforeID, afterID, filter)
	if afterID == beforeID {
		t.Fatalf("FirePositionDelete: snapshot did not advance — tx.Delete was a no-op (filter matched zero files)")
	}
	return newTbl
}
