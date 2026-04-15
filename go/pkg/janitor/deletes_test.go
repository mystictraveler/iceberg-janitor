package janitor_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/parquet-go/parquet-go"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
)

// TestPositionDeleteSet_Contains exercises the binary-search membership
// check on small, medium, and edge-case inputs.
func TestPositionDeleteSet_Contains(t *testing.T) {
	bundle := &janitor.DeleteBundle{
		PositionDeletes: map[string]*janitor.PositionDeleteSet{},
	}
	// Seed via the loader indirectly by constructing a file and loading.
	// For a pure-unit test we go through LoadPositionDeletes with a
	// file we write ourselves, to keep the set's internal construction
	// consistent with the prod path.
	dir := t.TempDir()
	path := filepath.Join(dir, "pos.parquet")
	writePositionDeleteFile(t, path, []positionDelRow{
		{FilePath: "s3://bucket/data/a.parquet", Pos: 1},
		{FilePath: "s3://bucket/data/a.parquet", Pos: 3},
		{FilePath: "s3://bucket/data/a.parquet", Pos: 5},
		{FilePath: "s3://bucket/data/a.parquet", Pos: 100},
		{FilePath: "s3://bucket/data/b.parquet", Pos: 0},
	})

	fs := localIO{root: dir}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos.parquet", bundle); err != nil {
		t.Fatalf("LoadPositionDeletes: %v", err)
	}

	setA := bundle.PositionDeletes["s3://bucket/data/a.parquet"]
	if setA == nil {
		t.Fatal("no set for file a")
	}
	for _, want := range []int64{1, 3, 5, 100} {
		if !setA.Contains(want) {
			t.Errorf("setA.Contains(%d) = false, want true", want)
		}
	}
	for _, notWant := range []int64{0, 2, 4, 6, 99, 101} {
		if setA.Contains(notWant) {
			t.Errorf("setA.Contains(%d) = true, want false", notWant)
		}
	}
	if setA.Len() != 4 {
		t.Errorf("setA.Len() = %d, want 4", setA.Len())
	}

	setB := bundle.PositionDeletes["s3://bucket/data/b.parquet"]
	if setB == nil || !setB.Contains(0) || setB.Len() != 1 {
		t.Errorf("setB bad: %+v", setB)
	}

	// Non-existent file path — Contains on nil set must be safe.
	var nilSet *janitor.PositionDeleteSet
	if nilSet.Contains(0) {
		t.Error("nil set Contains should be false")
	}
}

// TestLoadPositionDeletes_MergeMultipleFiles verifies that loading two
// delete files targeting the same source file merges + dedups.
func TestLoadPositionDeletes_MergeMultipleFiles(t *testing.T) {
	dir := t.TempDir()
	p1 := filepath.Join(dir, "pos1.parquet")
	p2 := filepath.Join(dir, "pos2.parquet")
	writePositionDeleteFile(t, p1, []positionDelRow{
		{FilePath: "data/a.parquet", Pos: 1},
		{FilePath: "data/a.parquet", Pos: 3},
		{FilePath: "data/a.parquet", Pos: 5},
	})
	writePositionDeleteFile(t, p2, []positionDelRow{
		{FilePath: "data/a.parquet", Pos: 3}, // duplicate
		{FilePath: "data/a.parquet", Pos: 7},
		{FilePath: "data/a.parquet", Pos: 9},
	})

	fs := localIO{root: dir}
	bundle := &janitor.DeleteBundle{}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos1.parquet", bundle); err != nil {
		t.Fatalf("LoadPositionDeletes p1: %v", err)
	}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos2.parquet", bundle); err != nil {
		t.Fatalf("LoadPositionDeletes p2: %v", err)
	}

	set := bundle.PositionDeletes["data/a.parquet"]
	if set == nil {
		t.Fatal("no set after merge")
	}
	if set.Len() != 5 {
		t.Errorf("merged Len() = %d, want 5 (1,3,5,7,9)", set.Len())
	}
	for _, want := range []int64{1, 3, 5, 7, 9} {
		if !set.Contains(want) {
			t.Errorf("missing %d after merge", want)
		}
	}
}

// TestBuildRowMask_PositionDeletes masks a synthetic Arrow batch using
// a position delete set.
func TestBuildRowMask_PositionDeletes(t *testing.T) {
	mem := memory.NewGoAllocator()
	batch := makeBatch(t, mem, []int64{10, 11, 12, 13, 14}, []string{"a", "b", "c", "d", "e"})
	defer batch.Release()

	dir := t.TempDir()
	writePositionDeleteFile(t, filepath.Join(dir, "pos.parquet"), []positionDelRow{
		{FilePath: "data/src.parquet", Pos: 1},
		{FilePath: "data/src.parquet", Pos: 3},
	})
	fs := localIO{root: dir}
	bundle := &janitor.DeleteBundle{}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos.parquet", bundle); err != nil {
		t.Fatalf("load: %v", err)
	}

	mask := janitor.BuildRowMask("data/src.parquet", 0, 0, batch, bundle, map[string]int{})
	want := []bool{true, false, true, false, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}
	if got := janitor.CountDroppedRows(mask); got != 2 {
		t.Errorf("dropped = %d, want 2", got)
	}
}

// TestBuildRowMask_PositionDeletes_Offset verifies that rowIndexOffset
// correctly translates batch-local row indices to source-file-level
// positions (critical when a source file is read in multiple batches).
func TestBuildRowMask_PositionDeletes_Offset(t *testing.T) {
	mem := memory.NewGoAllocator()
	// This batch represents rows [100, 101, 102, 103, 104] of the
	// source file. Position-delete 102 should mask row index 2.
	batch := makeBatch(t, mem, []int64{10, 11, 12, 13, 14}, []string{"a", "b", "c", "d", "e"})
	defer batch.Release()

	dir := t.TempDir()
	writePositionDeleteFile(t, filepath.Join(dir, "pos.parquet"), []positionDelRow{
		{FilePath: "data/src.parquet", Pos: 102},
	})
	fs := localIO{root: dir}
	bundle := &janitor.DeleteBundle{}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos.parquet", bundle); err != nil {
		t.Fatalf("load: %v", err)
	}

	mask := janitor.BuildRowMask("data/src.parquet", 0, 100, batch, bundle, map[string]int{})
	want := []bool{true, true, false, true, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}
}

// TestBuildRowMask_EqualityDeletes_Int64 covers the predicate path for
// a single int64 equality column.
func TestBuildRowMask_EqualityDeletes_Int64(t *testing.T) {
	mem := memory.NewGoAllocator()
	batch := makeBatch(t, mem, []int64{10, 11, 12, 13, 14}, []string{"a", "b", "c", "d", "e"})
	defer batch.Release()

	bundle := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{
			SeqNum:      5,
			FieldIDs:    []int{1},
			ColumnNames: []string{"id"},
			Predicates: []janitor.EqualityTuple{
				{int64(11)},
				{int64(13)},
			},
			FilePath: "eq.parquet",
		}},
	}

	arrowIdx := map[string]int{"id": 0, "region": 1}

	// sourceSeqNum < delete SeqNum → predicates apply.
	mask := janitor.BuildRowMask("data/src.parquet", 3, 0, batch, bundle, arrowIdx)
	want := []bool{true, false, true, false, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}

	// sourceSeqNum >= delete SeqNum → predicates should NOT apply.
	mask2 := janitor.BuildRowMask("data/src.parquet", 5, 0, batch, bundle, arrowIdx)
	for i, v := range mask2 {
		if !v {
			t.Errorf("mask2[%d] = false, want true (delete doesn't apply to newer file)", i)
		}
	}
}

// TestBuildRowMask_EqualityDeletes_MultiColumn uses a composite key
// (id + region) where BOTH must match.
func TestBuildRowMask_EqualityDeletes_MultiColumn(t *testing.T) {
	mem := memory.NewGoAllocator()
	batch := makeBatch(t, mem, []int64{10, 10, 11, 11, 12}, []string{"us", "eu", "us", "eu", "us"})
	defer batch.Release()

	bundle := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{
			SeqNum:      10,
			FieldIDs:    []int{1, 3},
			ColumnNames: []string{"id", "region"},
			Predicates: []janitor.EqualityTuple{
				{int64(10), "eu"}, // deletes row 1 only
				{int64(11), "us"}, // deletes row 2 only
			},
			FilePath: "eq.parquet",
		}},
	}
	arrowIdx := map[string]int{"id": 0, "region": 1}
	mask := janitor.BuildRowMask("data/src.parquet", 1, 0, batch, bundle, arrowIdx)
	want := []bool{true, false, false, true, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v (row was id=%d region=%s)",
				i, mask[i], want[i], []int64{10, 10, 11, 11, 12}[i],
				[]string{"us", "eu", "us", "eu", "us"}[i])
		}
	}
}

// TestBuildRowMask_PositionAndEqualityCombined verifies that both
// kinds of deletes compose correctly (union of dropped rows).
func TestBuildRowMask_PositionAndEqualityCombined(t *testing.T) {
	mem := memory.NewGoAllocator()
	batch := makeBatch(t, mem, []int64{10, 11, 12, 13, 14}, []string{"a", "b", "c", "d", "e"})
	defer batch.Release()

	dir := t.TempDir()
	writePositionDeleteFile(t, filepath.Join(dir, "pos.parquet"), []positionDelRow{
		{FilePath: "data/src.parquet", Pos: 0}, // drop row 0
	})
	fs := localIO{root: dir}
	bundle := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{
			SeqNum:      100,
			FieldIDs:    []int{1},
			ColumnNames: []string{"id"},
			Predicates:  []janitor.EqualityTuple{{int64(13)}},
			FilePath:    "eq.parquet",
		}},
	}
	if err := janitor.LoadPositionDeletes(context.Background(), fs, "pos.parquet", bundle); err != nil {
		t.Fatalf("load: %v", err)
	}

	mask := janitor.BuildRowMask("data/src.parquet", 1, 0, batch, bundle, map[string]int{"id": 0, "region": 1})
	want := []bool{false, true, true, false, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}
	if got := janitor.CountDroppedRows(mask); got != 2 {
		t.Errorf("dropped = %d, want 2", got)
	}
}

// TestBuildRowMask_EqualityDeletes_NullMatch ensures NULL values in
// the predicate tuple match NULL values in the data, not non-NULLs.
func TestBuildRowMask_EqualityDeletes_NullMatch(t *testing.T) {
	mem := memory.NewGoAllocator()
	// Build batch with a nullable int64 column containing one NULL.
	b := array.NewInt64Builder(mem)
	b.Append(10)
	b.AppendNull()
	b.Append(12)
	idArr := b.NewArray()
	defer idArr.Release()
	regB := array.NewStringBuilder(mem)
	regB.Append("a")
	regB.Append("b")
	regB.Append("c")
	regArr := regB.NewArray()
	defer regArr.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil)
	batch := array.NewRecord(schema, []arrow.Array{idArr, regArr}, 3)
	defer batch.Release()

	bundle := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{
			SeqNum:      10,
			FieldIDs:    []int{1},
			ColumnNames: []string{"id"},
			Predicates:  []janitor.EqualityTuple{{nil}}, // match NULL
			FilePath:    "eq.parquet",
		}},
	}
	mask := janitor.BuildRowMask("data/src.parquet", 1, 0, batch, bundle, map[string]int{"id": 0, "region": 1})
	want := []bool{true, false, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}
}

// TestLoadEqualityDelete_RefusesComplexType verifies that a column of a
// type the janitor can't safely compare causes a clean refusal.
func TestLoadEqualityDelete_RefusesComplexType(t *testing.T) {
	schema := icebergpkg.NewSchema(1,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.Int64Type{}},
		icebergpkg.NestedField{ID: 2, Name: "ts", Type: icebergpkg.TimestampType{}},
	)
	bundle := &janitor.DeleteBundle{}

	// Path doesn't matter — we never open the file because the type
	// check runs before the parquet open.
	err := janitor.LoadEqualityDelete(context.Background(),
		localIO{root: t.TempDir()},
		"nonexistent.parquet",
		5,
		[]int{2}, // equality on ts — refused
		schema,
		bundle,
	)
	if err == nil {
		t.Fatal("expected refusal for timestamp equality delete")
	}
	if _, ok := err.(*janitor.UnsupportedFeatureError); !ok {
		t.Errorf("expected *UnsupportedFeatureError, got %T: %v", err, err)
	}
	if len(bundle.EqualityDeletes) != 0 {
		t.Errorf("bundle should be untouched on refusal")
	}
}

// TestLoadEqualityDelete_AcceptsScalars verifies that eq deletes on
// supported scalar types (int64 + string) load successfully.
func TestLoadEqualityDelete_AcceptsScalars(t *testing.T) {
	schema := icebergpkg.NewSchema(1,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.Int64Type{}},
		icebergpkg.NestedField{ID: 2, Name: "region", Type: icebergpkg.StringType{}},
	)

	dir := t.TempDir()
	path := filepath.Join(dir, "eq.parquet")
	writeEqualityDeleteFile(t, path, []eqDelRow{
		{ID: 11, Region: "us"},
		{ID: 12, Region: "eu"},
	})

	bundle := &janitor.DeleteBundle{}
	err := janitor.LoadEqualityDelete(context.Background(),
		localIO{root: dir},
		"eq.parquet",
		42,
		[]int{1, 2},
		schema,
		bundle,
	)
	if err != nil {
		t.Fatalf("LoadEqualityDelete: %v", err)
	}
	if len(bundle.EqualityDeletes) != 1 {
		t.Fatalf("want 1 eq delete, got %d", len(bundle.EqualityDeletes))
	}
	eq := bundle.EqualityDeletes[0]
	if eq.SeqNum != 42 {
		t.Errorf("SeqNum = %d, want 42", eq.SeqNum)
	}
	if len(eq.Predicates) != 2 {
		t.Errorf("Predicates len = %d, want 2", len(eq.Predicates))
	}
	// Predicate 0 should be {int64(11), "us"}.
	p0 := eq.Predicates[0]
	if p0[0] != int64(11) {
		t.Errorf("p0[0] = %v (%T), want int64(11)", p0[0], p0[0])
	}
	if p0[1] != "us" {
		t.Errorf("p0[1] = %v, want \"us\"", p0[1])
	}
}

// TestDeleteBundle_IsEmpty covers the boolean gate used by the merge
// trigger.
func TestDeleteBundle_IsEmpty(t *testing.T) {
	if !(*janitor.DeleteBundle)(nil).IsEmpty() {
		t.Error("nil bundle should be empty")
	}
	empty := &janitor.DeleteBundle{}
	if !empty.IsEmpty() {
		t.Error("zero bundle should be empty")
	}
	withPos := &janitor.DeleteBundle{
		PositionDeletes: map[string]*janitor.PositionDeleteSet{"x": {}},
	}
	if withPos.IsEmpty() {
		t.Error("bundle with pos deletes should not be empty")
	}
	withEq := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{}},
	}
	if withEq.IsEmpty() {
		t.Error("bundle with eq deletes should not be empty")
	}
}

// TestBuildRowMask_CrossWidthInt tests that int32 data matches int64
// predicate (and vice versa), since a schema-driven decode can present
// either width depending on the column type.
func TestBuildRowMask_CrossWidthInt(t *testing.T) {
	mem := memory.NewGoAllocator()
	b := array.NewInt32Builder(mem)
	b.Append(10)
	b.Append(11)
	b.Append(12)
	idArr := b.NewArray()
	defer idArr.Release()
	regB := array.NewStringBuilder(mem)
	regB.Append("a")
	regB.Append("b")
	regB.Append("c")
	regArr := regB.NewArray()
	defer regArr.Release()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil)
	batch := array.NewRecord(schema, []arrow.Array{idArr, regArr}, 3)
	defer batch.Release()

	bundle := &janitor.DeleteBundle{
		EqualityDeletes: []*janitor.EqualityDeleteFile{{
			SeqNum:      10,
			FieldIDs:    []int{1},
			ColumnNames: []string{"id"},
			Predicates:  []janitor.EqualityTuple{{int64(11)}},
			FilePath:    "eq.parquet",
		}},
	}
	mask := janitor.BuildRowMask("data/src.parquet", 1, 0, batch, bundle, map[string]int{"id": 0, "region": 1})
	want := []bool{true, false, true}
	for i := range want {
		if mask[i] != want[i] {
			t.Errorf("mask[%d] = %v, want %v", i, mask[i], want[i])
		}
	}
}

// ---------- helpers ----------

// makeBatch builds an Arrow record with int64 "id" and string "region".
func makeBatch(t testing.TB, mem memory.Allocator, ids []int64, regions []string) arrow.Record {
	t.Helper()
	if len(ids) != len(regions) {
		t.Fatal("ids/regions length mismatch")
	}
	idB := array.NewInt64Builder(mem)
	for _, v := range ids {
		idB.Append(v)
	}
	idArr := idB.NewArray()
	defer idArr.Release()

	regB := array.NewStringBuilder(mem)
	for _, v := range regions {
		regB.Append(v)
	}
	regArr := regB.NewArray()
	defer regArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil)
	return array.NewRecord(schema, []arrow.Array{idArr, regArr}, int64(len(ids)))
}

// positionDelRow matches the V2 position delete schema.
type positionDelRow struct {
	FilePath string `parquet:"file_path"`
	Pos      int64  `parquet:"pos"`
}

func writePositionDeleteFile(t testing.TB, path string, rows []positionDelRow) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	w := parquet.NewGenericWriter[positionDelRow](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// eqDelRow is the fixture row shape for the accepts-scalars test.
type eqDelRow struct {
	ID     int64  `parquet:"id"`
	Region string `parquet:"region"`
}

func writeEqualityDeleteFile(t testing.TB, path string, rows []eqDelRow) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()
	w := parquet.NewGenericWriter[eqDelRow](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// localIO is a tiny icebergio.IO implementation over a local root
// directory — used so tests don't need a warehouse just to exercise
// the delete loaders.
type localIO struct{ root string }

func (l localIO) Open(p string) (icebergio.File, error) {
	return os.Open(filepath.Join(l.root, p))
}
func (l localIO) Remove(p string) error {
	return os.Remove(filepath.Join(l.root, p))
}
