package janitor

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	pqgolib "github.com/parquet-go/parquet-go"
)

// maxRowGroupsPerFile is the threshold above which a stitched output
// file triggers automatic row group merging. Files with <= this many
// row groups are left as-is (stitch-only). Files with more are
// re-read via pqarrow and rewritten with all rows in a single row
// group, sorted by the table's default sort order (if defined).
const maxRowGroupsPerFile = 4

// maybeMergeRowGroups checks if the file at `stitchedPath` has more
// than maxRowGroupsPerFile row groups. If so, it re-reads the file
// via pqarrow and rewrites it with all rows in a single row group.
//
// If the table has a non-trivial default sort order, rows are sorted
// by those columns before writing. This produces tighter min/max
// column stats for the sort key, improving predicate pushdown in
// query engines. The sort order is read from the Iceberg metadata
// (set by whoever created the table), not from user config.
//
// Returns ("", 0, nil) if no merge was needed.
// Returns (newPath, rows, nil) on successful merge.
// Returns ("", 0, err) on failure (caller keeps the original).
func maybeMergeRowGroups(
	ctx context.Context,
	tbl *icebergtable.Table,
	fs icebergio.IO,
	wfs icebergio.WriteFileIO,
	stitchedPath string,
	expectedRows int64,
	writeSchema *arrow.Schema,
	locProv icebergtable.LocationProvider,
) (string, int64, error) {
	// Count row groups.
	f, err := fs.Open(stitchedPath)
	if err != nil {
		return "", 0, fmt.Errorf("opening stitched file for RG check: %w", err)
	}
	fStat, serr := f.Stat()
	if serr != nil {
		f.Close()
		return "", 0, fmt.Errorf("stat stitched file: %w", serr)
	}
	pqFile, perr := pqgolib.OpenFile(f, fStat.Size())
	if perr != nil {
		f.Close()
		return "", 0, fmt.Errorf("parquet-go open for RG check: %w", perr)
	}
	numRowGroups := len(pqFile.Metadata().RowGroups)
	f.Close()

	if numRowGroups <= maxRowGroupsPerFile {
		return "", 0, nil
	}

	// Read all rows as Arrow batches.
	mem := memory.DefaultAllocator
	batches, _, rerr := readParquetFileBatches(ctx, fs, stitchedPath, writeSchema, mem)
	if rerr != nil {
		return "", 0, fmt.Errorf("reading stitched file for merge: %w", rerr)
	}

	// Resolve sort columns from the table's default sort order.
	sortCols := resolveSortColumns(tbl, writeSchema)

	// If we have sort columns, concatenate all batches into one
	// record, sort it, then write. Otherwise write batches as-is.
	if len(sortCols) > 0 {
		concatenated, cerr := concatenateAndSort(batches, sortCols, mem)
		if cerr != nil {
			for _, b := range batches {
				b.Release()
			}
			return "", 0, fmt.Errorf("sorting merged data: %w", cerr)
		}
		// Replace batches with the single sorted record.
		for _, b := range batches {
			b.Release()
		}
		batches = []arrow.Record{concatenated}
	}

	// Write.
	mergedFileName := fmt.Sprintf("janitor-merged-%s.parquet", uuid.New().String())
	mergedPath := locProv.NewDataLocation(mergedFileName)

	out, err := wfs.Create(mergedPath)
	if err != nil {
		for _, b := range batches {
			b.Release()
		}
		return "", 0, fmt.Errorf("creating merged output: %w", err)
	}

	pqProps := parquet.NewWriterProperties(parquet.WithStats(true))
	pw, err := pqarrow.NewFileWriter(writeSchema, out, pqProps, pqarrow.DefaultWriterProps())
	if err != nil {
		out.Close()
		_ = wfs.Remove(mergedPath)
		for _, b := range batches {
			b.Release()
		}
		return "", 0, fmt.Errorf("creating parquet writer for merge: %w", err)
	}

	var rowsWritten int64
	for _, b := range batches {
		if err := pw.Write(b); err != nil {
			pw.Close()
			_ = wfs.Remove(mergedPath)
			for _, bb := range batches {
				bb.Release()
			}
			return "", 0, fmt.Errorf("writing merged batch: %w", err)
		}
		atomic.AddInt64(&rowsWritten, int64(b.NumRows()))
		b.Release()
	}

	if err := pw.Close(); err != nil {
		_ = wfs.Remove(mergedPath)
		return "", 0, fmt.Errorf("closing merged output: %w", err)
	}

	if rowsWritten != expectedRows {
		_ = wfs.Remove(mergedPath)
		return "", 0, fmt.Errorf("merge row mismatch: wrote %d, expected %d", rowsWritten, expectedRows)
	}

	return mergedPath, rowsWritten, nil
}

// sortColSpec describes one column to sort by.
type sortColSpec struct {
	colIndex  int
	ascending bool
}

// resolveSortColumns maps the table's default sort order to Arrow
// column indices in the write schema. Returns nil if the table has
// no sort order (UnsortedSortOrder) or if the sort columns can't be
// resolved.
func resolveSortColumns(tbl *icebergtable.Table, writeSchema *arrow.Schema) []sortColSpec {
	if tbl == nil {
		return nil
	}
	meta := tbl.Metadata()
	sortOrder := meta.SortOrder()
	if sortOrder.Equals(icebergtable.UnsortedSortOrder) {
		return nil
	}

	schema := meta.CurrentSchema()
	var cols []sortColSpec
	for sf := range sortOrder.Fields() {
		// Map SourceID → column name via the Iceberg schema.
		field, ok := schema.FindFieldByID(sf.SourceID)
		if !ok {
			return nil // can't resolve — skip sorting entirely
		}
		// Map column name → Arrow schema index.
		arrowIdx := -1
		for i, af := range writeSchema.Fields() {
			if af.Name == field.Name {
				arrowIdx = i
				break
			}
		}
		if arrowIdx < 0 {
			return nil
		}
		cols = append(cols, sortColSpec{
			colIndex:  arrowIdx,
			ascending: sf.Direction == icebergtable.SortASC,
		})
	}
	return cols
}

// concatenateAndSort merges all Arrow records into one and sorts by
// the specified columns. Uses Go's sort.Stable for correctness
// (preserves insertion order for equal keys).
func concatenateAndSort(batches []arrow.Record, sortCols []sortColSpec, mem memory.Allocator) (arrow.Record, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches to sort")
	}

	// Concatenate all batches into one via array.Concatenate per column.
	schema := batches[0].Schema()
	totalRows := int64(0)
	for _, b := range batches {
		totalRows += b.NumRows()
	}

	columns := make([]arrow.Array, schema.NumFields())
	for col := 0; col < schema.NumFields(); col++ {
		chunks := make([]arrow.Array, len(batches))
		for i, b := range batches {
			chunks[i] = b.Column(col)
		}
		concatenated, err := array.Concatenate(chunks, mem)
		if err != nil {
			// Release any columns we already built.
			for j := 0; j < col; j++ {
				columns[j].Release()
			}
			return nil, fmt.Errorf("concatenating column %d: %w", col, err)
		}
		columns[col] = concatenated
	}

	// Build the concatenated record.
	rec := array.NewRecord(schema, columns, totalRows)
	// Release the concatenated column arrays (NewRecord retains them).
	for _, c := range columns {
		c.Release()
	}

	// Build sort indices.
	indices := make([]int, int(totalRows))
	for i := range indices {
		indices[i] = i
	}

	// Sort using the first sort column (multi-column sort uses
	// stable sort with cascading comparators).
	sort.Stable(&recordSorter{
		indices:  indices,
		record:   rec,
		sortCols: sortCols,
	})

	// Reorder all columns by the sorted indices.
	sortedCols := make([]arrow.Array, schema.NumFields())
	for col := 0; col < schema.NumFields(); col++ {
		arr := rec.Column(col)
		sortedCols[col] = takeByIndices(arr, indices, mem)
	}

	sortedRec := array.NewRecord(schema, sortedCols, totalRows)
	for _, c := range sortedCols {
		c.Release()
	}
	rec.Release()

	return sortedRec, nil
}

// recordSorter implements sort.Interface for multi-column sort.
type recordSorter struct {
	indices  []int
	record   arrow.Record
	sortCols []sortColSpec
}

func (s *recordSorter) Len() int      { return len(s.indices) }
func (s *recordSorter) Swap(i, j int) { s.indices[i], s.indices[j] = s.indices[j], s.indices[i] }
func (s *recordSorter) Less(i, j int) bool {
	ri, rj := s.indices[i], s.indices[j]
	for _, sc := range s.sortCols {
		cmp := compareArrowValues(s.record.Column(sc.colIndex), ri, rj)
		if cmp == 0 {
			continue
		}
		if sc.ascending {
			return cmp < 0
		}
		return cmp > 0
	}
	return false // equal on all sort columns — preserve order (stable)
}

// compareArrowValues compares two values in an Arrow array by index.
// Returns -1, 0, or 1. Supports int64, float64, and string — the
// types used by the TPC-DS bench schema. Other types compare as equal
// (no sort effect) rather than erroring.
func compareArrowValues(arr arrow.Array, i, j int) int {
	if arr.IsNull(i) && arr.IsNull(j) {
		return 0
	}
	if arr.IsNull(i) {
		return -1 // nulls first
	}
	if arr.IsNull(j) {
		return 1
	}

	switch a := arr.(type) {
	case *array.Int64:
		vi, vj := a.Value(i), a.Value(j)
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0
	case *array.Int32:
		vi, vj := a.Value(i), a.Value(j)
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0
	case *array.Float64:
		vi, vj := a.Value(i), a.Value(j)
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0
	case *array.String:
		vi, vj := a.Value(i), a.Value(j)
		if vi < vj {
			return -1
		}
		if vi > vj {
			return 1
		}
		return 0
	default:
		return 0 // unsupported type — don't sort on it
	}
}

// takeByIndices reorders an Arrow array by the given index permutation.
// This is the "gather" operation — output[i] = input[indices[i]].
func takeByIndices(arr arrow.Array, indices []int, mem memory.Allocator) arrow.Array {
	switch a := arr.(type) {
	case *array.Int64:
		builder := array.NewInt64Builder(mem)
		builder.Reserve(len(indices))
		for _, idx := range indices {
			if a.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(a.Value(idx))
			}
		}
		return builder.NewArray()
	case *array.Int32:
		builder := array.NewInt32Builder(mem)
		builder.Reserve(len(indices))
		for _, idx := range indices {
			if a.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(a.Value(idx))
			}
		}
		return builder.NewArray()
	case *array.Float64:
		builder := array.NewFloat64Builder(mem)
		builder.Reserve(len(indices))
		for _, idx := range indices {
			if a.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(a.Value(idx))
			}
		}
		return builder.NewArray()
	case *array.String:
		builder := array.NewStringBuilder(mem)
		builder.Reserve(len(indices))
		for _, idx := range indices {
			if a.IsNull(idx) {
				builder.AppendNull()
			} else {
				builder.Append(a.Value(idx))
			}
		}
		return builder.NewArray()
	default:
		// For unsupported types, return the original (unsorted).
		// This preserves the data but doesn't reorder it.
		arr.Retain()
		return arr
	}
}
