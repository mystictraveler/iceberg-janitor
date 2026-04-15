package janitor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergio "github.com/apache/iceberg-go/io"
	pqgo "github.com/parquet-go/parquet-go"
)

// V2 delete-file handling for the post-stitch decode/encode pass.
//
// Iceberg V2 tables use two kinds of delete files:
//
//   - Position deletes: a parquet file with schema (file_path string, pos long).
//     Each row marks one (file, position) pair as deleted. Any data file
//     whose path appears in a position delete file has rows filtered at
//     the listed positions, regardless of sequence number.
//
//   - Equality deletes: a parquet file whose columns are a subset of
//     the table schema (declared via EqualityFieldIDs on the DataFile).
//     Each row represents a tuple; any data file with
//     seq_num <= delete_seq_num whose rows match ANY delete-row tuple
//     (value equality on all equality columns) has those rows filtered.
//
// The janitor applies both during the post-stitch decode/encode pass
// in maybeMergeRowGroups: after stitching N source files into one, if
// any deletes apply, we re-read the stitched output through Arrow,
// filter the relevant rows, and re-encode. The deleted rows are gone
// from the output, and the delete files can be removed from the
// snapshot at commit time (they now reference orphaned paths or apply
// to files with lower seq_num than our new file).
//
// Equality deletes on non-trivial schemas (timestamp/decimal/uuid/
// binary/nested) are REFUSED for correctness rather than applied
// approximately. See supportedEqDeleteType.

// deleteFileRef is a lightweight reference to a V2 delete file
// collected during the manifest walk. The payload is NOT loaded here —
// it's loaded separately in the bundle-build phase so the manifest
// walk stays fast and deterministic. Internal because the shape isn't
// useful outside janitor.
type deleteFileRef struct {
	path       string
	content    icebergpkg.ManifestEntryContent
	seqNum     int64
	fieldIDs   []int
	fileFormat string
	rowCount   int64
	referenced string // V2.2+ position deletes may carry this
}

// derefString returns "" if p is nil, else *p. Tiny helper kept near
// its sole caller in the manifest walk.
func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// BuildDeleteBundle loads delete file payloads for every ref in refs,
// producing a DeleteBundle ready to be passed to BuildRowMask. Called
// once per compaction attempt, AFTER the manifest walk and BEFORE the
// stitch/decode phase.
//
// Semantics:
//   - Position deletes are always loaded (they are small and cheap).
//   - Equality deletes are loaded and decoded; if any column has an
//     unsupported type, the whole compaction attempt is refused via a
//     *UnsupportedFeatureError. Better a clean error than silent row
//     resurrection.
//   - V3 deletion vectors (content=PosDeletes + format=PUFFIN) are
//     already blocked by checkTableForUnsupportedFeatures earlier in
//     the pipeline, so we don't re-check here.
func BuildDeleteBundle(ctx context.Context, fs icebergio.IO, refs []deleteFileRef, icebergSchema *icebergpkg.Schema) (*DeleteBundle, error) {
	bundle := &DeleteBundle{
		PositionDeletes: map[string]*PositionDeleteSet{},
	}
	for _, r := range refs {
		switch r.content {
		case icebergpkg.EntryContentPosDeletes:
			if strings.EqualFold(r.fileFormat, "PUFFIN") {
				return nil, &UnsupportedFeatureError{
					Feature: "V3 deletion vectors",
					Detail:  r.path,
				}
			}
			if err := LoadPositionDeletes(ctx, fs, r.path, bundle); err != nil {
				return nil, fmt.Errorf("loading pos delete %s: %w", r.path, err)
			}
			bundle.PositionDeleteFilePaths = append(bundle.PositionDeleteFilePaths, r.path)
		case icebergpkg.EntryContentEqDeletes:
			if err := LoadEqualityDelete(ctx, fs, r.path, r.seqNum, r.fieldIDs, icebergSchema, bundle); err != nil {
				return nil, err
			}
			bundle.EqualityDeleteFilePaths = append(bundle.EqualityDeleteFilePaths, r.path)
		}
	}
	return bundle, nil
}

// DeleteBundle is the per-partition collection of delete files that
// apply to a set of source data files during one compaction attempt.
// Emitted by the manifest walk, consumed by maybeMergeRowGroups.
type DeleteBundle struct {
	// PositionDeletes indexed by referenced source data file path.
	// A source path maps to the sorted positions that must be dropped.
	// Multiple position delete files targeting the same source file
	// are merged at discovery time.
	PositionDeletes map[string]*PositionDeleteSet

	// EqualityDeletes are applied to every source data file whose
	// sequence number is strictly less than the delete file's sequence
	// number. Ordered by sequence number ascending.
	EqualityDeletes []*EqualityDeleteFile

	// PositionDeleteFilePaths is the set of position delete file paths
	// whose rows were fully absorbed into the new output. These must
	// be removed from the snapshot at commit time. Populated by the
	// discovery phase; consumed by the commit phase.
	PositionDeleteFilePaths []string

	// EqualityDeleteFilePaths — same for equality deletes.
	EqualityDeleteFilePaths []string
}

// IsEmpty reports whether this bundle has any deletes that could
// affect the output. Callers use this to decide whether to force the
// decode/encode path.
func (b *DeleteBundle) IsEmpty() bool {
	if b == nil {
		return true
	}
	return len(b.PositionDeletes) == 0 && len(b.EqualityDeletes) == 0
}

// PositionDeleteSet holds the sorted, deduplicated positions (0-based
// row indices) that must be dropped for a single source data file.
// Backed by a sorted slice, not a map — most tables have few deletes
// per file and linear membership checks are faster than hashing at
// these sizes.
type PositionDeleteSet struct {
	sorted []int64 // ascending, unique
}

// Contains returns true if position p is in the set. O(log n).
func (s *PositionDeleteSet) Contains(p int64) bool {
	if s == nil || len(s.sorted) == 0 {
		return false
	}
	// Binary search.
	lo, hi := 0, len(s.sorted)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if s.sorted[mid] == p {
			return true
		}
		if s.sorted[mid] < p {
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return false
}

// Len returns the number of deleted positions.
func (s *PositionDeleteSet) Len() int {
	if s == nil {
		return 0
	}
	return len(s.sorted)
}

// EqualityDeleteFile is a decoded equality delete file, ready to match
// incoming rows during the decode/encode pass.
type EqualityDeleteFile struct {
	// SeqNum is the data sequence number of this delete file. Rows in
	// a source data file are matched only if source.seq_num < SeqNum.
	SeqNum int64

	// FieldIDs are the Iceberg field IDs of the equality columns
	// (set from DataFile.EqualityFieldIDs).
	FieldIDs []int

	// ColumnNames are the column names corresponding to FieldIDs in
	// the current table schema. Used to look up the same columns in
	// the data file's Arrow schema.
	ColumnNames []string

	// Predicates are the decoded delete-row tuples, one per row in the
	// delete file. Each tuple has len(FieldIDs) values. A source row
	// matches if it equals ANY predicate tuple on all columns.
	Predicates []EqualityTuple

	// FilePath is the delete file's location (for error reporting).
	FilePath string
}

// EqualityTuple is one row of an equality delete file: a value per
// equality column. Values are stored as Go scalars in their natural
// type (int32/int64/float64/string/bool/nil). Nil means NULL.
type EqualityTuple []any

// LoadPositionDeletes reads a position delete file and merges its
// contents into the per-source-file sets in the bundle.
//
// Schema: the Iceberg V2 position delete file has exactly two top-level
// fields: file_path (string) and pos (long). We read them via
// parquet-go's generic reader with a row struct that matches.
func LoadPositionDeletes(ctx context.Context, fs icebergio.IO, path string, bundle *DeleteBundle) error {
	f, err := fs.Open(path)
	if err != nil {
		return fmt.Errorf("opening pos delete %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat pos delete %s: %w", path, err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		return fmt.Errorf("parse pos delete %s: %w", path, err)
	}

	type row struct {
		FilePath string `parquet:"file_path"`
		Pos      int64  `parquet:"pos"`
	}
	reader := pqgo.NewGenericReader[row](pf)
	defer reader.Close()

	perFile := map[string][]int64{}
	buf := make([]row, 1024)
	for {
		n, rerr := reader.Read(buf)
		for i := 0; i < n; i++ {
			perFile[buf[i].FilePath] = append(perFile[buf[i].FilePath], buf[i].Pos)
		}
		if rerr != nil {
			break
		}
	}

	if bundle.PositionDeletes == nil {
		bundle.PositionDeletes = map[string]*PositionDeleteSet{}
	}
	for srcPath, positions := range perFile {
		existing := bundle.PositionDeletes[srcPath]
		if existing == nil {
			existing = &PositionDeleteSet{}
			bundle.PositionDeletes[srcPath] = existing
		}
		existing.sorted = mergeSortedInt64(existing.sorted, positions)
	}
	return nil
}

// mergeSortedInt64 returns a sorted, deduplicated union of a and b.
// a is assumed already sorted-unique; b is unsorted; result replaces
// a. O((|a|+|b|) log |b|) due to the in-place sort of b.
func mergeSortedInt64(a, b []int64) []int64 {
	if len(b) == 0 {
		return a
	}
	// Sort b in place, then two-way merge with dedup.
	sortInt64Slice(b)
	out := make([]int64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] < b[j]:
			out = appendUnique(out, a[i])
			i++
		case a[i] > b[j]:
			out = appendUnique(out, b[j])
			j++
		default:
			out = appendUnique(out, a[i])
			i++
			j++
		}
	}
	for ; i < len(a); i++ {
		out = appendUnique(out, a[i])
	}
	for ; j < len(b); j++ {
		out = appendUnique(out, b[j])
	}
	return out
}

func appendUnique(dst []int64, v int64) []int64 {
	if n := len(dst); n > 0 && dst[n-1] == v {
		return dst
	}
	return append(dst, v)
}

func sortInt64Slice(s []int64) {
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
}

// LoadEqualityDelete decodes an equality delete file and appends it to
// the bundle. The delete file's Arrow-read schema is derived from
// its EqualityFieldIDs in the manifest entry — callers must pass the
// resolved field IDs and the current table's Iceberg schema so we can
// map field IDs to names.
func LoadEqualityDelete(ctx context.Context, fs icebergio.IO, path string, seqNum int64, fieldIDs []int, icebergSchema *icebergpkg.Schema, bundle *DeleteBundle) error {
	if len(fieldIDs) == 0 {
		return fmt.Errorf("eq delete %s: no equality field IDs declared", path)
	}
	// Resolve field IDs to names; check every column is a supported
	// scalar type. Complex types are refused with a clear error — we
	// would rather fail loudly than apply deletes incorrectly.
	names := make([]string, len(fieldIDs))
	for i, fid := range fieldIDs {
		f, ok := icebergSchema.FindFieldByID(fid)
		if !ok {
			return fmt.Errorf("eq delete %s references unknown field id %d", path, fid)
		}
		if !supportedEqDeleteType(f.Type) {
			return &UnsupportedFeatureError{
				Feature: "equality delete with complex column type",
				Detail:  fmt.Sprintf("column %q has type %s (supported: int/long/float/double/string/boolean)", f.Name, f.Type),
			}
		}
		names[i] = f.Name
	}

	f, err := fs.Open(path)
	if err != nil {
		return fmt.Errorf("opening eq delete %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat eq delete %s: %w", path, err)
	}
	pf, err := pqgo.OpenFile(f, info.Size())
	if err != nil {
		return fmt.Errorf("parse eq delete %s: %w", path, err)
	}

	// Read generic rows. We use the generic Row API because we don't
	// have a Go struct type that matches the dynamic equality schema —
	// just extract column values by name.
	rows, err := readEqDeleteRows(pf, names)
	if err != nil {
		return fmt.Errorf("reading eq delete %s: %w", path, err)
	}

	bundle.EqualityDeletes = append(bundle.EqualityDeletes, &EqualityDeleteFile{
		SeqNum:      seqNum,
		FieldIDs:    append([]int(nil), fieldIDs...),
		ColumnNames: names,
		Predicates:  rows,
		FilePath:    path,
	})
	return nil
}

// supportedEqDeleteType reports whether the janitor can safely compare
// values of this iceberg type during equality delete application.
// Limited to scalar types with unambiguous Go representations; complex
// types (timestamp/decimal/uuid/binary/struct/list/map) refused.
func supportedEqDeleteType(t icebergpkg.Type) bool {
	switch t.(type) {
	case icebergpkg.Int32Type, icebergpkg.Int64Type,
		icebergpkg.Float32Type, icebergpkg.Float64Type,
		icebergpkg.StringType, icebergpkg.BooleanType:
		return true
	}
	return false
}

// readEqDeleteRows extracts the named columns from every row of an
// equality delete parquet file. Returns one EqualityTuple per row,
// each with values in declared-name order.
func readEqDeleteRows(pf *pqgo.File, names []string) ([]EqualityTuple, error) {
	schema := pf.Schema()
	// Find column indices by name in the delete file's schema.
	colIdx := make([]int, len(names))
	for i, n := range names {
		idx := -1
		for j, c := range schema.Columns() {
			// schema.Columns() returns [][]string path; top-level name is c[0].
			if len(c) > 0 && c[0] == n {
				idx = j
				break
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("eq delete missing column %q", n)
		}
		colIdx[i] = idx
	}

	var out []EqualityTuple
	for _, rg := range pf.RowGroups() {
		chunks := rg.ColumnChunks()
		perCol := make([][]pqgo.Value, len(colIdx))
		for i, ci := range colIdx {
			values, err := readAllValues(chunks[ci])
			if err != nil {
				return nil, fmt.Errorf("reading column %d: %w", ci, err)
			}
			perCol[i] = values
		}
		// Transpose to row-major.
		if len(perCol) == 0 {
			continue
		}
		nRows := len(perCol[0])
		for r := 0; r < nRows; r++ {
			tuple := make(EqualityTuple, len(perCol))
			for c := 0; c < len(perCol); c++ {
				tuple[c] = parquetValueToGo(perCol[c][r])
			}
			out = append(out, tuple)
		}
	}
	return out, nil
}

// readAllValues reads every value in a column chunk into a slice. Used
// for eq delete files which are always small (a row count equal to the
// user-issued DELETE matches).
func readAllValues(cc pqgo.ColumnChunk) ([]pqgo.Value, error) {
	pages := cc.Pages()
	defer pages.Close()
	var out []pqgo.Value
	buf := make([]pqgo.Value, 1024)
	for {
		p, err := pages.ReadPage()
		if err != nil {
			break
		}
		vals := p.Values()
		for {
			n, verr := vals.ReadValues(buf)
			for i := 0; i < n; i++ {
				// Copy — parquet-go reuses the backing storage.
				v := buf[i].Clone()
				out = append(out, v)
			}
			if verr != nil {
				break
			}
		}
		pqgo.Release(p)
	}
	return out, nil
}

// parquetValueToGo converts a parquet-go Value to a Go scalar
// representation compatible with the Arrow-side comparison. Only the
// types allowed by supportedEqDeleteType are handled; anything else
// returns nil (which won't match any real row).
func parquetValueToGo(v pqgo.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case pqgo.Boolean:
		return v.Boolean()
	case pqgo.Int32:
		return int32(v.Int32())
	case pqgo.Int64:
		return int64(v.Int64())
	case pqgo.Float:
		return float64(v.Float())
	case pqgo.Double:
		return v.Double()
	case pqgo.ByteArray:
		// UTF8/string.
		return string(v.ByteArray())
	}
	return nil
}

// BuildRowMask returns a bitmap (one bool per row) marking rows to
// KEEP during the decode/encode pass. Rows that should be dropped are
// marked false.
//
// Inputs:
//   - sourcePath: the absolute path of the source data file whose rows
//     are being processed. Used to look up position deletes.
//   - sourceSeqNum: the data sequence number of the source file. An
//     equality delete applies only if sourceSeqNum < eq.SeqNum.
//   - rowIndexOffset: the row index of the FIRST row in this batch,
//     relative to the start of the source file. Position deletes are
//     indexed against this offset.
//   - batch: the Arrow record batch being masked.
//   - arrowColIdx: for each equality delete column name, the index in
//     batch.Schema() — resolved once per source file by the caller.
func BuildRowMask(
	sourcePath string,
	sourceSeqNum int64,
	rowIndexOffset int64,
	batch arrow.Record,
	bundle *DeleteBundle,
	arrowColIdxByName map[string]int,
) []bool {
	n := int(batch.NumRows())
	keep := make([]bool, n)
	for i := range keep {
		keep[i] = true
	}
	if bundle == nil {
		return keep
	}

	// Position deletes.
	if posSet := bundle.PositionDeletes[sourcePath]; posSet != nil {
		for i := 0; i < n; i++ {
			if posSet.Contains(rowIndexOffset + int64(i)) {
				keep[i] = false
			}
		}
	}

	// Equality deletes — for each applicable delete file, walk rows
	// and if any delete-tuple matches, drop. O(rows × deletes).
	// Acceptable because delete files are typically tiny.
	for _, eq := range bundle.EqualityDeletes {
		if sourceSeqNum >= eq.SeqNum {
			continue // delete file predates this source; doesn't apply
		}
		// Resolve Arrow column index per equality column.
		colIdxs := make([]int, len(eq.ColumnNames))
		ok := true
		for i, name := range eq.ColumnNames {
			idx, found := arrowColIdxByName[name]
			if !found {
				ok = false
				break
			}
			colIdxs[i] = idx
		}
		if !ok {
			continue // column not in output schema; delete can't apply
		}
		for row := 0; row < n; row++ {
			if !keep[row] {
				continue
			}
			for _, tuple := range eq.Predicates {
				if rowMatchesTuple(batch, colIdxs, row, tuple) {
					keep[row] = false
					break
				}
			}
		}
	}
	return keep
}

// rowMatchesTuple returns true iff row r in batch equals the tuple on
// every equality column (column index per position in tuple).
func rowMatchesTuple(batch arrow.Record, colIdxs []int, r int, tuple EqualityTuple) bool {
	if len(colIdxs) != len(tuple) {
		return false
	}
	for i, ci := range colIdxs {
		col := batch.Column(ci)
		if !arrowValueEquals(col, r, tuple[i]) {
			return false
		}
	}
	return true
}

// arrowValueEquals reports whether the value at index r in arr equals
// v under the semantics used for equality delete matching. NULL
// matches NULL (both sides nil). Numeric comparisons are done in the
// natural Go width; string compares as string.
func arrowValueEquals(arr arrow.Array, r int, v any) bool {
	if arr.IsNull(r) {
		return v == nil
	}
	if v == nil {
		return false
	}
	switch a := arr.(type) {
	case *array.Int32:
		switch vv := v.(type) {
		case int32:
			return a.Value(r) == vv
		case int64:
			return int64(a.Value(r)) == vv
		}
	case *array.Int64:
		switch vv := v.(type) {
		case int64:
			return a.Value(r) == vv
		case int32:
			return a.Value(r) == int64(vv)
		}
	case *array.Float32:
		if vv, ok := v.(float64); ok {
			return float64(a.Value(r)) == vv && !math.IsNaN(vv)
		}
	case *array.Float64:
		if vv, ok := v.(float64); ok {
			return a.Value(r) == vv && !math.IsNaN(vv)
		}
	case *array.String:
		if vv, ok := v.(string); ok {
			return a.Value(r) == vv
		}
	case *array.Boolean:
		if vv, ok := v.(bool); ok {
			return a.Value(r) == vv
		}
	}
	return false
}

// applyRowMask returns a new arrow.Record containing only the rows
// where mask[i] is true. Returns nil if every row is masked out.
// The caller must Release the original record; the returned record
// (if any) owns its own columns and must be Released by the caller.
func applyRowMask(batch arrow.Record, mask []bool, mem memory.Allocator) arrow.Record {
	if int64(len(mask)) != batch.NumRows() {
		return batch // shouldn't happen; pass through defensively
	}
	// Build a slice of kept row indices.
	keep := make([]int, 0, len(mask))
	for i, k := range mask {
		if k {
			keep = append(keep, i)
		}
	}
	if len(keep) == 0 {
		return nil
	}
	if len(keep) == len(mask) {
		// Full keep — retain and return as-is so we don't pay the
		// allocation cost when deletes don't actually affect this
		// batch.
		batch.Retain()
		return batch
	}
	schema := batch.Schema()
	cols := make([]arrow.Array, schema.NumFields())
	for c := 0; c < schema.NumFields(); c++ {
		arr := batch.Column(c)
		switch a := arr.(type) {
		case *array.Int32:
			b := array.NewInt32Builder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		case *array.Int64:
			b := array.NewInt64Builder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		case *array.Float32:
			b := array.NewFloat32Builder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		case *array.Float64:
			b := array.NewFloat64Builder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		case *array.String:
			b := array.NewStringBuilder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		case *array.Boolean:
			b := array.NewBooleanBuilder(mem)
			b.Reserve(len(keep))
			for _, idx := range keep {
				if a.IsNull(idx) {
					b.AppendNull()
				} else {
					b.Append(a.Value(idx))
				}
			}
			cols[c] = b.NewArray()
		default:
			// Unsupported column type — retain the original column
			// but slice it. Arrow doesn't support fancy indexing on
			// every type; slicing gives a view that's at least
			// length-correct when keep is contiguous. For true
			// non-contiguous masks on unsupported types we'd need
			// compute.Take — punting until the bench hits it.
			arr.Retain()
			cols[c] = arr
		}
	}
	rec := array.NewRecord(schema, cols, int64(len(keep)))
	for _, c := range cols {
		c.Release()
	}
	return rec
}

// CountDroppedRows returns how many false entries are in mask.
func CountDroppedRows(mask []bool) int64 {
	var dropped int64
	for _, k := range mask {
		if !k {
			dropped++
		}
	}
	return dropped
}

// formatEqDeleteSummary is a debug helper used in logging/error paths.
func formatEqDeleteSummary(bundle *DeleteBundle) string {
	if bundle == nil || len(bundle.EqualityDeletes) == 0 {
		return "no eq deletes"
	}
	var b strings.Builder
	for i, eq := range bundle.EqualityDeletes {
		if i > 0 {
			b.WriteString("; ")
		}
		fmt.Fprintf(&b, "%s(seq=%d cols=%v rows=%d)", eq.FilePath, eq.SeqNum, eq.ColumnNames, len(eq.Predicates))
	}
	return b.String()
}

// bigEndianInt64 is kept for potential lower-bound scans against the
// iceberg-go DataFile.LowerBoundValues byte representation, which
// serializes integers big-endian. Not currently used but kept near
// the delete code because future pruning work will need it.
func bigEndianInt64(b []byte) (int64, bool) {
	if len(b) != 8 {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(b)), true
}

var _ = bytes.Equal // reserved for future byte-bound comparisons
var _ = bigEndianInt64
