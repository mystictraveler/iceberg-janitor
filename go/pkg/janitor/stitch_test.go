package janitor

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/parquet-go/parquet-go"
)

// memIO is an in-memory implementation of icebergio.IO sufficient for
// stitching tests. It does not need Range/Stat semantics beyond what
// stitchParquetFiles uses (Open + ReadAll). The fake exists so tests
// can stay in pkg/janitor without depending on a fileblob bucket or
// the bench-tagged seed. memIO returns *memFile (pointer) from Open
// because the icebergio.File interface bundles fs.File + ReadSeekCloser
// + io.ReaderAt and we need pointer receivers for the bytes.Reader
// state to be shared across calls within one Open.
type memIO struct {
	files map[string][]byte
}

func (m *memIO) Open(name string) (icebergio.File, error) {
	body, ok := m.files[name]
	if !ok {
		return nil, fmt.Errorf("memIO: file %q not found", name)
	}
	return &memFile{name: name, body: body, r: bytes.NewReader(body)}, nil
}

func (m *memIO) Remove(name string) error {
	delete(m.files, name)
	return nil
}

// memFile satisfies icebergio.File: fs.File + io.ReadSeekCloser + io.ReaderAt.
type memFile struct {
	name string
	body []byte
	r    *bytes.Reader
}

func (f *memFile) Read(p []byte) (int, error)                  { return f.r.Read(p) }
func (f *memFile) Seek(offset int64, whence int) (int64, error) { return f.r.Seek(offset, whence) }
func (f *memFile) Close() error                                 { return nil }
func (f *memFile) ReadAt(p []byte, off int64) (int, error)      { return f.r.ReadAt(p, off) }
func (f *memFile) Stat() (fs.FileInfo, error)                   { return memFileInfo{name: f.name, size: int64(len(f.body))}, nil }

type memFileInfo struct {
	name string
	size int64
}

func (i memFileInfo) Name() string       { return i.name }
func (i memFileInfo) Size() int64        { return i.size }
func (i memFileInfo) Mode() fs.FileMode  { return 0644 }
func (i memFileInfo) ModTime() time.Time { return time.Time{} }
func (i memFileInfo) IsDir() bool        { return false }
func (i memFileInfo) Sys() any           { return nil }

// stitchTestRow is the synthetic row type for the round-trip test.
// All columns are nullable so the test exercises the I4-equivalent
// (null-count preservation) check that v1 stitching was failing.
type stitchTestRow struct {
	UserID    *string `parquet:"user_id"`
	EventType *string `parquet:"event_type"`
	Score     *int64  `parquet:"score"`
	Flag      *bool   `parquet:"flag"`
}

// makeRows builds n synthetic rows. Every 5th row has a null user_id
// and every 7th row has a null score, so the resulting parquet file
// has nontrivial per-column null counts that the master check can
// compare.
func makeRows(n int) []stitchTestRow {
	rows := make([]stitchTestRow, n)
	for i := 0; i < n; i++ {
		var uid *string
		if i%5 != 0 {
			s := fmt.Sprintf("user-%04d", i)
			uid = &s
		}
		et := fmt.Sprintf("event-%d", i%4)
		var sc *int64
		if i%7 != 0 {
			v := int64(i * 13)
			sc = &v
		}
		flag := i%2 == 0
		rows[i] = stitchTestRow{
			UserID:    uid,
			EventType: &et,
			Score:     sc,
			Flag:      &flag,
		}
	}
	return rows
}

// writeTestParquet builds an in-memory parquet file containing rows.
// Returns the encoded bytes and the count of nulls actually written
// per column (for the assertion side of the round-trip).
func writeTestParquet(t *testing.T, rows []stitchTestRow) ([]byte, map[string]int64) {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[stitchTestRow](&buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	nulls := map[string]int64{}
	for _, r := range rows {
		if r.UserID == nil {
			nulls["user_id"]++
		}
		if r.EventType == nil {
			nulls["event_type"]++
		}
		if r.Score == nil {
			nulls["score"]++
		}
		if r.Flag == nil {
			nulls["flag"]++
		}
	}
	return buf.Bytes(), nulls
}

// readParquetMeta opens an in-memory parquet file and returns
// (total rows, per-column null count summed across all column chunks).
// This is the assertion side: we use parquet-go's own reader to
// inspect the stitched output and confirm the byte copy preserved
// row counts and null counts.
func readParquetMeta(t *testing.T, body []byte) (int64, map[string]int64) {
	t.Helper()
	pf, err := parquet.OpenFile(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatalf("open stitched output: %v", err)
	}
	meta := pf.Metadata()
	nulls := map[string]int64{}
	var totalRows int64
	for _, rg := range meta.RowGroups {
		totalRows += rg.NumRows
		for _, cc := range rg.Columns {
			path := cc.MetaData.PathInSchema
			if len(path) == 0 {
				continue
			}
			name := path[len(path)-1]
			// Statistics.NullCount is a plain int64. Sum across all
			// row groups so the test result is comparable to the
			// per-column null counts we computed at write time.
			nulls[name] += cc.MetaData.Statistics.NullCount
		}
	}
	return totalRows, nulls
}

// TestStitchParquetFiles_RoundTrip writes two synthetic parquet files
// to memory, stitches them via stitchParquetFiles, and verifies that
// the stitched output has:
//   - the combined row count (sum of input row counts)
//   - the combined per-column null count (sum of input null counts)
//   - column statistics that survived the byte copy verbatim (this is
//     the I4 invariant the master check exercises in production)
//
// This is the regression guard for the v1→v2 transition: v1's
// row-by-row CopyRows path silently dropped null counts under the
// FileSchema-override workaround. v2's byte copy must preserve them.
func TestStitchParquetFiles_RoundTrip(t *testing.T) {
	rows1 := makeRows(100)
	rows2 := makeRows(150)
	body1, nulls1 := writeTestParquet(t, rows1)
	body2, nulls2 := writeTestParquet(t, rows2)

	fakeFS := &memIO{files: map[string][]byte{
		"src1.parquet": body1,
		"src2.parquet": body2,
	}}

	var out bytes.Buffer
	rowsWritten, err := stitchParquetFiles(
		context.Background(),
		fakeFS,
		[]string{"src1.parquet", "src2.parquet"},
		&out,
	)
	if err != nil {
		t.Fatalf("stitchParquetFiles: %v", err)
	}

	wantRows := int64(len(rows1) + len(rows2))
	if rowsWritten != wantRows {
		t.Errorf("rowsWritten=%d want %d", rowsWritten, wantRows)
	}

	gotRows, gotNulls := readParquetMeta(t, out.Bytes())
	if gotRows != wantRows {
		t.Errorf("output row count=%d want %d", gotRows, wantRows)
	}

	// Build the expected null counts. parquet-go's writer reports
	// null_count for EVERY column, including those with zero nulls,
	// so the comparison must include all four columns explicitly.
	wantNulls := map[string]int64{
		"user_id":    0,
		"event_type": 0,
		"score":      0,
		"flag":       0,
	}
	for k, v := range nulls1 {
		wantNulls[k] += v
	}
	for k, v := range nulls2 {
		wantNulls[k] += v
	}
	for col, want := range wantNulls {
		if got := gotNulls[col]; got != want {
			t.Errorf("output null count for %q: got %d want %d", col, got, want)
		}
	}
	for col := range gotNulls {
		if _, ok := wantNulls[col]; !ok {
			t.Errorf("output reported null count for unexpected column %q", col)
		}
	}
}

// TestStitchParquetFiles_StripsFieldIDs verifies that the stitched
// output has FieldID=0 on every SchemaElement. This is the contract
// with iceberg-go's filesToDataFiles (#861): files with field_ids are
// rejected at add-files time, so the byte-copy stitcher must strip
// them.
func TestStitchParquetFiles_StripsFieldIDs(t *testing.T) {
	rows := makeRows(50)
	body, _ := writeTestParquet(t, rows)

	fakeFS := &memIO{files: map[string][]byte{"src.parquet": body}}

	var out bytes.Buffer
	if _, err := stitchParquetFiles(
		context.Background(),
		fakeFS,
		[]string{"src.parquet"},
		&out,
	); err != nil {
		t.Fatalf("stitchParquetFiles: %v", err)
	}

	pf, err := parquet.OpenFile(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatalf("open stitched output: %v", err)
	}
	for i, se := range pf.Metadata().Schema {
		if se.FieldID != 0 {
			t.Errorf("schema element %d (%q) has FieldID=%d, want 0", i, se.Name, se.FieldID)
		}
	}
}

// TestStitchParquetFiles_EmptyInput verifies the no-op path.
func TestStitchParquetFiles_EmptyInput(t *testing.T) {
	fakeFS := &memIO{files: map[string][]byte{}}
	var out bytes.Buffer
	rows, err := stitchParquetFiles(context.Background(), fakeFS, nil, &out)
	if err != nil {
		t.Fatalf("empty input: %v", err)
	}
	if rows != 0 {
		t.Errorf("rowsWritten=%d want 0", rows)
	}
	if out.Len() != 0 {
		t.Errorf("output buffer should be empty for nil src list, got %d bytes", out.Len())
	}
}
