package janitor

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
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
// group.
//
// 4 is the threshold because:
// - 1 row group per file is the Athena/Trino-optimal layout
// - 2-4 row groups have negligible scheduling overhead
// - 50+ row groups (the typical stitch output) is the pathology
//   Run 19 proved: 2500 scan units vs 200 → 46-111% slower
const maxRowGroupsPerFile = 4

// maybeMergeRowGroups checks if the file at `stitchedPath` has more
// than maxRowGroupsPerFile row groups. If so, it re-reads the file
// via pqarrow and rewrites it with all rows in a single row group.
//
// Returns ("", 0, nil) if no merge was needed (row group count <= threshold).
// Returns (newPath, rows, nil) on successful merge.
// Returns ("", 0, err) on failure (caller should keep the original).
func maybeMergeRowGroups(
	ctx context.Context,
	fs icebergio.IO,
	wfs icebergio.WriteFileIO,
	stitchedPath string,
	expectedRows int64,
	writeSchema *arrow.Schema,
	locProv icebergtable.LocationProvider,
) (string, int64, error) {
	// Open the stitched file and count row groups via parquet-go
	// (same reader the stitch path uses — guaranteed to work with
	// iceberg-go's File which implements io.ReaderAt).
	f, err := fs.Open(stitchedPath)
	if err != nil {
		return "", 0, fmt.Errorf("opening stitched file for RG check: %w", err)
	}
	fStat, serr := f.Stat()
	if serr != nil {
		f.Close()
		return "", 0, fmt.Errorf("stat stitched file: %w", serr)
	}
	fSize := fStat.Size()

	pqFile, perr := pqgolib.OpenFile(f, fSize)
	if perr != nil {
		f.Close()
		return "", 0, fmt.Errorf("parquet-go open for RG check: %w", perr)
	}
	numRowGroups := len(pqFile.Metadata().RowGroups)
	f.Close()

	if numRowGroups <= maxRowGroupsPerFile {
		return "", 0, nil
	}

	// Re-read via pqarrow for the decode/encode merge. We need a
	// parquet.ReaderAtSeeker for arrow's file reader. iceberg-go's
	// File satisfies io.ReaderAt + io.ReadSeeker so we adapt it.
	f2, err := fs.Open(stitchedPath)
	if err != nil {
		return "", 0, fmt.Errorf("reopening for merge: %w", err)
	}
	defer f2.Close()

	// Read all source data as Arrow batches via the pqarrow helper
	// that the fallback path already uses. This handles the
	// ReaderAt → arrow FileReader conversion correctly.
	var batches []arrow.Record
	mem := memory.DefaultAllocator
	b, n, rerr := readParquetFileBatches(ctx, fs, stitchedPath, writeSchema, mem)
	if rerr != nil {
		return "", 0, fmt.Errorf("reading stitched file for merge: %w", rerr)
	}
	batches = b
	_ = n

	// Write all batches into a new file with a single row group.
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
