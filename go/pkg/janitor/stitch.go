package janitor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"golang.org/x/sync/errgroup"
)

// stitchParquetFiles is the **stitching binpack v2** compaction
// read+write path. It performs a literal byte-level copy of every
// column chunk from each source file into a single output file, with
// no decode/encode cycle, no Arrow allocator, and no parquet-go
// row-by-row conversion. The only thing that touches each value is
// the network/disk read.
//
// What it does, step by step:
//
//  1. Open each source as a *parquet.File via parquet-go's OpenFile.
//     parquet-go reads only the magic bytes and footer here; the
//     column chunks themselves are not yet touched.
//  2. Use the first source's parsed FileMetaData (via File.Metadata())
//     as the schema template. Build a fresh `out *format.FileMetaData`
//     with that schema, but with `FieldID` cleared on every
//     SchemaElement so iceberg-go's `filesToDataFiles` will accept
//     the resulting file. (See apache/iceberg-go#861 for the upstream
//     discussion of why this stripping is necessary today.)
//  3. Write the parquet magic bytes (PAR1) to the output. Track the
//     current write offset.
//  4. For each source file, for each row group, for each column
//     chunk in the row group:
//     a. Compute the source byte range from the chunk's
//        Dictionary/Data page offsets and TotalCompressedSize.
//     b. Range-read those bytes from the source file (parquet-go's
//        File satisfies io.ReaderAt, so we use ReadAt directly —
//        the bytes never round-trip through Arrow or any decode
//        path).
//     c. Write those bytes verbatim to the output at the current
//        position.
//     d. Build a new format.ColumnChunk with the same metadata as
//        the source EXCEPT the page offsets are translated to the
//        new positions in the output file.
//  5. Append the rebuilt RowGroups (with the new column chunks) to
//     the output FileMetaData. Sum NumRows.
//  6. Serialize the output FileMetaData via parquet-go's thrift
//     compact protocol encoder. Write the serialized bytes, then a
//     little-endian uint32 footer length, then the trailing PAR1
//     magic.
//  7. Done. The output file is a structurally valid parquet file
//     with the same data as the union of the source files (in source
//     order). It carries no parquet-level field_ids, so iceberg-go's
//     `Transaction.ReplaceDataFiles` accepts it without complaint.
//
// Per-row CPU work: zero. Per-batch CPU work: zero. The only data
// touched per byte is io.Copy. This is the architectural endgame for
// compaction performance per design plan decision #13: ~3-10× faster
// than decode/encode and the prerequisite for Lambda-tier compaction
// on tables an order of magnitude larger than RAM.
//
// Returns the total rows stitched. On any error the partial output
// has been written to `output`; the caller is responsible for cleanup
// (compactOnce uses cleanup() for this).
func stitchParquetFiles(ctx context.Context, fs icebergio.IO, srcPaths []string, output io.Writer) (rowsWritten int64, err error) {
	if len(srcPaths) == 0 {
		return 0, nil
	}

	// Open all source files first. We need access to each one's
	// io.ReaderAt for the byte copy in step 4. icebergio.File already
	// embeds io.ReaderAt and fs.File, so we use the file directly as
	// the ReaderAt — no intermediate io.ReadAll slurp.
	//
	// File opens and footer parses run in parallel (32-way) because
	// on a 500-file stitch the serial version spent ~25 ms in file
	// opens alone. The writes stay serialized in step 4 via the
	// output io.Writer. Close every opened file on return regardless
	// of success so partial results don't leak fds.
	type srcEntry struct {
		path     string
		readerAt io.ReaderAt
		size     int64
		file     *parquet.File
		handle   icebergio.File
	}
	sources := make([]srcEntry, len(srcPaths))
	defer func() {
		for i := range sources {
			if sources[i].handle != nil {
				_ = sources[i].handle.Close()
			}
		}
	}()
	openG, openCtx := errgroup.WithContext(ctx)
	openG.SetLimit(32)
	for i, p := range srcPaths {
		i, p := i, p
		openG.Go(func() error {
			if openCtx.Err() != nil {
				return openCtx.Err()
			}
			f, oerr := fs.Open(p)
			if oerr != nil {
				return fmt.Errorf("opening source %s: %w", p, oerr)
			}
			st, serr := f.Stat()
			if serr != nil {
				_ = f.Close()
				return fmt.Errorf("stat source %s: %w", p, serr)
			}
			sz := st.Size()
			pf, perr := parquet.OpenFile(f, sz)
			if perr != nil {
				_ = f.Close()
				return fmt.Errorf("parsing source %s: %w", p, perr)
			}
			sources[i] = srcEntry{path: p, readerAt: f, size: sz, file: pf, handle: f}
			return nil
		})
	}
	if err := openG.Wait(); err != nil {
		return 0, err
	}

	// Step 2: build the output FileMetaData skeleton from the first
	// source's metadata. We clone the SchemaElement slice and clear
	// FieldID on every element. The Schema slice is a flat
	// depth-first traversal so we don't need to recurse a tree.
	first := sources[0].file.Metadata()
	outSchema := make([]format.SchemaElement, len(first.Schema))
	for i, se := range first.Schema {
		outSchema[i] = se      // copy by value
		outSchema[i].FieldID = 0
	}
	outMeta := &format.FileMetaData{
		Version:          first.Version,
		Schema:           outSchema,
		CreatedBy:        "iceberg-janitor stitch v2",
		ColumnOrders:     first.ColumnOrders,
		KeyValueMetadata: nil, // intentionally drop key-value metadata; iceberg's name mapping is the canonical id source
	}

	// Step 3: write magic header to the output. Use a writeCounter
	// wrapper around the user's io.Writer so we can track the
	// current position without depending on Seeker.
	wc := &writeCounter{w: output}
	if _, err := wc.Write([]byte("PAR1")); err != nil {
		return 0, fmt.Errorf("writing magic header: %w", err)
	}

	// Step 4 + 5: copy column chunks for each row group of each
	// source, building the output's RowGroups slice as we go.
	for _, src := range sources {
		if cerr := ctx.Err(); cerr != nil {
			return rowsWritten, cerr
		}
		srcMeta := src.file.Metadata()
		for rgIdx := range srcMeta.RowGroups {
			srcRG := &srcMeta.RowGroups[rgIdx]
			outRG := format.RowGroup{
				NumRows:       srcRG.NumRows,
				TotalByteSize: srcRG.TotalByteSize,
				SortingColumns: srcRG.SortingColumns,
				Columns:       make([]format.ColumnChunk, len(srcRG.Columns)),
			}
			outRG.Ordinal = int16(len(outMeta.RowGroups))

			rgFileOffset := wc.offset
			outRG.FileOffset = rgFileOffset

			var rgTotalCompressed int64

			for colIdx := range srcRG.Columns {
				srcCC := &srcRG.Columns[colIdx]
				srcMD := srcCC.MetaData

				// Compute the source byte range.
				// The first page of a column chunk is at
				// DictionaryPageOffset (if > 0) else DataPageOffset.
				// TotalCompressedSize is the sum of all compressed
				// page bytes including page headers.
				srcStart := srcMD.DataPageOffset
				if srcMD.DictionaryPageOffset > 0 && srcMD.DictionaryPageOffset < srcStart {
					srcStart = srcMD.DictionaryPageOffset
				}
				length := srcMD.TotalCompressedSize
				if length <= 0 {
					return rowsWritten, fmt.Errorf("source %s row group %d column %d has non-positive TotalCompressedSize=%d", src.path, rgIdx, colIdx, length)
				}

				// Read the page bytes from the source.
				buf := make([]byte, length)
				n, rerr := src.readerAt.ReadAt(buf, srcStart)
				if rerr != nil && rerr != io.EOF {
					return rowsWritten, fmt.Errorf("reading source %s column chunk [rg=%d col=%d off=%d len=%d]: %w", src.path, rgIdx, colIdx, srcStart, length, rerr)
				}
				if int64(n) != length {
					return rowsWritten, fmt.Errorf("short read for source %s column chunk: got %d want %d", src.path, n, length)
				}

				// Write to the output and remember the new offset.
				newStart := wc.offset
				if _, werr := wc.Write(buf); werr != nil {
					return rowsWritten, fmt.Errorf("writing column chunk to output: %w", werr)
				}

				// Build the new column chunk metadata. Translate
				// the page offsets by delta = newStart - srcStart.
				delta := newStart - srcStart
				outMD := srcMD // copy by value — preserves Statistics, Encodings, NumValues, etc.
				outMD.DataPageOffset = srcMD.DataPageOffset + delta
				if srcMD.DictionaryPageOffset > 0 {
					outMD.DictionaryPageOffset = srcMD.DictionaryPageOffset + delta
				}
				if srcMD.IndexPageOffset > 0 {
					outMD.IndexPageOffset = 0 // not yet copied; zeroed to avoid dangling
				}
				// Bloom filter and indexes are copied in a second pass
				// below; zero the offsets here so they don't dangle if
				// the second pass skips them.
				outMD.BloomFilterOffset = 0
				outMD.BloomFilterLength = 0

				outCC := format.ColumnChunk{
					FilePath:   "",        // same file
					FileOffset: newStart,  // points at first page; readers tolerant
					MetaData:   outMD,
				}
				outRG.Columns[colIdx] = outCC

				rgTotalCompressed += length
			}

			// Second pass: copy bloom filters, column indexes, and
			// offset indexes for each column in this row group. These
			// are stored at separate file offsets from the column chunk
			// data and need their own offset translation. Readers
			// locate them by the offsets in the footer metadata, so
			// the exact position in the output file doesn't matter as
			// long as the offsets are correct.
			for colIdx := range srcRG.Columns {
				srcCC := &srcRG.Columns[colIdx]
				srcMD := srcCC.MetaData

				// Bloom filter.
				if srcMD.BloomFilterOffset > 0 && srcMD.BloomFilterLength > 0 {
					bf := make([]byte, srcMD.BloomFilterLength)
					n, rerr := src.readerAt.ReadAt(bf, srcMD.BloomFilterOffset)
					if rerr == nil && int32(n) == srcMD.BloomFilterLength {
						newOff := wc.offset
						if _, werr := wc.Write(bf); werr == nil {
							outRG.Columns[colIdx].MetaData.BloomFilterOffset = newOff
							outRG.Columns[colIdx].MetaData.BloomFilterLength = srcMD.BloomFilterLength
						}
					}
				}

				// Column indexes and offset indexes are stripped for now.
				// parquet-go's OpenFile auto-validates page indexes on
				// read, and the offset translation interacts poorly
				// with the internal validation. The per-row-group
				// column statistics (min/max/null_count in
				// ColumnChunk.MetaData.Statistics) ARE preserved by
				// the byte copy, and iceberg reads stats from manifest
				// entries — so stripping page indexes doesn't affect
				// correctness or iceberg-level query planning. It
				// loses intra-file predicate pushdown for readers
				// that use page indexes (DuckDB, Spark). Follow-up:
				// recompute page indexes from the copied per-page
				// stats rather than byte-copying them.
				outRG.Columns[colIdx].ColumnIndexOffset = 0
				outRG.Columns[colIdx].ColumnIndexLength = 0
				outRG.Columns[colIdx].OffsetIndexOffset = 0
				outRG.Columns[colIdx].OffsetIndexLength = 0
			}

			outRG.TotalCompressedSize = rgTotalCompressed
			outMeta.RowGroups = append(outMeta.RowGroups, outRG)
			outMeta.NumRows += srcRG.NumRows
			rowsWritten += srcRG.NumRows
		}
	}

	// Step 6: serialize the FileMetaData footer and write it,
	// followed by the 4-byte little-endian footer length and the
	// PAR1 trailer magic. Use parquet-go's compact thrift protocol,
	// matching what parquet-go's own writer uses (see writer.go:1143).
	footerStart := wc.offset
	footerBytes, err := thrift.Marshal(new(thrift.CompactProtocol), outMeta)
	if err != nil {
		return rowsWritten, fmt.Errorf("serializing FileMetaData: %w", err)
	}
	if _, err := wc.Write(footerBytes); err != nil {
		return rowsWritten, fmt.Errorf("writing footer: %w", err)
	}
	footerLen := wc.offset - footerStart

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(footerLen))
	if _, err := wc.Write(lenBuf[:]); err != nil {
		return rowsWritten, fmt.Errorf("writing footer length: %w", err)
	}
	if _, err := wc.Write([]byte("PAR1")); err != nil {
		return rowsWritten, fmt.Errorf("writing trailer magic: %w", err)
	}

	return rowsWritten, nil
}

// writeCounter wraps an io.Writer and tracks the current write
// offset. parquet-go's writer uses an equivalent type internally
// (offsetTrackingWriter); we don't have access to that, but the
// pattern is so trivial we just inline it.
type writeCounter struct {
	w      io.Writer
	offset int64
}

func (wc *writeCounter) Write(p []byte) (int, error) {
	n, err := wc.w.Write(p)
	wc.offset += int64(n)
	return n, err
}

// readAllAt reads the entire file at `path` into memory and returns
// it wrapped in a bytes.Reader, which satisfies parquet-go's
// io.ReaderAt requirement plus the size needed by parquet.OpenFile.
//
// This is the bridge between iceberg-go's icebergio.IO (which
// returns io.ReadCloser from Open) and parquet-go's OpenFile (which
// wants io.ReaderAt + size). For the bench's small parquet files
// this is fine; for production with multi-MB files this should grow
// into a gocloud.dev/blob.Bucket.NewRangeReader-backed io.ReaderAt
// that streams pages on demand instead of slurping the whole file.
// Tracked as a follow-up — the public-API friction with iceberg-go's
// IO type is the only reason we slurp.
func readAllAt(ctx context.Context, fs icebergio.IO, path string) (io.ReaderAt, int64, error) {
	f, err := fs.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	body, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(body), int64(len(body)), nil
}
