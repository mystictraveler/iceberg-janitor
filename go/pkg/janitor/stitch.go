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
	// io.ReaderAt for the byte copy in step 4.
	type srcEntry struct {
		path     string
		readerAt io.ReaderAt
		size     int64
		file     *parquet.File
	}
	sources := make([]srcEntry, 0, len(srcPaths))
	for _, p := range srcPaths {
		ra, sz, oerr := readAllAt(ctx, fs, p)
		if oerr != nil {
			return 0, fmt.Errorf("opening source %s: %w", p, oerr)
		}
		pf, perr := parquet.OpenFile(ra, sz)
		if perr != nil {
			return 0, fmt.Errorf("parsing source %s: %w", p, perr)
		}
		sources = append(sources, srcEntry{path: p, readerAt: ra, size: sz, file: pf})
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
					// We don't copy index pages in v2, so this would be
					// dangling. Zero it out.
					outMD.IndexPageOffset = 0
				}
				// Drop bloom filter offset/length too (we don't copy bloom filter bytes).
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
