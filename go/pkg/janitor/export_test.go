package janitor

import (
	"context"
	"io"

	icebergio "github.com/apache/iceberg-go/io"
)

// StitchParquetFilesForTest exports stitchParquetFiles for use in
// _test packages. Not available outside of test builds.
func StitchParquetFilesForTest(ctx context.Context, fs icebergio.IO, srcPaths []string, output io.Writer) (int64, error) {
	return stitchParquetFiles(ctx, fs, srcPaths, output)
}
