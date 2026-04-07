// Command janitor-cli is the local CLI for analyzing and maintaining
// Iceberg tables on any supported object store.
//
// MVP scope: discover tables in a warehouse and analyze a single table's
// health. No compaction yet (next iteration), no config package, no
// safety circuit breakers — the bare minimum to test the read loop
// against MinIO + DuckDB.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergtable "github.com/apache/iceberg-go/table"
	"gocloud.dev/blob"

	// Blank imports register URL openers with gocloud.dev/blob so that
	// blob.OpenBucket can dispatch on the URL scheme.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "discover":
		if err := runDiscover(args); err != nil {
			fmt.Fprintln(os.Stderr, "discover:", err)
			os.Exit(1)
		}
	case "analyze":
		if err := runAnalyze(args); err != nil {
			fmt.Fprintln(os.Stderr, "analyze:", err)
			os.Exit(1)
		}
	case "compact":
		if err := runCompact(args); err != nil {
			fmt.Fprintln(os.Stderr, "compact:", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `janitor-cli — Iceberg warehouse maintenance (v0 / proof of concept)

USAGE:
  janitor-cli <command> [args]

COMMANDS:
  discover [prefix]      List Iceberg tables under the warehouse root, or
                         under the optional prefix relative to the root.
  analyze <table_path>   Print a HealthReport for the table at the given
                         relative path. Use --json for machine-readable
                         output.
  compact <table_path>   Read all data from the table and rewrite it as
                         the minimum number of files needed. The pre-commit
                         master check (I1 row count) is mandatory and
                         non-bypassable.

ENVIRONMENT:
  JANITOR_WAREHOUSE_URL   gocloud.dev/blob URL of the warehouse bucket.
                          Examples:
                            file:///tmp/warehouse
                            s3://my-warehouse?region=us-east-1
                            s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1
                            (for MinIO; AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
                             must be set in the environment)

EXAMPLES:
  # local fileblob fixture
  JANITOR_WAREHOUSE_URL=file:///tmp/warehouse janitor-cli discover

  # MinIO
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  JANITOR_WAREHOUSE_URL='s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1' \
  janitor-cli discover
`)
}

func runDiscover(args []string) error {
	rootPrefix := ""
	if len(args) > 0 {
		rootPrefix = args[0]
	}

	url := os.Getenv("JANITOR_WAREHOUSE_URL")
	if url == "" {
		return fmt.Errorf("JANITOR_WAREHOUSE_URL is not set")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	bucket, err := blob.OpenBucket(ctx, url)
	if err != nil {
		return fmt.Errorf("opening bucket %q: %w", url, err)
	}
	defer bucket.Close()

	tables, err := catalog.DiscoverTables(ctx, bucket, rootPrefix)
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		fmt.Fprintln(os.Stderr, "no Iceberg tables found")
		return nil
	}

	maxPrefix := 0
	for _, t := range tables {
		if len(t.Prefix) > maxPrefix {
			maxPrefix = len(t.Prefix)
		}
	}
	fmt.Printf("%-*s  %-7s  %s\n", maxPrefix, "TABLE", "VERSION", "CURRENT METADATA")
	fmt.Printf("%s  %s  %s\n", strings.Repeat("-", maxPrefix), "-------", strings.Repeat("-", 16))
	for _, t := range tables {
		fmt.Printf("%-*s  v%-6d  %s\n", maxPrefix, t.Prefix, t.CurrentVersion, t.CurrentMetadata)
	}
	return nil
}

// resolveTable opens the warehouse bucket, discovers the table at the given
// relative path, and loads it via the DirectoryCatalog. Used by both
// `analyze` and `compact`. The DirectoryCatalog gives the loaded table a
// real catalog reference so subsequent transaction commits can succeed.
// Returns the loaded table, the bucket-relative table prefix, and a
// cleanup function the caller must defer.
func resolveTable(ctx context.Context, tablePath string) (*icebergtable.Table, string, func(), error) {
	tablePath = strings.Trim(tablePath, "/")
	url := os.Getenv("JANITOR_WAREHOUSE_URL")
	if url == "" {
		return nil, "", nil, fmt.Errorf("JANITOR_WAREHOUSE_URL is not set")
	}

	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", url, propsFromEnv())
	if err != nil {
		return nil, "", nil, err
	}

	// Map a janitor-style path ("mvp.db/events") to an iceberg-go
	// Identifier (["mvp", "events"]). The leading namespace component is
	// stripped of its ".db" suffix because that suffix is iceberg-go's
	// default location-provider convention, not part of the namespace name.
	parts := strings.Split(tablePath, "/")
	if len(parts) < 2 {
		cat.Close()
		return nil, "", nil, fmt.Errorf("table path %q must be at least <namespace>.db/<table>", tablePath)
	}
	ns := strings.TrimSuffix(parts[0], ".db")
	rest := parts[1:]
	ident := icebergtable.Identifier(append([]string{ns}, rest...))

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		cat.Close()
		return nil, "", nil, fmt.Errorf("loading table %v: %w", ident, err)
	}
	return tbl, tablePath, func() { cat.Close() }, nil
}

// propsFromEnv collects S3-style credentials/endpoint env vars into the
// property map iceberg-go's IO layer expects. Lets the same binary work
// against MinIO without any per-cloud config code. It also exports the
// matching AWS_* env vars because iceberg-go's table-level IO is built
// from table properties (not catalog props), and reads
// AWS_S3_ENDPOINT / AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION
// from the env as fallbacks.
func propsFromEnv() map[string]string {
	props := map[string]string{}
	if v := os.Getenv("S3_ENDPOINT"); v != "" {
		props["s3.endpoint"] = v
		os.Setenv("AWS_S3_ENDPOINT", v)
		os.Setenv("AWS_ENDPOINT_URL_S3", v)
	}
	if v := os.Getenv("S3_REGION"); v != "" {
		props["s3.region"] = v
		os.Setenv("AWS_REGION", v)
		os.Setenv("AWS_DEFAULT_REGION", v)
	} else if v := os.Getenv("AWS_REGION"); v != "" {
		props["s3.region"] = v
	}
	if v := os.Getenv("AWS_ACCESS_KEY_ID"); v != "" {
		props["s3.access-key-id"] = v
	}
	if v := os.Getenv("AWS_SECRET_ACCESS_KEY"); v != "" {
		props["s3.secret-access-key"] = v
	}
	return props
}

func runAnalyze(args []string) error {
	jsonOut := false
	var tablePath string
	for _, a := range args {
		switch {
		case a == "--json":
			jsonOut = true
		case strings.HasPrefix(a, "--"):
			return fmt.Errorf("unknown flag %q", a)
		default:
			if tablePath != "" {
				return fmt.Errorf("multiple table paths provided")
			}
			tablePath = a
		}
	}
	if tablePath == "" {
		return fmt.Errorf("usage: janitor-cli analyze <table_path> [--json]")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	tbl, prefix, cleanup, err := resolveTable(ctx, tablePath)
	if err != nil {
		return err
	}
	defer cleanup()

	report, err := analyzer.Assess(ctx, tbl, analyzer.AnalyzerOptions{})
	if err != nil {
		return fmt.Errorf("assessing table: %w", err)
	}

	if jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(report)
	}
	printReport(prefix, report)
	return nil
}

// runCompact reads all data from the table, rewrites it via iceberg-go's
// transactional Overwrite, and runs the pre-commit master check (I1 row
// count, I2 schema, I7 manifest references) before the transaction
// commits.
//
// Streaming compaction: we use Scan().ToArrowRecords() to get an
// iterator of record batches, wrap it as an array.RecordReader, and
// pass that to Transaction.Overwrite. iceberg-go consumes the reader
// streaming-style, so peak memory is bounded to one record batch at a
// time instead of the entire table. For a 10 GB table on a Lambda with
// 10 GB RAM, this is the difference between OOM and success.
//
// This is still NOT the byte-level stitching binpack from the design
// plan — the data is decoded from the source Parquet files and re-
// encoded into new ones. True stitching (column-chunk byte copy via
// parquet-go.CopyRowGroups) lands in a subsequent iteration. Streaming
// is the prerequisite step that makes the memory bound correct.
func runCompact(args []string) error {
	var tablePath string
	for _, a := range args {
		if strings.HasPrefix(a, "--") {
			return fmt.Errorf("unknown flag %q", a)
		}
		if tablePath != "" {
			return fmt.Errorf("multiple table paths provided")
		}
		tablePath = a
	}
	if tablePath == "" {
		return fmt.Errorf("usage: janitor-cli compact <table_path>")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	tbl, prefix, cleanup, err := resolveTable(ctx, tablePath)
	if err != nil {
		return err
	}
	defer cleanup()

	beforeFiles, beforeBytes, beforeRows := snapshotFileStats(ctx, tbl)
	fmt.Printf("compacting %s\n", prefix)
	fmt.Printf("  before: %d data files, %s, %d rows\n", beforeFiles, humanBytes(beforeBytes), beforeRows)

	// Streaming scan: ToArrowRecords returns the schema and an iterator
	// of record batches. We wrap the iterator as an array.RecordReader
	// so iceberg-go's Overwrite consumes one batch at a time without
	// loading the whole table.
	scan := tbl.Scan()
	scanSchema, recordIter, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return fmt.Errorf("opening scan: %w", err)
	}
	reader := newStreamingRecordReader(scanSchema, recordIter)
	defer reader.Release()

	// Open a transaction so we can stage the rewrite, run the master
	// check on the staged result, and only then commit.
	tx := tbl.NewTransaction()
	if err := tx.Overwrite(ctx, reader, nil); err != nil {
		return fmt.Errorf("staging streaming overwrite: %w", err)
	}
	if err := reader.Err(); err != nil {
		return fmt.Errorf("scan iterator error: %w", err)
	}
	staged, err := tx.StagedTable()
	if err != nil {
		return fmt.Errorf("getting staged table: %w", err)
	}

	// Mandatory pre-commit master check (I1 row count). Non-bypassable.
	verification, err := safety.VerifyCompactionConsistency(ctx, tbl, staged, propsFromEnv())
	if err != nil {
		fmt.Fprintf(os.Stderr, "MASTER CHECK FAILED — refusing to commit\n")
		fmt.Fprintf(os.Stderr, "  verification: %+v\n", verification)
		return err
	}
	fmt.Printf("  master check: PASS (I1 in=%d out=%d  I2 schema=%d  I7 refs=%d/%d)\n",
		verification.I1RowCount.In, verification.I1RowCount.Out,
		verification.I2Schema.OutID,
		verification.I7ManifestRefs.Passed, verification.I7ManifestRefs.Checked)

	// Commit.
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	afterFiles, afterBytes, afterRows := snapshotFileStats(ctx, newTbl)
	fmt.Printf("  after:  %d data files, %s, %d rows\n", afterFiles, humanBytes(afterBytes), afterRows)
	if newSnap := newTbl.CurrentSnapshot(); newSnap != nil {
		fmt.Printf("  new snapshot: %d\n", newSnap.SnapshotID)
	}
	if afterRows != beforeRows {
		// Should be impossible since the master check passed, but defense in depth.
		return fmt.Errorf("post-commit row count mismatch: before=%d after=%d", beforeRows, afterRows)
	}
	fmt.Printf("compaction complete: %d → %d files (%.1fx reduction)\n",
		beforeFiles, afterFiles, float64(beforeFiles)/maxF(float64(afterFiles), 1))
	return nil
}

// streamingRecordReader adapts an iter.Seq2[arrow.RecordBatch, error]
// (the streaming scan iterator iceberg-go returns from ToArrowRecords)
// into the array.RecordReader interface that Transaction.Overwrite
// expects. It pulls one batch at a time via iter.Pull2 — no buffering,
// no whole-table-in-memory.
//
// This is the load-bearing piece for memory-bounded compaction. With
// the previous ToArrowTable approach, peak memory was proportional to
// total table size; with this reader it's proportional to a single
// record batch (default ~64K rows, kilobytes to a few MB).
type streamingRecordReader struct {
	schema  *arrow.Schema
	next    func() (arrow.RecordBatch, error, bool)
	stop    func()
	current arrow.RecordBatch
	err     error
	refs    int64
}

func newStreamingRecordReader(schema *arrow.Schema, seq iter.Seq2[arrow.RecordBatch, error]) *streamingRecordReader {
	next, stop := iter.Pull2(seq)
	return &streamingRecordReader{
		schema: schema,
		next:   next,
		stop:   stop,
		refs:   1,
	}
}

func (r *streamingRecordReader) Schema() *arrow.Schema { return r.schema }

func (r *streamingRecordReader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	rec, err, ok := r.next()
	if !ok {
		return false
	}
	if err != nil {
		r.err = err
		return false
	}
	r.current = rec
	return true
}

func (r *streamingRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *streamingRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *streamingRecordReader) Err() error                     { return r.err }

func (r *streamingRecordReader) Retain() {
	r.refs++
}

func (r *streamingRecordReader) Release() {
	r.refs--
	if r.refs > 0 {
		return
	}
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.stop != nil {
		r.stop()
		r.stop = nil
	}
}

// snapshotFileStats walks the current snapshot's manifests and returns
// (data file count, total bytes, total rows).
func snapshotFileStats(ctx context.Context, tbl *icebergtable.Table) (files int, bytes int64, rows int64) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return 0, 0, 0
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return 0, 0, 0
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return 0, 0, 0
	}
	for _, m := range manifests {
		mf, err := fs.Open(m.FilePath())
		if err != nil {
			continue
		}
		entries, err := icebergpkg.ReadManifest(m, mf, true)
		mf.Close()
		if err != nil {
			continue
		}
		for _, e := range entries {
			df := e.DataFile()
			if df == nil || df.ContentType() != icebergpkg.EntryContentData {
				continue
			}
			files++
			bytes += df.FileSizeBytes()
			rows += df.Count()
		}
	}
	return files, bytes, rows
}

func maxF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// absoluteMetadataURL converts a warehouse URL plus a bucket-relative key
// into the absolute URL that iceberg-go's IO registry expects. For
// gocloud.dev/blob URLs (s3://bucket?...), the key becomes a path component
// of an s3:// URL like s3://bucket/db/table/metadata/v3.metadata.json.
//
// This is intentionally simple and only handles the schemes the MVP
// supports (file://, s3://). The full implementation in pkg/blob will be
// more complete.
func absoluteMetadataURL(warehouseURL, key string) (string, error) {
	switch {
	case strings.HasPrefix(warehouseURL, "file://"):
		// file:///tmp/warehouse + db/table/metadata/v1.metadata.json
		base := strings.TrimPrefix(warehouseURL, "file://")
		// strip query string if any (gocloud.dev allows it)
		if i := strings.Index(base, "?"); i >= 0 {
			base = base[:i]
		}
		base = strings.TrimSuffix(base, "/")
		return "file://" + base + "/" + key, nil
	case strings.HasPrefix(warehouseURL, "s3://"):
		// s3://bucket?endpoint=...&region=... + key
		// iceberg-go's gocloud-backed s3 IO accepts s3://bucket/key
		// and reads endpoint/region from props or env vars.
		rest := strings.TrimPrefix(warehouseURL, "s3://")
		bucketName := rest
		if i := strings.Index(rest, "?"); i >= 0 {
			bucketName = rest[:i]
		}
		bucketName = strings.TrimSuffix(bucketName, "/")
		return "s3://" + bucketName + "/" + key, nil
	default:
		return "", fmt.Errorf("unsupported warehouse URL scheme: %s", warehouseURL)
	}
}

func printReport(tablePath string, r *analyzer.HealthReport) {
	fmt.Printf("Table:               %s\n", tablePath)
	fmt.Printf("Location:            %s\n", r.TableLocation)
	fmt.Printf("UUID:                %s\n", r.TableUUID)
	fmt.Printf("Format version:      v%d\n", r.FormatVersion)
	fmt.Printf("Current snapshot:    %d\n", r.CurrentSnapshotID)
	fmt.Printf("Snapshot count:      %d\n", r.SnapshotCount)
	fmt.Println()
	fmt.Printf("Data files:          %d\n", r.DataFileCount)
	fmt.Printf("Data bytes:          %s (%d)\n", humanBytes(r.DataBytes), r.DataBytes)
	fmt.Printf("Total rows:          %d\n", r.TotalRowCount)
	fmt.Println()
	fmt.Printf("Small file threshold:%s\n", humanBytes(r.SmallFileThreshold))
	fmt.Printf("Small files:         %d (%.1f%% of all data files)\n", r.SmallFileCount, r.SmallFileRatio*100)
	fmt.Printf("Small file bytes:    %s\n", humanBytes(r.SmallFileBytes))
	fmt.Println()
	fmt.Printf("Manifests:           %d\n", r.ManifestCount)
	fmt.Printf("Manifest bytes:      %s\n", humanBytes(r.ManifestBytes))
	fmt.Printf("Avg manifest bytes:  %s\n", humanBytes(r.AvgManifestBytes))
	fmt.Println()
	fmt.Printf("Metadata bytes:      %s\n", humanBytes(r.MetadataBytes))
	fmt.Printf("Metadata/data ratio: %.4f%%   <-- the H1 axiom (CB3): healthy < 5%%, critical > 10%%\n", r.MetadataDataRatio*100)
	fmt.Println()
	switch {
	case r.IsCritical:
		fmt.Printf("STATUS: CRITICAL — %s\n", r.AttentionReason)
	case r.NeedsAttention:
		fmt.Printf("STATUS: needs attention — %s\n", r.AttentionReason)
	case r.IsHealthy:
		fmt.Printf("STATUS: healthy\n")
	}
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
