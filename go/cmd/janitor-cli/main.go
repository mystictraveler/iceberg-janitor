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
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	icebergtable "github.com/apache/iceberg-go/table"
	"gocloud.dev/blob"

	// Blank imports register URL openers with gocloud.dev/blob so that
	// blob.OpenBucket can dispatch on the URL scheme.
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
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

// runCompact compacts the named table. Two modes:
//
//   1. If JANITOR_API_URL is set, dispatch to a running janitor-server
//      via HTTP. The server runs the actual compaction with its own
//      warehouse credentials; the operator only needs network access
//      to the API.
//   2. Otherwise, run the compaction in-process using the operator's
//      warehouse credentials. This is the standalone fallback for
//      laptop dev and emergency operator access.
//
// Both modes call into the same pkg/janitor.Compact function and
// produce the same CompactResult; the only difference is which process
// holds the warehouse credentials.
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

	if apiURL := os.Getenv("JANITOR_API_URL"); apiURL != "" {
		return runCompactViaAPI(ctx, apiURL, tablePath)
	}
	return runCompactInProcess(ctx, tablePath)
}

// runCompactInProcess runs the compaction directly against the
// warehouse using the operator's credentials. This is the standalone
// fallback path; in production an operator would normally hit the API
// instead.
func runCompactInProcess(ctx context.Context, tablePath string) error {
	tablePath = strings.Trim(tablePath, "/")
	url := os.Getenv("JANITOR_WAREHOUSE_URL")
	if url == "" {
		return fmt.Errorf("JANITOR_WAREHOUSE_URL is not set (and JANITOR_API_URL is not set either)")
	}

	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", url, propsFromEnv())
	if err != nil {
		return err
	}
	defer cat.Close()

	ident, err := tablePathToIdentifier(tablePath)
	if err != nil {
		return err
	}

	fmt.Printf("compacting %s (in-process; warehouse=%s)\n", tablePath, url)
	result, err := janitor.Compact(ctx, cat, ident, janitor.CompactOptions{})
	printCompactResult(tablePath, result, err)
	return err
}

// runCompactViaAPI POSTs to a running janitor-server and prints the
// returned CompactResult.
func runCompactViaAPI(ctx context.Context, apiURL, tablePath string) error {
	ident, err := tablePathToIdentifier(strings.Trim(tablePath, "/"))
	if err != nil {
		return err
	}
	if len(ident) != 2 {
		return fmt.Errorf("API mode currently supports only two-part identifiers (namespace, table); got %v", ident)
	}
	endpoint := strings.TrimRight(apiURL, "/") + fmt.Sprintf("/v1/tables/%s/%s/compact", ident[0], ident[1])
	fmt.Printf("compacting %s (via API; %s)\n", tablePath, endpoint)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("calling janitor-server: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("janitor-server returned HTTP %d: %s", resp.StatusCode, string(body))
	}
	var result janitor.CompactResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}
	printCompactResult(tablePath, &result, nil)
	return nil
}

// printCompactResult writes a human-readable summary of the result to
// stdout. Used by both the in-process and API paths.
func printCompactResult(tablePath string, r *janitor.CompactResult, runErr error) {
	if r == nil {
		return
	}
	fmt.Printf("  before: %d data files, %s, %d rows (snapshot %d)\n",
		r.BeforeFiles, humanBytes(r.BeforeBytes), r.BeforeRows, r.BeforeSnapshotID)
	if r.Verification != nil {
		v := r.Verification
		fmt.Printf("  master check: %s\n", strings.ToUpper(v.Overall))
		fmt.Printf("    I1 row count:   in=%d out=%d (%s)\n",
			v.I1RowCount.In, v.I1RowCount.Out, v.I1RowCount.Result)
		fmt.Printf("    I2 schema:      id=%d (%s)\n",
			v.I2Schema.OutID, v.I2Schema.Result)
		fmt.Printf("    I3 value cnts:  %d/%d cols (%s)\n",
			v.I3ValueCounts.Passed, v.I3ValueCounts.Checked, v.I3ValueCounts.Result)
		fmt.Printf("    I4 null cnts:   %d/%d cols (%s)\n",
			v.I4NullCounts.Passed, v.I4NullCounts.Checked, v.I4NullCounts.Result)
		fmt.Printf("    I5 bounds:      in=%d out=%d cols (%s)\n",
			v.I5Bounds.InColumns, v.I5Bounds.OutColumns, v.I5Bounds.Result)
		fmt.Printf("    I7 manif refs:  %d/%d files (%s)\n",
			v.I7ManifestRefs.Passed, v.I7ManifestRefs.Checked, v.I7ManifestRefs.Result)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "compaction failed after %d attempt(s): %v\n", r.Attempts, runErr)
		return
	}
	fmt.Printf("  after:  %d data files, %s, %d rows (snapshot %d)\n",
		r.AfterFiles, humanBytes(r.AfterBytes), r.AfterRows, r.AfterSnapshotID)
	reduction := float64(r.BeforeFiles) / maxF(float64(r.AfterFiles), 1)
	fmt.Printf("compaction complete: %d → %d files (%.1fx reduction) in %d attempt(s), %dms\n",
		r.BeforeFiles, r.AfterFiles, reduction, r.Attempts, r.DurationMs)
}

// tablePathToIdentifier converts a janitor-style path ("mvp.db/events")
// into an iceberg-go Identifier (["mvp", "events"]). The leading
// namespace component is stripped of its ".db" suffix because that
// suffix is iceberg-go's default location-provider convention, not
// part of the namespace name.
func tablePathToIdentifier(tablePath string) (icebergtable.Identifier, error) {
	parts := strings.Split(tablePath, "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("table path %q must be at least <namespace>.db/<table>", tablePath)
	}
	ns := strings.TrimSuffix(parts[0], ".db")
	rest := parts[1:]
	return icebergtable.Identifier(append([]string{ns}, rest...)), nil
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
