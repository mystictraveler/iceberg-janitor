// Command janitor-cli is the local CLI for analyzing and maintaining
// Iceberg tables on any supported object store.
//
// MVP scope: discover tables in a warehouse and analyze a single table's
// health. No compaction yet (next iteration), no config package, no
// safety circuit breakers — the bare minimum to test the read loop
// against MinIO + DuckDB.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	icebergtable "github.com/apache/iceberg-go/table"
	"gocloud.dev/blob"

	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/analyzer"
	janitoraws "github.com/mystictraveler/iceberg-janitor/go/pkg/aws"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/config"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/safety"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/state"
)

// loadConfig loads the typed Config from the environment and applies the
// AWS_* env-var side effects that iceberg-go's table-level IO depends on.
// All run* commands route their env access through this so the binary
// has a single, validated configuration source per design decision #26.
func loadConfig() (*config.Config, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, err
	}
	cfg.ApplyAWSEnv()
	return cfg, nil
}

func newAPIClient(ctx context.Context, apiURL string) (*janitoraws.APIClient, error) {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}
	return janitoraws.NewAPIClient(ctx, apiURL, region)
}

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
	case "expire":
		if err := runExpire(args); err != nil {
			fmt.Fprintln(os.Stderr, "expire:", err)
			os.Exit(1)
		}
	case "rewrite-manifests":
		if err := runRewriteManifests(args); err != nil {
			fmt.Fprintln(os.Stderr, "rewrite-manifests:", err)
			os.Exit(1)
		}
	case "pause":
		if err := runPause(args); err != nil {
			fmt.Fprintln(os.Stderr, "pause:", err)
			os.Exit(1)
		}
	case "resume":
		if err := runResume(args); err != nil {
			fmt.Fprintln(os.Stderr, "resume:", err)
			os.Exit(1)
		}
	case "status":
		if err := runStatus(args); err != nil {
			fmt.Fprintln(os.Stderr, "status:", err)
			os.Exit(1)
		}
	case "glue-register":
		if err := runGlueRegister(args); err != nil {
			fmt.Fprintln(os.Stderr, "glue-register:", err)
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
                         non-bypassable. Optional flags:
                           --partition col=value
                               Scope to one iceberg partition.
                           --target-file-size SIZE
                               Skip-already-large-files threshold (Pattern B).
                               Files ≥ SIZE are kept unchanged. Bounds the
                               per-round cost. Suffixes: KB / MB / GB.
  expire <table_path>    Drop old snapshots from the table's metadata,
                         then GC the orphaned manifest_lists / manifests
                         / data files. The current snapshot (main branch
                         tip) is always retained. Optional flags:
                           --keep-last N
                               Minimum snapshots to retain on the parent
                               chain (default 5).
                           --keep-within DURATION
                               Minimum age before a snapshot is eligible
                               (default 168h = 7 days). Go duration syntax.
                           --no-delete
                               Stage and commit the metadata change but
                               skip the post-commit orphan file delete.
                               Use when consumers may still be reading
                               the expired snapshots' files.
  rewrite-manifests <table_path>
                         Consolidate per-commit micro-manifests in the
                         current snapshot into fewer larger manifests
                         organized by partition tuple. Pairs with expire:
                         expire drops snapshots, rewrite-manifests
                         shrinks the surviving snapshot's metadata
                         layer so manifest pruning is more effective on
                         the next compact / scan. Optional flags:
                           --min-manifests N
                               Skip rewrite if the snapshot has fewer
                               than N manifests (default 4).
  pause <table_path>     Manually pause maintenance on a table. Requires
                         --reason "<text>". Writes a pause file to
                         _janitor/control/paused/<uuid>.json that the next
                         compact will see and refuse to proceed against.
  resume <table_path>    Clear the pause file for a table AND reset the
                         CB8 consecutive-failure counter. Use after
                         investigating an auto-pause.
  status <table_path>    Print the per-table janitor state: pause file
                         (if any), consecutive failures, last errors.
  glue-register          Register all discovered Iceberg tables in a Glue
    --database <db>      database with column definitions extracted from
    [prefix]             the Iceberg metadata. Enables Athena queries.

ENVIRONMENT:
  JANITOR_API_URL         URL of a running janitor-server (e.g. the private
                          API Gateway endpoint). When set, ALL commands are
                          dispatched to the server via SigV4-signed HTTP
                          requests. The operator needs execute-api:Invoke
                          permission, NOT direct S3 access.

  JANITOR_WAREHOUSE_URL   gocloud.dev/blob URL of the warehouse bucket.
                          Used only when JANITOR_API_URL is NOT set (local /
                          standalone mode). Examples:
                            file:///tmp/warehouse
                            s3://my-warehouse?region=us-east-1
                            s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1

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

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	url := cfg.Warehouse.URL

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if apiURL := os.Getenv("JANITOR_API_URL"); apiURL != "" {
		return runDiscoverViaAPI(ctx, apiURL, rootPrefix)
	}

	if url == "" {
		return fmt.Errorf("JANITOR_WAREHOUSE_URL is not set (and JANITOR_API_URL is not set either)")
	}

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

func runDiscoverViaAPI(ctx context.Context, apiURL, prefix string) error {
	client, err := newAPIClient(ctx, apiURL)
	if err != nil {
		return err
	}
	tables, err := client.ListTables(ctx, prefix)
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
	cfg, err := loadConfig()
	if err != nil {
		return nil, "", nil, err
	}
	url := cfg.Warehouse.URL

	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", url, cfg.ToBlobProps())
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

	if apiURL := os.Getenv("JANITOR_API_URL"); apiURL != "" {
		return runAnalyzeViaAPI(ctx, apiURL, tablePath, jsonOut)
	}

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

func runAnalyzeViaAPI(ctx context.Context, apiURL, tablePath string, jsonOut bool) error {
	ident, err := tablePathToIdentifier(strings.Trim(tablePath, "/"))
	if err != nil {
		return err
	}
	if len(ident) != 2 {
		return fmt.Errorf("API mode supports two-part identifiers (namespace, table); got %v", ident)
	}
	client, err := newAPIClient(ctx, apiURL)
	if err != nil {
		return err
	}
	data, err := client.Analyze(ctx, ident[0]+".db", ident[1])
	if err != nil {
		return err
	}
	if jsonOut {
		var buf bytes.Buffer
		json.Indent(&buf, data, "", "  ")
		buf.WriteTo(os.Stdout)
		fmt.Println()
		return nil
	}
	// For non-JSON output, decode into HealthReport and use the standard printer.
	var report analyzer.HealthReport
	if err := json.Unmarshal(data, &report); err != nil {
		// Fallback: just print the raw JSON.
		fmt.Println(string(data))
		return nil
	}
	printReport(tablePath, &report)
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
// Optional flag: --partition <col>=<int_value> scopes the compaction
// to a single partition (the smaller-scope-per-compaction feature).
// Example: --partition ss_store_bucket=5
//
// Both modes call into the same pkg/janitor.Compact function and
// produce the same CompactResult; the only difference is which process
// holds the warehouse credentials.
func runCompact(args []string) error {
	var tablePath string
	var partitionFilter string
	var targetFileSize int64
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--partition" && i+1 < len(args):
			partitionFilter = args[i+1]
			i++
		case strings.HasPrefix(a, "--partition="):
			partitionFilter = strings.TrimPrefix(a, "--partition=")
		case a == "--target-file-size" && i+1 < len(args):
			parsed, err := parseFileSize(args[i+1])
			if err != nil {
				return fmt.Errorf("parsing --target-file-size: %w", err)
			}
			targetFileSize = parsed
			i++
		case strings.HasPrefix(a, "--target-file-size="):
			parsed, err := parseFileSize(strings.TrimPrefix(a, "--target-file-size="))
			if err != nil {
				return fmt.Errorf("parsing --target-file-size: %w", err)
			}
			targetFileSize = parsed
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
		return fmt.Errorf("usage: janitor-cli compact <table_path> [--partition col=value] [--target-file-size SIZE]")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	if cfg.Warehouse.APIURL != "" {
		// API mode doesn't yet support partition filters; warn if the
		// operator passed one with --partition.
		if partitionFilter != "" {
			fmt.Fprintln(os.Stderr, "warning: --partition is ignored in API mode (not yet supported by janitor-server)")
		}
		if targetFileSize > 0 {
			fmt.Fprintln(os.Stderr, "warning: --target-file-size is ignored in API mode (not yet supported by janitor-server)")
		}
		return runCompactViaAPI(ctx, cfg, tablePath)
	}
	return runCompactInProcess(ctx, cfg, tablePath, partitionFilter, targetFileSize)
}

// runExpire is the entry point for `janitor-cli expire <table_path>`.
// It mirrors runCompact's shape: parse flags, load config, route
// in-process to pkg/maintenance.Expire, print the result.
//
// API mode is intentionally NOT plumbed yet — the janitor-server side
// of expire (handler + result type wiring) hasn't been written. The
// flag parser will refuse to fall through to the server in that
// configuration so the operator gets a clear error rather than silent
// "did nothing".
func runExpire(args []string) error {
	var tablePath string
	keepLast := 5
	keepWithin := 7 * 24 * time.Hour
	postDelete := true
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--keep-last" && i+1 < len(args):
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid --keep-last %q: must be positive integer", args[i+1])
			}
			keepLast = n
			i++
		case strings.HasPrefix(a, "--keep-last="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--keep-last="))
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid --keep-last %q: must be positive integer", a)
			}
			keepLast = n
		case a == "--keep-within" && i+1 < len(args):
			d, err := time.ParseDuration(args[i+1])
			if err != nil || d <= 0 {
				return fmt.Errorf("invalid --keep-within %q: %v", args[i+1], err)
			}
			keepWithin = d
			i++
		case strings.HasPrefix(a, "--keep-within="):
			d, err := time.ParseDuration(strings.TrimPrefix(a, "--keep-within="))
			if err != nil || d <= 0 {
				return fmt.Errorf("invalid --keep-within %q: %v", a, err)
			}
			keepWithin = d
		case a == "--no-delete":
			postDelete = false
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
		return fmt.Errorf("usage: janitor-cli expire <table_path> [--keep-last N] [--keep-within DURATION] [--no-delete]")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.Warehouse.APIURL != "" {
		return fmt.Errorf("expire is not yet wired through janitor-server; unset JANITOR_API_URL to run in-process")
	}

	tablePath = strings.Trim(tablePath, "/")
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", cfg.Warehouse.URL, cfg.ToBlobProps())
	if err != nil {
		return err
	}
	defer cat.Close()

	ident, err := tablePathToIdentifier(tablePath)
	if err != nil {
		return err
	}

	opts := maintenance.ExpireOptions{
		KeepLast:         keepLast,
		KeepWithin:       keepWithin,
		PostCommitDelete: postDelete,
	}
	if !cfg.Safety.DisableCircuitBreaker {
		cb := safety.New(cat.Bucket())
		cb.TriggeredBy = "janitor-cli"
		opts.CircuitBreaker = cb
	}

	fmt.Printf("expiring %s (in-process; warehouse=%s; keep-last=%d; keep-within=%s; post-delete=%t)\n",
		tablePath, cfg.Warehouse.URL, keepLast, keepWithin, postDelete)
	result, err := maintenance.Expire(ctx, cat, ident, opts)
	if safety.IsPaused(err) {
		fmt.Fprintf(os.Stderr, "skipped: %v\n", err)
		fmt.Fprintf(os.Stderr, "         (use 'janitor-cli status %s' to inspect, 'resume' to clear)\n", tablePath)
		return err
	}
	printExpireResult(tablePath, result, err)
	return err
}

func printExpireResult(tablePath string, r *maintenance.ExpireResult, runErr error) {
	if r == nil {
		return
	}
	fmt.Printf("  before: %d snapshots, %d data files, %d rows (snapshot %d)\n",
		r.BeforeSnapshots, r.BeforeFiles, r.BeforeRows, r.BeforeSnapshotID)
	if r.Verification != nil {
		v := r.Verification
		fmt.Printf("  master check: %s\n", strings.ToUpper(v.Overall))
		fmt.Printf("    I1 row count:   in=%d out=%d (%s)\n",
			v.I1RowCount.In, v.I1RowCount.Out, v.I1RowCount.Result)
		fmt.Printf("    I2 schema:      id=%d (%s)\n",
			v.I2Schema.OutID, v.I2Schema.Result)
		fmt.Printf("    snap retain:    before=%d after=%d removed=%d (%s)\n",
			v.SnapshotRetain.BeforeSnapshots, v.SnapshotRetain.AfterSnapshots,
			v.SnapshotRetain.Removed, v.SnapshotRetain.Result)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "expire failed after %d attempt(s): %v\n", r.Attempts, runErr)
		return
	}
	fmt.Printf("  after:  %d snapshots, %d data files, %d rows (snapshot %d)\n",
		r.AfterSnapshots, r.AfterFiles, r.AfterRows, r.AfterSnapshotID)
	fmt.Printf("expire complete: removed %d snapshot(s) in %d attempt(s), %dms\n",
		len(r.RemovedSnapshotIDs), r.Attempts, r.DurationMs)
}

// runRewriteManifests is the entry point for `janitor-cli rewrite-manifests`.
// Same shape as runExpire: parse one optional flag, route in-process to
// pkg/maintenance.RewriteManifests, print the result.
func runRewriteManifests(args []string) error {
	var tablePath string
	minManifests := 4
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--min-manifests" && i+1 < len(args):
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid --min-manifests %q: must be positive integer", args[i+1])
			}
			minManifests = n
			i++
		case strings.HasPrefix(a, "--min-manifests="):
			n, err := strconv.Atoi(strings.TrimPrefix(a, "--min-manifests="))
			if err != nil || n <= 0 {
				return fmt.Errorf("invalid --min-manifests %q: must be positive integer", a)
			}
			minManifests = n
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
		return fmt.Errorf("usage: janitor-cli rewrite-manifests <table_path> [--min-manifests N]")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if cfg.Warehouse.APIURL != "" {
		return fmt.Errorf("rewrite-manifests is not yet wired through janitor-server; unset JANITOR_API_URL to run in-process")
	}

	tablePath = strings.Trim(tablePath, "/")
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", cfg.Warehouse.URL, cfg.ToBlobProps())
	if err != nil {
		return err
	}
	defer cat.Close()

	ident, err := tablePathToIdentifier(tablePath)
	if err != nil {
		return err
	}

	opts := maintenance.RewriteManifestsOptions{MinManifestsToTrigger: minManifests}
	if !cfg.Safety.DisableCircuitBreaker {
		cb := safety.New(cat.Bucket())
		cb.TriggeredBy = "janitor-cli"
		opts.CircuitBreaker = cb
	}

	fmt.Printf("rewriting manifests for %s (in-process; warehouse=%s; min-manifests=%d)\n",
		tablePath, cfg.Warehouse.URL, minManifests)
	result, err := maintenance.RewriteManifests(ctx, cat, ident, opts)
	if safety.IsPaused(err) {
		fmt.Fprintf(os.Stderr, "skipped: %v\n", err)
		fmt.Fprintf(os.Stderr, "         (use 'janitor-cli status %s' to inspect, 'resume' to clear)\n", tablePath)
		return err
	}
	printRewriteResult(tablePath, result, err)
	return err
}

func printRewriteResult(tablePath string, r *maintenance.RewriteManifestsResult, runErr error) {
	if r == nil {
		return
	}
	fmt.Printf("  before: %d manifests, %d data files, %d rows (snapshot %d)\n",
		r.BeforeManifests, r.BeforeDataFiles, r.BeforeRows, r.BeforeSnapshotID)
	if r.Verification != nil {
		v := r.Verification
		fmt.Printf("  master check: %s\n", strings.ToUpper(v.Overall))
		fmt.Printf("    I1 row count:   in=%d out=%d (%s)\n",
			v.I1RowCount.In, v.I1RowCount.Out, v.I1RowCount.Result)
		fmt.Printf("    I2 schema:      id=%d (%s)\n",
			v.I2Schema.OutID, v.I2Schema.Result)
	}
	if runErr != nil {
		fmt.Fprintf(os.Stderr, "rewrite-manifests failed after %d attempt(s): %v\n", r.Attempts, runErr)
		return
	}
	fmt.Printf("  after:  %d manifests, %d data files, %d rows (snapshot %d)\n",
		r.AfterManifests, r.AfterDataFiles, r.AfterRows, r.AfterSnapshotID)
	reduction := float64(r.BeforeManifests) / maxF(float64(r.AfterManifests), 1)
	fmt.Printf("rewrite-manifests complete: %d → %d manifests (%.1fx reduction) in %d attempt(s), %dms\n",
		r.BeforeManifests, r.AfterManifests, reduction, r.Attempts, r.DurationMs)
}

// parseFileSize parses a human-readable file size string like "1MB",
// "128MB", "512MB", "1024" (bytes), or "1GB" into an int64 byte count.
// Suffixes: KB / MB / GB (powers of 1024). No suffix means bytes.
// Case-insensitive on the suffix. Returns an error on unparseable
// input or non-positive sizes.
//
// This is the parser for the --target-file-size flag on
// `janitor-cli compact`. It exists to make Pattern B's threshold
// (CompactOptions.TargetFileSizeBytes) human-friendly without
// pulling in a heavyweight units package.
func parseFileSize(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	upper := strings.ToUpper(s)
	var multiplier int64 = 1
	switch {
	case strings.HasSuffix(upper, "KB"):
		multiplier = 1024
		s = s[:len(s)-2]
	case strings.HasSuffix(upper, "MB"):
		multiplier = 1024 * 1024
		s = s[:len(s)-2]
	case strings.HasSuffix(upper, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-2]
	}
	s = strings.TrimSpace(s)
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", s, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("size must be positive, got %d", n)
	}
	return n * multiplier, nil
}

// runCompactInProcess runs the compaction directly against the
// warehouse using the operator's credentials. This is the standalone
// fallback path; in production an operator would normally hit the API
// instead.
//
// If partitionFilter is non-empty (e.g. "ss_store_sk=5") it's parsed
// into a CompactOptions.PartitionTuple map and threaded through to
// pkg/janitor.Compact, which scopes the compaction to a single
// partition by walking the snapshot's manifest list and matching
// against DataFile.Partition().
//
// If targetFileSize > 0, Pattern B (skip-already-large-files) is
// enabled: files at or above the threshold are kept in the snapshot
// unchanged and only the small-file tail is read, merged, and
// rewritten. See CompactOptions.TargetFileSizeBytes for the
// architectural rationale.
func runCompactInProcess(ctx context.Context, cfg *config.Config, tablePath, partitionFilter string, targetFileSize int64) error {
	tablePath = strings.Trim(tablePath, "/")
	url := cfg.Warehouse.URL

	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", url, cfg.ToBlobProps())
	if err != nil {
		return err
	}
	defer cat.Close()

	ident, err := tablePathToIdentifier(tablePath)
	if err != nil {
		return err
	}

	opts := janitor.CompactOptions{
		TargetFileSizeBytes: targetFileSize,
	}
	if partitionFilter != "" {
		tuple, err := parsePartitionFilter(partitionFilter)
		if err != nil {
			return fmt.Errorf("parsing --partition: %w", err)
		}
		opts.PartitionTuple = tuple
	}
	// CB8: gate every compact attempt on the per-table circuit
	// breaker unless the operator explicitly disables it. The
	// breaker reads/writes _janitor/state/<uuid>.json and the
	// pause files under _janitor/control/paused/. Disabled via
	// JANITOR_DISABLE_CB=1 (escape hatch for the bench harness
	// when we want to observe raw failure modes without the
	// breaker hiding them; default is on).
	if !cfg.Safety.DisableCircuitBreaker {
		cb := safety.New(cat.Bucket())
		cb.TriggeredBy = "janitor-cli"
		opts.CircuitBreaker = cb
	}

	scope := "full table"
	if partitionFilter != "" {
		scope = "partition " + partitionFilter
	}
	if targetFileSize > 0 {
		scope = fmt.Sprintf("%s; skip files ≥ %s", scope, humanBytes(targetFileSize))
	}
	fmt.Printf("compacting %s (in-process; warehouse=%s; scope=%s)\n", tablePath, url, scope)
	result, err := janitor.Compact(ctx, cat, ident, opts)
	if safety.IsPaused(err) {
		// Surface CB8 pauses distinctly from genuine failures so
		// the operator's eye lands on the right line. The breaker
		// is doing its job here, not breaking.
		fmt.Fprintf(os.Stderr, "skipped: %v\n", err)
		fmt.Fprintf(os.Stderr, "         (use 'janitor-cli status %s' to inspect, 'resume' to clear)\n", tablePath)
		return err
	}
	printCompactResult(tablePath, result, err)
	return err
}

// parsePartitionFilter parses a "col=value" string into the
// PartitionTuple map that pkg/janitor.Compact expects. The CLI
// stores raw string values; pkg/janitor.compactOnce parses them as
// the corresponding iceberg types at runtime, after loading the
// table. This is what makes the CLI flag work for ANY iceberg
// partition column type — int, string, date, timestamp, uuid, etc.
// — without the CLI needing to know the iceberg type ahead of
// time. The supported set is documented on
// CompactOptions.PartitionTuple.
//
// The --partition flag will be removed entirely once AutoCompact
// reads the partition spec from manifest discovery and chooses
// partitions on its own.
func parsePartitionFilter(s string) (map[string]string, error) {
	eq := strings.Index(s, "=")
	if eq < 0 {
		return nil, fmt.Errorf("expected col=value, got %q", s)
	}
	col := strings.TrimSpace(s[:eq])
	valStr := strings.TrimSpace(s[eq+1:])
	if col == "" || valStr == "" {
		return nil, fmt.Errorf("expected col=value, got %q", s)
	}
	return map[string]string{col: valStr}, nil
}

// runCompactViaAPI POSTs to a running janitor-server with SigV4 auth
// and prints the returned CompactResult.
func runCompactViaAPI(ctx context.Context, cfg *config.Config, tablePath string) error {
	ident, err := tablePathToIdentifier(strings.Trim(tablePath, "/"))
	if err != nil {
		return err
	}
	if len(ident) != 2 {
		return fmt.Errorf("API mode currently supports only two-part identifiers (namespace, table); got %v", ident)
	}
	apiURL := cfg.Warehouse.APIURL
	endpoint := strings.TrimRight(apiURL, "/") + fmt.Sprintf("/v1/tables/%s/%s/compact", ident[0], ident[1])
	fmt.Printf("compacting %s (via API; %s)\n", tablePath, endpoint)

	client, err := newAPIClient(ctx, apiURL)
	if err != nil {
		return err
	}
	data, err := client.Compact(ctx, ident[0]+".db", ident[1])
	if err != nil {
		return err
	}
	var result janitor.CompactResult
	if err := json.Unmarshal(data, &result); err != nil {
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
	fmt.Printf("Workload class:      %s (foreign commits: %d in last 24h, %d in last 7d)\n",
		r.Workload.Class, r.Workload.ForeignCommitsLast24h, r.Workload.ForeignCommitsLast7d)
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

// resolveTableUUID opens the warehouse, loads the named table, and
// returns its UUID along with the open catalog (so the caller can
// reuse the underlying bucket for state/pause file ops). The caller
// must Close the catalog.
func resolveTableUUID(ctx context.Context, tablePath string) (*catalog.DirectoryCatalog, string, error) {
	tablePath = strings.Trim(tablePath, "/")
	cfg, err := loadConfig()
	if err != nil {
		return nil, "", err
	}
	url := cfg.Warehouse.URL
	cat, err := catalog.NewDirectoryCatalog(ctx, "janitor", url, cfg.ToBlobProps())
	if err != nil {
		return nil, "", err
	}
	ident, err := tablePathToIdentifier(tablePath)
	if err != nil {
		cat.Close()
		return nil, "", err
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		cat.Close()
		return nil, "", fmt.Errorf("loading table %v: %w", ident, err)
	}
	return cat, tbl.Metadata().TableUUID().String(), nil
}

// runPause writes a manual pause file for the given table. The
// operator must supply a reason string so future operators understand
// the intent. The reason ends up in _janitor/control/paused/<uuid>.json
// and is shown by `status`.
func runPause(args []string) error {
	var tablePath, reason string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--reason" && i+1 < len(args):
			reason = args[i+1]
			i++
		case strings.HasPrefix(a, "--reason="):
			reason = strings.TrimPrefix(a, "--reason=")
		case strings.HasPrefix(a, "--"):
			return fmt.Errorf("unknown flag %q", a)
		default:
			if tablePath != "" {
				return fmt.Errorf("multiple table paths provided")
			}
			tablePath = a
		}
	}
	if tablePath == "" || reason == "" {
		return fmt.Errorf("usage: janitor-cli pause <table_path> --reason \"<text>\"")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cat, uuid, err := resolveTableUUID(ctx, tablePath)
	if err != nil {
		return err
	}
	defer cat.Close()

	cb := safety.New(cat.Bucket())
	cb.TriggeredBy = "janitor-cli"
	if err := cb.ManualPause(ctx, uuid, reason); err != nil {
		return err
	}
	fmt.Printf("paused %s (uuid=%s)\n", tablePath, uuid)
	fmt.Printf("  reason: %s\n", reason)
	return nil
}

// runResume clears the pause file AND resets the failure counter so
// the next compact starts from a clean slate. Resetting the counter
// is essential — see safety.CircuitBreaker.Resume for the rationale.
func runResume(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: janitor-cli resume <table_path>")
	}
	tablePath := args[0]

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cat, uuid, err := resolveTableUUID(ctx, tablePath)
	if err != nil {
		return err
	}
	defer cat.Close()

	cb := safety.New(cat.Bucket())
	cb.TriggeredBy = "janitor-cli"
	if err := cb.Resume(ctx, uuid); err != nil {
		return err
	}
	fmt.Printf("resumed %s (uuid=%s)\n", tablePath, uuid)
	fmt.Printf("  pause file cleared, consecutive_failed_runs reset to 0\n")
	return nil
}

// runStatus prints the per-table janitor state plus the pause file
// (if any). The output is operator-facing, not machine-readable; a
// --json mode can land later.
func runStatus(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: janitor-cli status <table_path>")
	}
	tablePath := args[0]

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cat, uuid, err := resolveTableUUID(ctx, tablePath)
	if err != nil {
		return err
	}
	defer cat.Close()

	bucket := cat.Bucket()
	s, err := state.Load(ctx, bucket, uuid)
	if err != nil {
		return err
	}
	pause, err := state.LoadPause(ctx, bucket, uuid)
	if err != nil {
		return err
	}

	fmt.Printf("Table:                   %s\n", tablePath)
	fmt.Printf("UUID:                    %s\n", uuid)
	if pause != nil {
		fmt.Printf("Status:                  PAUSED\n")
		fmt.Printf("Pause reason:            %s\n", pause.Reason)
		fmt.Printf("Pause triggered at:      %s\n", pause.TriggeredAt.Format("2006-01-02 15:04:05 MST"))
		fmt.Printf("Pause triggered by:      %s\n", pause.TriggeredBy)
	} else {
		fmt.Printf("Status:                  active\n")
	}
	fmt.Printf("Consecutive failed runs: %d (CB8 trips at %d)\n", s.ConsecutiveFailedRuns, safety.CB8DefaultThreshold)
	if !s.LastSuccessAt.IsZero() {
		fmt.Printf("Last success:            %s\n", s.LastSuccessAt.Format("2006-01-02 15:04:05 MST"))
	}
	if s.LastOutcome != "" {
		fmt.Printf("Last outcome:            %s\n", s.LastOutcome)
	}
	if len(s.LastErrors) > 0 {
		fmt.Println("Recent errors:")
		for i, e := range s.LastErrors {
			msg := e.Message
			if len(msg) > 200 {
				msg = msg[:200] + "..."
			}
			fmt.Printf("  [%d] %s — %s\n", i+1, e.At.Format("15:04:05"), msg)
		}
	}
	return nil
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
