// Command janitor-streamer is the TPC-DS streaming benchmark writer.
// It seeds the 9 dimension tables once at startup and then streams
// micro-batches into the 3 fact tables (store_sales, store_returns,
// catalog_sales) at a configurable cadence to simulate the kind of
// high-frequency commit pattern that Flink, Kafka Connect, or any
// other streaming Iceberg writer produces in production.
//
// The schemas, generators, and benchmark queries match the Python
// TPC-DS benchmark in tests/test_tpcds_benchmark.py and
// scripts/tpcds_*.py — same field shapes, same row distribution
// behavior, same SQL queries — so the Go and Python benchmark
// numbers are directly comparable.
//
// This is the writer half of the AWS streaming benchmark harness.
// It's a test fixture, not a production ingestion engine.
//
// Configuration
// =============
//
//	JANITOR_WAREHOUSE_URL    gocloud.dev/blob URL of the warehouse.
//	                         s3://my-warehouse for AWS, file://... for
//	                         local, s3://warehouse?endpoint=...&... for MinIO.
//	NAMESPACE                Iceberg namespace. Default "tpcds".
//	CATALOG_DB               SqlCatalog state file (write-side
//	                         convenience; the janitor reads via the
//	                         directory catalog and never touches this).
//	                         Default /tmp/janitor-bench-catalog.db.
//
//	COMMITS_PER_MINUTE       Streaming cadence per fact table.
//	                         Default 10. Set to 0 for max throughput.
//	STORE_SALES_PER_BATCH    Rows per store_sales commit. Default 500.
//	STORE_RETURNS_PER_BATCH  Rows per store_returns commit. Default 100.
//	CATALOG_SALES_PER_BATCH  Rows per catalog_sales commit. Default 300.
//	DURATION_SECONDS         How long to stream. Default 600 (10 min).
//	                         0 means run forever (until SIGTERM).
//	NUM_STREAM_BATCHES       Stop after this many TOTAL fact-table
//	                         commits across all 3 facts. Default 0
//	                         (no cap; use DURATION_SECONDS instead).
//
//	TRUNCATE_TABLES          If "true", drop and recreate all tables
//	                         before starting. Default true.
//	SKIP_DIMENSIONS          If "true", do not seed the dimension
//	                         tables (assume they already exist).
//	                         Default false.
//
//	SEED                     Generator RNG seed. Default 42 (matches
//	                         the Python generator's _RNG seed).
//
//	S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION
//	                         For MinIO / S3-compatible. AWS uses the
//	                         standard AWS credential chain.
//
// Usage
// =====
//
//	# Local fileblob, 1-minute smoke test:
//	JANITOR_WAREHOUSE_URL=file:///tmp/tpcds-bench \
//	  COMMITS_PER_MINUTE=30 DURATION_SECONDS=60 \
//	  go run ./cmd/janitor-streamer
//
//	# AWS S3, 1-hour streaming workload:
//	JANITOR_WAREHOUSE_URL=s3://my-warehouse \
//	  AWS_REGION=us-east-1 \
//	  COMMITS_PER_MINUTE=10 DURATION_SECONDS=3600 \
//	  go run ./cmd/janitor-streamer
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergpkg "github.com/apache/iceberg-go"
	icebergsql "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "github.com/apache/iceberg-go/io/gocloud"
	_ "modernc.org/sqlite"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/tpcds"
)

type config struct {
	WarehouseURL         string
	Namespace            string
	CatalogDB            string
	CommitsPerMinute     int
	StoreSalesPerBatch   int
	StoreReturnsPerBatch int
	CatalogSalesPerBatch int
	DurationSeconds      int
	NumStreamBatches     int
	TruncateTables       bool
	SkipDimensions       bool
	Seed                 uint64

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
}

func main() {
	cfg := loadConfig()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "streamer:", err)
		os.Exit(1)
	}
}

func loadConfig() config {
	getenv := func(key, def string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return def
	}
	getint := func(key string, def int) int {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			return def
		}
		return n
	}
	getbool := func(key string, def bool) bool {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		b, err := strconv.ParseBool(v)
		if err != nil {
			return def
		}
		return b
	}
	getuint64 := func(key string, def uint64) uint64 {
		v := os.Getenv(key)
		if v == "" {
			return def
		}
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return def
		}
		return n
	}
	return config{
		WarehouseURL:         getenv("JANITOR_WAREHOUSE_URL", "file:///tmp/janitor-bench"),
		Namespace:            getenv("NAMESPACE", "tpcds"),
		CatalogDB:            getenv("CATALOG_DB", "/tmp/janitor-bench-catalog.db"),
		CommitsPerMinute:     getint("COMMITS_PER_MINUTE", 10),
		StoreSalesPerBatch:   getint("STORE_SALES_PER_BATCH", 500),
		StoreReturnsPerBatch: getint("STORE_RETURNS_PER_BATCH", 100),
		CatalogSalesPerBatch: getint("CATALOG_SALES_PER_BATCH", 300),
		DurationSeconds:      getint("DURATION_SECONDS", 600),
		NumStreamBatches:     getint("NUM_STREAM_BATCHES", 0),
		TruncateTables:       getbool("TRUNCATE_TABLES", true),
		SkipDimensions:       getbool("SKIP_DIMENSIONS", false),
		Seed:                 getuint64("SEED", 42),
		S3Endpoint:           getenv("S3_ENDPOINT", ""),
		S3AccessKey:          getenv("S3_ACCESS_KEY", ""),
		S3SecretKey:          getenv("S3_SECRET_KEY", ""),
		S3Region:             getenv("S3_REGION", "us-east-1"),
	}
}

func run(ctx context.Context, cfg config) error {
	fmt.Printf("warehouse:        %s\n", cfg.WarehouseURL)
	fmt.Printf("namespace:        %s\n", cfg.Namespace)
	fmt.Printf("dimensions:       %d tables (date_dim, time_dim, item, customer, customer_address, customer_demographics, household_demographics, store, promotion)\n", len(tpcds.DimensionTables))
	fmt.Printf("facts:            %d tables × %d commits/min cadence\n", len(tpcds.FactTables), cfg.CommitsPerMinute)
	if cfg.DurationSeconds > 0 {
		fmt.Printf("duration:         %ds\n", cfg.DurationSeconds)
	} else if cfg.NumStreamBatches > 0 {
		fmt.Printf("commits cap:      %d\n", cfg.NumStreamBatches)
	} else {
		fmt.Printf("duration:         indefinite (Ctrl+C to stop)\n")
	}
	fmt.Println()

	// iceberg-go's table-level IO is constructed from table properties
	// (not catalog props), so the catalog-level S3 credentials don't
	// propagate through. Set the AWS_* env-var fallbacks.
	if cfg.S3Endpoint != "" {
		os.Setenv("AWS_S3_ENDPOINT", cfg.S3Endpoint)
		os.Setenv("AWS_ENDPOINT_URL_S3", cfg.S3Endpoint)
	}
	if cfg.S3AccessKey != "" {
		os.Setenv("AWS_ACCESS_KEY_ID", cfg.S3AccessKey)
	}
	if cfg.S3SecretKey != "" {
		os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.S3SecretKey)
	}
	if cfg.S3Region != "" {
		os.Setenv("AWS_REGION", cfg.S3Region)
		os.Setenv("AWS_DEFAULT_REGION", cfg.S3Region)
	}

	if err := os.MkdirAll(dirOf(cfg.CatalogDB), 0o755); err != nil {
		return fmt.Errorf("creating catalog db dir: %w", err)
	}
	db, err := sql.Open("sqlite", cfg.CatalogDB)
	if err != nil {
		return fmt.Errorf("opening sqlite catalog db: %w", err)
	}
	defer db.Close()

	props := icebergpkg.Properties{"warehouse": cfg.WarehouseURL}
	if cfg.S3Endpoint != "" {
		props["s3.endpoint"] = cfg.S3Endpoint
	}
	if cfg.S3AccessKey != "" {
		props["s3.access-key-id"] = cfg.S3AccessKey
	}
	if cfg.S3SecretKey != "" {
		props["s3.secret-access-key"] = cfg.S3SecretKey
	}
	if cfg.S3Region != "" {
		props["s3.region"] = cfg.S3Region
	}

	cat, err := icebergsql.NewCatalog("streamer", db, icebergsql.SQLite, props)
	if err != nil {
		return fmt.Errorf("constructing sql catalog: %w", err)
	}
	if err := cat.CreateSQLTables(ctx); err != nil {
		return fmt.Errorf("initializing sql catalog tables: %w", err)
	}

	nsIdent := icebergtable.Identifier{cfg.Namespace}
	if exists, _ := cat.CheckNamespaceExists(ctx, nsIdent); !exists {
		if err := cat.CreateNamespace(ctx, nsIdent, nil); err != nil {
			return fmt.Errorf("creating namespace %q: %w", cfg.Namespace, err)
		}
	}

	gen := tpcds.NewGenerator(cfg.Seed)

	// === Phase 1: seed dimensions (one-shot, batch) ===
	if !cfg.SkipDimensions {
		fmt.Println("Phase 1: seeding dimensions")
		for name, schema := range tpcds.DimensionTables {
			if err := seedDim(ctx, cat, gen, cfg.Namespace, name, schema, cfg.TruncateTables); err != nil {
				return fmt.Errorf("seeding %s: %w", name, err)
			}
		}
		fmt.Println()
	}

	// === Phase 2: stream facts (continuous, micro-batch) ===
	fmt.Println("Phase 2: streaming facts")

	// Make sure each fact table exists.
	factTables := map[string]*icebergtable.Table{}
	for name, schema := range tpcds.FactTables {
		ident := icebergtable.Identifier{cfg.Namespace, name}
		if cfg.TruncateTables {
			_ = cat.DropTable(ctx, ident)
		}
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			tbl, err = cat.CreateTable(ctx, ident, schema)
			if err != nil {
				return fmt.Errorf("creating %s: %w", name, err)
			}
		}
		factTables[name] = tbl
		fmt.Printf("  %s ready at %s\n", name, tbl.Location())
	}
	fmt.Println()

	// The fact tables are streamed round-robin: each "tick" picks one
	// fact and writes one batch into it. The cadence is per-table, so
	// the overall commit rate is COMMITS_PER_MINUTE × len(facts).
	factOrder := []string{"store_sales", "store_returns", "catalog_sales"}
	batchSizes := map[string]int{
		"store_sales":   cfg.StoreSalesPerBatch,
		"store_returns": cfg.StoreReturnsPerBatch,
		"catalog_sales": cfg.CatalogSalesPerBatch,
	}

	var sleepBetween time.Duration
	if cfg.CommitsPerMinute > 0 {
		// One commit per fact per (minute / commits_per_minute), then
		// the round-robin advances to the next fact.
		sleepBetween = time.Minute / time.Duration(cfg.CommitsPerMinute*len(factOrder))
	}

	started := time.Now()
	deadline := time.Time{}
	if cfg.DurationSeconds > 0 {
		deadline = started.Add(time.Duration(cfg.DurationSeconds) * time.Second)
	}

	totalCommits := 0
	commitsPerFact := map[string]int{}
	rowsPerFact := map[string]int{}
	progressEvery := 10 * time.Second
	nextProgress := started.Add(progressEvery)

	for {
		if ctx.Err() != nil {
			break
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		if cfg.NumStreamBatches > 0 && totalCommits >= cfg.NumStreamBatches {
			break
		}

		factName := factOrder[totalCommits%len(factOrder)]
		commitStart := time.Now()
		batch := batchSizes[factName]
		rec := genFactBatch(gen, factName, batch)
		atbl := array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
		newTbl, err := factTables[factName].AppendTable(ctx, atbl, int64(batch), nil)
		atbl.Release()
		rec.Release()
		if err != nil {
			return fmt.Errorf("append %s commit %d: %w", factName, commitsPerFact[factName], err)
		}
		factTables[factName] = newTbl
		totalCommits++
		commitsPerFact[factName]++
		rowsPerFact[factName] += batch

		if time.Now().After(nextProgress) {
			elapsed := time.Since(started).Round(time.Second)
			fmt.Printf("[%s] total %d commits  store_sales=%d store_returns=%d catalog_sales=%d\n",
				elapsed, totalCommits, commitsPerFact["store_sales"], commitsPerFact["store_returns"], commitsPerFact["catalog_sales"])
			nextProgress = time.Now().Add(progressEvery)
		}

		if sleepBetween > 0 {
			elapsed := time.Since(commitStart)
			if remaining := sleepBetween - elapsed; remaining > 0 {
				select {
				case <-ctx.Done():
					goto done
				case <-time.After(remaining):
				}
			}
		}
	}

done:
	elapsed := time.Since(started).Round(time.Second)
	fmt.Println()
	fmt.Printf("done: %d total commits in %s\n", totalCommits, elapsed)
	for _, name := range factOrder {
		fmt.Printf("  %s: %d commits, %d rows\n", name, commitsPerFact[name], rowsPerFact[name])
	}
	if elapsed.Seconds() > 0 {
		fmt.Printf("observed rate: %.2f commits/min total\n",
			float64(totalCommits)/elapsed.Seconds()*60)
	}
	return nil
}

// seedDim creates the dimension table (dropping it first if requested)
// and writes a single batch of dimension rows. Each dimension is
// generated once at startup and never updated during streaming.
func seedDim(ctx context.Context, cat *icebergsql.Catalog, gen *tpcds.Generator, ns, name string, schema *icebergpkg.Schema, truncate bool) error {
	ident := icebergtable.Identifier{ns, name}
	if truncate {
		_ = cat.DropTable(ctx, ident)
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		tbl, err = cat.CreateTable(ctx, ident, schema)
		if err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}

	rec := genDim(gen, name)
	atbl := array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
	defer atbl.Release()
	defer rec.Release()

	if _, err := tbl.AppendTable(ctx, atbl, int64(rec.NumRows()), nil); err != nil {
		return fmt.Errorf("append: %w", err)
	}
	fmt.Printf("  %s: %d rows\n", name, rec.NumRows())
	return nil
}

// genDim dispatches to the right dimension generator by name. Each
// dimension has its own row count (date_dim has NumDates rows,
// customer has NumCustomers rows, etc.) — see pkg/tpcds/datagen.go.
func genDim(g *tpcds.Generator, name string) arrow.Record {
	switch name {
	case "date_dim":
		return g.GenDateDim(0)
	case "time_dim":
		return g.GenTimeDim()
	case "item":
		return g.GenItem(0)
	case "customer":
		return g.GenCustomer(0)
	case "customer_address":
		return g.GenCustomerAddress(0)
	case "customer_demographics":
		return g.GenCustomerDemographics()
	case "household_demographics":
		return g.GenHouseholdDemographics()
	case "store":
		return g.GenStore(0)
	case "promotion":
		return g.GenPromotion(0)
	default:
		panic("unknown dimension: " + name)
	}
}

// genFactBatch dispatches to the right fact generator by name.
func genFactBatch(g *tpcds.Generator, name string, batch int) arrow.Record {
	switch name {
	case "store_sales":
		return g.GenStoreSalesBatch(batch)
	case "store_returns":
		return g.GenStoreReturnsBatch(batch)
	case "catalog_sales":
		return g.GenCatalogSalesBatch(batch)
	default:
		panic("unknown fact: " + name)
	}
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}
