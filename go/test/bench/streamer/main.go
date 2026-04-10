//go:build bench

// Command janitor-streamer is the TPC-DS streaming benchmark writer.
// It is a TEST FIXTURE — gated behind the `bench` build tag so it is
// not pulled into the production binaries' build graph. The streamer
// depends on iceberg-go's SqlCatalog and the pure-Go sqlite driver,
// which the production janitor does not use; gating keeps those
// dependencies out of `go build ./cmd/...`.
//
// Build it with:
//
//	go build -tags bench ./test/bench/streamer
//
// The bench harness (test/bench/bench-tpcds.sh) and the Makefile mvp
// targets pass `-tags bench` for this purpose.
//
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
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergsql "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "github.com/apache/iceberg-go/io/gocloud"
	_ "modernc.org/sqlite"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/tpcds"
)

// factPartitionSpec declares the partition spec the streamer applies
// to each fact table at create time. We use IDENTITY transforms on
// columns with good uniformity rather than bucket transforms because
// identity-partitioned columns can be directly filtered by row-level
// predicates (iceberg.EqualTo(iceberg.Reference("ss_store_sk"), 5)),
// which is what pkg/janitor.Compact's RowFilter expects. Bucket
// transforms produce a partition-field name that isn't in the row
// schema, so iceberg-go's predicate binder can't bind to it.
//
// Partition cardinality:
//   store_sales:   ss_store_sk        → 50 partitions (50 stores)
//   store_returns: sr_store_sk        → 50 partitions
//   catalog_sales: cs_call_center_sk  → 10 partitions (10 call centers)
//
// SourceID values match the field IDs in pkg/tpcds/schemas.go:
// ss_store_sk is field 8 of store_sales, sr_store_sk is field 8 of
// store_returns, cs_call_center_sk is field 12 of catalog_sales.
func factPartitionSpec(table string) icebergpkg.PartitionSpec {
	switch table {
	case "store_sales":
		return icebergpkg.NewPartitionSpec(icebergpkg.PartitionField{
			SourceID:  8,
			FieldID:   1000,
			Name:      "ss_store_sk",
			Transform: icebergpkg.IdentityTransform{},
		})
	case "store_returns":
		return icebergpkg.NewPartitionSpec(icebergpkg.PartitionField{
			SourceID:  8,
			FieldID:   1000,
			Name:      "sr_store_sk",
			Transform: icebergpkg.IdentityTransform{},
		})
	case "catalog_sales":
		return icebergpkg.NewPartitionSpec(icebergpkg.PartitionField{
			SourceID:  12,
			FieldID:   1000,
			Name:      "cs_call_center_sk",
			Transform: icebergpkg.IdentityTransform{},
		})
	}
	return *icebergpkg.UnpartitionedSpec
}

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
	Bursty               bool
	BurstMax             int
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
		Bursty:               getbool("BURSTY", false),
		BurstMax:             getint("BURST_MAX", 10),
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

	// Strip the query string from the warehouse URL before handing
	// it to iceberg-go's SqlCatalog. The query string (?endpoint=...,
	// ?s3ForcePathStyle=true, ?region=...) is meaningful to
	// gocloud.dev/blob.OpenBucket but iceberg-go's location provider
	// uses the warehouse property as a path PREFIX for newly-created
	// metadata files — it concatenates the warehouse string with the
	// table path and feeds the result to gocloud as a key, which
	// produces nonsense like
	//   s3://bucket?endpoint=.../tpcds.db/.../00000-...metadata.json
	// and gocloud rejects with "invalid argument". The S3 endpoint /
	// region / credentials are already passed via the s3.* properties
	// below, so the query string contributes nothing useful to
	// iceberg-go and can be safely dropped.
	warehouseForCatalog := cfg.WarehouseURL
	if i := strings.IndexByte(warehouseForCatalog, '?'); i >= 0 {
		warehouseForCatalog = warehouseForCatalog[:i]
	}
	props := icebergpkg.Properties{"warehouse": warehouseForCatalog}
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

	// Make sure each fact table exists. Apply a per-fact partition spec
	// at create time so the bench can compact one partition at a time
	// — the smaller-scope-per-compaction fix for the writer-fight
	// pathology that mystictraveler/iceberg-janitor#1 only partially
	// addressed.
	factTables := map[string]*icebergtable.Table{}
	for name, schema := range tpcds.FactTables {
		ident := icebergtable.Identifier{cfg.Namespace, name}
		if cfg.TruncateTables {
			_ = cat.DropTable(ctx, ident)
		}
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			spec := factPartitionSpec(name)
			tbl, err = cat.CreateTable(ctx, ident, schema, icebergcat.WithPartitionSpec(&spec))
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

	// Bursty mode: instead of uniform spacing, fire bursts of 1-N
	// commits with no delay, then sleep for a random quiet period
	// drawn from an exponential distribution so the average rate
	// stays at COMMITS_PER_MINUTE. This simulates real-world
	// streaming where micro-batches arrive in clumps (e.g. a Kafka
	// consumer draining a partition, or a cron job landing 5 files
	// at once then going quiet).
	burstRng := rand.New(rand.NewSource(int64(cfg.Seed + 999)))
	burstRemaining := 0 // commits left in the current burst

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

		if cfg.Bursty && sleepBetween > 0 {
			burstRemaining--
			if burstRemaining <= 0 {
				// End of burst. Pick the next burst size and compute
				// a quiet period. The quiet period is drawn from an
				// exponential distribution with mean = burstSize ×
				// sleepBetween, so the average throughput stays at
				// COMMITS_PER_MINUTE. The exponential distribution
				// naturally produces mostly-short and occasionally-
				// long pauses — realistic for streaming workloads.
				nextBurst := burstRng.Intn(cfg.BurstMax) + 1
				meanQuiet := float64(nextBurst) * float64(sleepBetween)
				quiet := time.Duration(burstRng.ExpFloat64() * meanQuiet)
				if quiet > 30*time.Second {
					quiet = 30 * time.Second // cap so the bench doesn't stall
				}
				burstRemaining = nextBurst
				select {
				case <-ctx.Done():
					goto done
				case <-time.After(quiet):
				}
			}
			// Within a burst: no sleep, fire immediately.
		} else if sleepBetween > 0 {
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
