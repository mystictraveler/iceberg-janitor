// Command janitor-seed creates an Iceberg table at a configured warehouse
// URL and writes N small batches into it via Table.AppendTable, simulating
// the streaming-churn pattern (many small files) that the janitor exists to
// fix.
//
// This is a TEST FIXTURE program. It uses iceberg-go's catalog/sql with a
// pure-Go sqlite driver as the *write-side* catalog — that catalog is
// purely a build-time convenience for assigning the metadata pointer
// during table creation. The Go janitor itself reads the warehouse via
// its no-catalog-service directory catalog (pkg/catalog) and never
// touches the sqlite file. This is intentional and matches the
// production design: write-side tools may use any catalog they like; the
// janitor reads the warehouse layout directly.
//
// Environment variables:
//
//	JANITOR_WAREHOUSE_URL   Warehouse URL. Defaults to file:///tmp/janitor-mvp.
//	                        Use s3://warehouse for MinIO; set the s3.* env
//	                        vars below.
//	NAMESPACE               Iceberg namespace. Default "mvp".
//	TABLE                   Table name. Default "events".
//	NUM_BATCHES             How many micro-batches to write. Default 100.
//	ROWS_PER_BATCH          Rows per batch. Default 200.
//	CATALOG_DB              Path to the SqlCatalog sqlite file. Default
//	                        /tmp/janitor-mvp-catalog.db.
//
//	S3_ENDPOINT             For MinIO: http://localhost:9000
//	S3_ACCESS_KEY           For MinIO: minioadmin
//	S3_SECRET_KEY           For MinIO: minioadmin
//	S3_REGION               Default us-east-1
//
// After running, the warehouse contains a real Iceberg table with
// NUM_BATCHES small data files. Point the Go janitor at the same warehouse
// URL: `JANITOR_WAREHOUSE_URL=file:///tmp/janitor-mvp janitor-cli analyze mvp.db/events`.
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
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergsql "github.com/apache/iceberg-go/catalog/sql"
	icebergtable "github.com/apache/iceberg-go/table"

	// Register the gocloud-backed IO schemes (s3, gs, azblob, mem) and
	// the built-in file scheme so that table writes go to the configured
	// warehouse URL.
	_ "github.com/apache/iceberg-go/io/gocloud"

	// Pure-Go sqlite driver for the SqlCatalog state.
	_ "modernc.org/sqlite"
)

func main() {
	cfg := loadConfig()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
}

type config struct {
	WarehouseURL string
	Namespace    string
	Table        string
	NumBatches   int
	RowsPerBatch int
	CatalogDB    string

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
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
	return config{
		WarehouseURL: getenv("JANITOR_WAREHOUSE_URL", "file:///tmp/janitor-mvp"),
		Namespace:    getenv("NAMESPACE", "mvp"),
		Table:        getenv("TABLE", "events"),
		NumBatches:   getint("NUM_BATCHES", 100),
		RowsPerBatch: getint("ROWS_PER_BATCH", 200),
		CatalogDB:    getenv("CATALOG_DB", "/tmp/janitor-mvp-catalog.db"),
		S3Endpoint:   getenv("S3_ENDPOINT", ""),
		S3AccessKey:  getenv("S3_ACCESS_KEY", ""),
		S3SecretKey:  getenv("S3_SECRET_KEY", ""),
		S3Region:     getenv("S3_REGION", "us-east-1"),
	}
}

// schema is a small synthetic events schema. Field IDs match a typical
// streaming sink layout.
var schema = icebergpkg.NewSchema(0,
	icebergpkg.NestedField{ID: 1, Name: "event_id", Type: icebergpkg.PrimitiveTypes.String, Required: false},
	icebergpkg.NestedField{ID: 2, Name: "event_type", Type: icebergpkg.PrimitiveTypes.String, Required: false},
	icebergpkg.NestedField{ID: 3, Name: "user_id", Type: icebergpkg.PrimitiveTypes.String, Required: false},
	icebergpkg.NestedField{ID: 4, Name: "payload", Type: icebergpkg.PrimitiveTypes.String, Required: false},
	icebergpkg.NestedField{ID: 5, Name: "event_time", Type: icebergpkg.PrimitiveTypes.TimestampTz, Required: false},
)

func run(ctx context.Context, cfg config) error {
	fmt.Printf("warehouse:        %s\n", cfg.WarehouseURL)
	fmt.Printf("catalog db:       %s\n", cfg.CatalogDB)
	fmt.Printf("namespace.table:  %s.%s\n", cfg.Namespace, cfg.Table)
	fmt.Printf("batches:          %d × %d rows\n", cfg.NumBatches, cfg.RowsPerBatch)

	// iceberg-go's table-level IO is constructed from the TABLE's
	// properties (not the catalog's), so the catalog-level s3.endpoint
	// prop doesn't propagate to AppendTable's IO. The reliable
	// workaround is to set the env vars iceberg-go reads as fallbacks:
	// AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
	// AWS_REGION. They reach the AWS SDK regardless of how the IO is
	// instantiated.
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

	// 1. Open the sqlite catalog state file.
	if err := os.MkdirAll(dirOf(cfg.CatalogDB), 0o755); err != nil {
		return fmt.Errorf("creating catalog db dir: %w", err)
	}
	db, err := sql.Open("sqlite", cfg.CatalogDB)
	if err != nil {
		return fmt.Errorf("opening sqlite catalog db: %w", err)
	}
	defer db.Close()

	// 2. Build catalog properties. The warehouse URL is the home for new
	//    table data, manifests, and metadata files. S3 credentials and
	//    endpoint are forwarded into iceberg-go's IO layer via props.
	props := icebergpkg.Properties{
		"warehouse": cfg.WarehouseURL,
	}
	if cfg.S3Endpoint != "" {
		props["s3.endpoint"] = cfg.S3Endpoint
		props["s3.path-style-access"] = "true"
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

	cat, err := icebergsql.NewCatalog("seed", db, icebergsql.SQLite, props)
	if err != nil {
		return fmt.Errorf("constructing sql catalog: %w", err)
	}
	if err := cat.CreateSQLTables(ctx); err != nil {
		return fmt.Errorf("initializing sql catalog tables: %w", err)
	}

	// 3. Create the namespace if it doesn't exist.
	nsIdent := icebergtable.Identifier{cfg.Namespace}
	if exists, _ := cat.CheckNamespaceExists(ctx, nsIdent); !exists {
		if err := cat.CreateNamespace(ctx, nsIdent, nil); err != nil {
			return fmt.Errorf("creating namespace %q: %w", cfg.Namespace, err)
		}
	}

	// 4. Drop and recreate the table so the seed is reproducible.
	tableIdent := icebergtable.Identifier{cfg.Namespace, cfg.Table}
	_ = cat.DropTable(ctx, tableIdent)
	tbl, err := cat.CreateTable(ctx, tableIdent, schema)
	if err != nil {
		return fmt.Errorf("creating table %q: %w", cfg.Table, err)
	}
	fmt.Printf("created table at %s\n", tbl.Location())

	// 5. Build the Arrow schema once. AppendTable infers from the table's
	//    iceberg schema, but we need an Arrow schema to construct each batch.
	arrowSchema, err := icebergtable.SchemaToArrowSchema(schema, nil, false, false)
	if err != nil {
		return fmt.Errorf("converting schema to arrow: %w", err)
	}

	// 6. Append batches in a loop. Each AppendTable produces one new
	//    snapshot, one new manifest list, and at least one new data file.
	pool := memory.NewGoAllocator()
	started := time.Now()
	for i := 0; i < cfg.NumBatches; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rec := buildBatch(pool, arrowSchema, cfg.RowsPerBatch, i)
		atbl := array.NewTableFromRecords(arrowSchema, []arrow.Record{rec})
		newTbl, err := tbl.AppendTable(ctx, atbl, int64(cfg.RowsPerBatch), nil)
		atbl.Release()
		rec.Release()
		if err != nil {
			return fmt.Errorf("append batch %d: %w", i, err)
		}
		tbl = newTbl
		if (i+1)%25 == 0 {
			fmt.Printf("  wrote %d/%d batches\n", i+1, cfg.NumBatches)
		}
	}
	elapsed := time.Since(started)
	fmt.Printf("done in %s — %d small files written\n", elapsed.Round(time.Millisecond), cfg.NumBatches)

	if snap := tbl.CurrentSnapshot(); snap != nil {
		fmt.Printf("current snapshot: %d\n", snap.SnapshotID)
	}
	fmt.Printf("\nNext: JANITOR_WAREHOUSE_URL=%s janitor-cli analyze %s.db/%s\n",
		cfg.WarehouseURL, cfg.Namespace, cfg.Table)
	return nil
}

// buildBatch produces one Arrow record of synthetic event data. Field order
// MUST match the iceberg schema's order.
func buildBatch(pool memory.Allocator, schema *arrow.Schema, rows int, batchIdx int) arrow.Record {
	bldr := array.NewRecordBuilder(pool, schema)
	defer bldr.Release()

	now := time.Now().UTC()
	eventTypes := []string{"click", "pageview", "purchase", "signup", "logout", "search"}

	eid := bldr.Field(0).(*array.StringBuilder)
	etype := bldr.Field(1).(*array.StringBuilder)
	uid := bldr.Field(2).(*array.StringBuilder)
	payload := bldr.Field(3).(*array.StringBuilder)
	etime := bldr.Field(4).(*array.TimestampBuilder)

	for i := 0; i < rows; i++ {
		eid.Append(fmt.Sprintf("evt-%d-%d", batchIdx, i))
		etype.Append(eventTypes[(batchIdx+i)%len(eventTypes)])
		uid.Append(fmt.Sprintf("user_%d", (batchIdx*1000+i)%10000))
		payload.Append(fmt.Sprintf("payload-data-%d-%d-stub-stub-stub-stub-stub", batchIdx, i))
		etime.Append(arrow.Timestamp(now.UnixMicro()))
	}

	return bldr.NewRecord()
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[:i]
		}
	}
	return "."
}
