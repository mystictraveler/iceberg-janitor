//go:build bench

// Command delete-bench measures end-to-end V2 position-delete handling
// against a MinIO-backed warehouse.
//
// Pipeline:
//  1. Open a DirectoryCatalog at s3://<bucket>?endpoint=...
//  2. Drop + recreate a V2 merge-on-read table.
//  3. Seed N micro-batches × R rows via tx.Append (= N data files).
//  4. Fire K position-delete commits via tx.Delete with random
//     id-equality filters; each commit produces a position delete file
//     because of write.delete.mode=merge-on-read.
//  5. Reload the table, run janitor.Compact, time the wall clock.
//  6. Validate: rows_after == rows_before - K AND master check passes.
//
// Output: a CSV row + a human-readable summary on stdout.
//
// Env (defaults in brackets):
//
//	JANITOR_WAREHOUSE_URL  s3://warehouse?endpoint=...&...    [required]
//	S3_ENDPOINT            http://localhost:9000              [required]
//	S3_ACCESS_KEY          minioadmin                         [minioadmin]
//	S3_SECRET_KEY          minioadmin                         [minioadmin]
//	S3_REGION              us-east-1                          [us-east-1]
//	NAMESPACE              v2del                              [v2del]
//	TABLE                  events                             [events]
//	NUM_BATCHES            number of seed Append commits      [200]
//	ROWS_PER_BATCH         rows per Append                    [50]
//	NUM_DELETES            number of tx.Delete commits        [50]
//
// Build + run:
//
//	go build -tags bench -o /tmp/delete-bench ./test/bench/delete-bench
//	S3_ENDPOINT=http://localhost:9000 \
//	  JANITOR_WAREHOUSE_URL='s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1' \
//	  /tmp/delete-bench
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "github.com/apache/iceberg-go/io/gocloud"
	_ "gocloud.dev/blob/s3blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
)

type config struct {
	WarehouseURL string
	Namespace    string
	Table        string
	NumBatches   int
	RowsPerBatch int
	NumDeletes   int

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
		fmt.Fprintln(os.Stderr, "delete-bench:", err)
		os.Exit(1)
	}
}

func loadConfig() config {
	return config{
		WarehouseURL: must("JANITOR_WAREHOUSE_URL"),
		Namespace:    getenv("NAMESPACE", "v2del"),
		Table:        getenv("TABLE", "events"),
		NumBatches:   atoi(getenv("NUM_BATCHES", "200"), 200),
		RowsPerBatch: atoi(getenv("ROWS_PER_BATCH", "50"), 50),
		NumDeletes:   atoi(getenv("NUM_DELETES", "50"), 50),
		// S3 env vars are only required when the warehouse is on
		// s3://; in fileblob (file://) mode they are unused.
		S3Endpoint:  os.Getenv("S3_ENDPOINT"),
		S3AccessKey: getenv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey: getenv("S3_SECRET_KEY", "minioadmin"),
		S3Region:    getenv("S3_REGION", "us-east-1"),
	}
}

func run(ctx context.Context, cfg config) error {
	// Mirror env vars for iceberg-go's IO + AWS SDK fallback path
	// (same trick as test/bench/seed/main.go). Only meaningful when
	// the warehouse is s3://; harmless on file://.
	if cfg.S3Endpoint != "" {
		os.Setenv("AWS_S3_ENDPOINT", cfg.S3Endpoint)
		os.Setenv("AWS_ENDPOINT_URL_S3", cfg.S3Endpoint)
		os.Setenv("AWS_ACCESS_KEY_ID", cfg.S3AccessKey)
		os.Setenv("AWS_SECRET_ACCESS_KEY", cfg.S3SecretKey)
		os.Setenv("AWS_REGION", cfg.S3Region)
		os.Setenv("AWS_DEFAULT_REGION", cfg.S3Region)
	}

	fmt.Printf("=== V2 delete bench ===\n")
	fmt.Printf("warehouse:    %s\n", cfg.WarehouseURL)
	fmt.Printf("table:        %s.%s\n", cfg.Namespace, cfg.Table)
	fmt.Printf("seed shape:   %d batches × %d rows = %d rows\n",
		cfg.NumBatches, cfg.RowsPerBatch, cfg.NumBatches*cfg.RowsPerBatch)
	fmt.Printf("delete count: %d position-delete commits\n\n", cfg.NumDeletes)

	cat, err := catalog.NewDirectoryCatalog(ctx, "bench", cfg.WarehouseURL, map[string]string{})
	if err != nil {
		return fmt.Errorf("opening DirectoryCatalog: %w", err)
	}
	defer cat.Close()

	// 1. Drop + create a V2 merge-on-read table.
	ident := icebergtable.Identifier{cfg.Namespace, cfg.Table}
	_ = cat.DropTable(ctx, ident)

	schema := icebergpkg.NewSchema(1,
		icebergpkg.NestedField{ID: 1, Name: "id", Type: icebergpkg.Int64Type{}, Required: true},
		icebergpkg.NestedField{ID: 2, Name: "value", Type: icebergpkg.Int64Type{}},
		icebergpkg.NestedField{ID: 3, Name: "region", Type: icebergpkg.StringType{}},
	)
	tbl, err := cat.CreateTable(ctx, ident, schema, icebergcat.WithProperties(icebergpkg.Properties{
		icebergtable.PropertyFormatVersion: "2",
		icebergtable.WriteDeleteModeKey:    icebergtable.WriteModeMergeOnRead,
	}))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	fmt.Println("created v2 merge-on-read table")

	// 2. Seed via tx.Append. Each Append is one commit → one data file.
	seedStart := time.Now()
	totalRows := 0
	for b := 0; b < cfg.NumBatches; b++ {
		base := int64(b * cfg.RowsPerBatch)
		tbl, err = appendBatch(ctx, tbl, base, cfg.RowsPerBatch)
		if err != nil {
			return fmt.Errorf("append batch %d: %w", b, err)
		}
		totalRows += cfg.RowsPerBatch
		if b%50 == 49 {
			fmt.Printf("seeded %d / %d batches\n", b+1, cfg.NumBatches)
		}
	}
	seedDur := time.Since(seedStart)
	fmt.Printf("seeded: %d rows in %d batches in %v (%.1f batches/s)\n",
		totalRows, cfg.NumBatches, seedDur, float64(cfg.NumBatches)/seedDur.Seconds())

	// 3. Fire K position-delete commits. Each commit deletes one
	// random id from the seeded range. iceberg-go's classifier may
	// fully delete a file (rare for single-id) or rewrite it as a
	// position-delete tuple (the common case).
	delStart := time.Now()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	deletedIDs := make(map[int64]struct{}, cfg.NumDeletes)
	for d := 0; d < cfg.NumDeletes; d++ {
		var id int64
		for {
			id = rng.Int63n(int64(totalRows))
			if _, dup := deletedIDs[id]; !dup {
				break
			}
		}
		deletedIDs[id] = struct{}{}
		filter := icebergpkg.EqualTo(icebergpkg.Reference("id"), id)
		tx := tbl.NewTransaction()
		if err := tx.Delete(ctx, filter, nil); err != nil {
			return fmt.Errorf("tx.Delete id=%d: %w", id, err)
		}
		tbl, err = tx.Commit(ctx)
		if err != nil {
			return fmt.Errorf("commit delete id=%d: %w", id, err)
		}
		if d%10 == 9 {
			fmt.Printf("fired %d / %d deletes\n", d+1, cfg.NumDeletes)
		}
	}
	delDur := time.Since(delStart)
	fmt.Printf("deletes: %d commits in %v (%.1f commits/s)\n\n",
		cfg.NumDeletes, delDur, float64(cfg.NumDeletes)/delDur.Seconds())

	// 4. Pre-compact stats.
	tbl, err = cat.LoadTable(ctx, ident)
	if err != nil {
		return fmt.Errorf("reload table: %w", err)
	}
	beforeFiles, beforeRows := janitor.SnapshotFileStatsFast(ctx, tbl)
	fmt.Printf("pre-compact: %d data files, %d data rows (logical: %d after %d deletes)\n",
		beforeFiles, beforeRows, beforeRows-int64(cfg.NumDeletes), cfg.NumDeletes)

	// 5. Run Compact. This is the bench measurement.
	compactStart := time.Now()
	res, cerr := janitor.Compact(ctx, cat, ident, janitor.CompactOptions{
		TargetFileSizeBytes: 128 * 1024 * 1024,
	})
	compactDur := time.Since(compactStart)
	if cerr != nil {
		return fmt.Errorf("janitor.Compact: %w", cerr)
	}

	// 6. Validate.
	tbl, _ = cat.LoadTable(ctx, ident)
	afterFiles, afterRows := janitor.SnapshotFileStatsFast(ctx, tbl)
	expectedAfterRows := beforeRows - int64(cfg.NumDeletes)
	rowsOK := afterRows == expectedAfterRows
	masterOK := res.Verification != nil && res.Verification.Overall == "pass"

	fmt.Printf("\n=== RESULT ===\n")
	fmt.Printf("compact wall:       %v\n", compactDur)
	fmt.Printf("files: %d → %d  (%.1f× reduction)\n",
		beforeFiles, afterFiles, float64(beforeFiles)/float64(max1(afterFiles)))
	fmt.Printf("rows:  %d → %d  (expected %d, deletedHint=%d)\n",
		beforeRows, afterRows, expectedAfterRows, cfg.NumDeletes)
	fmt.Printf("master check:       %v\n", verdictStr(masterOK))
	fmt.Printf("row count correct:  %v\n", verdictStr(rowsOK))
	if res.Verification != nil {
		fmt.Printf("I1 row count:       in=%d DVs=%d out=%d (%s)\n",
			res.Verification.I1RowCount.In,
			res.Verification.I1RowCount.DVs,
			res.Verification.I1RowCount.Out,
			res.Verification.I1RowCount.Result)
	}

	// CSV-friendly single line at the end for easy aggregation.
	fmt.Printf("\nCSV: %s,%d,%d,%d,%d,%d,%d,%d,%v,%v,%v\n",
		"v2_delete_minio",
		cfg.NumBatches, cfg.RowsPerBatch, cfg.NumDeletes,
		beforeFiles, afterFiles,
		beforeRows, afterRows,
		compactDur.Milliseconds(),
		boolStr(masterOK), boolStr(rowsOK))

	if !rowsOK || !masterOK {
		return fmt.Errorf("bench failed validation")
	}
	return nil
}

func appendBatch(ctx context.Context, tbl *icebergtable.Table, base int64, n int) (*icebergtable.Table, error) {
	mem := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String},
	}, nil)
	idB := array.NewInt64Builder(mem)
	valB := array.NewInt64Builder(mem)
	regB := array.NewStringBuilder(mem)
	for i := 0; i < n; i++ {
		idB.Append(base + int64(i))
		valB.Append(int64(i))
		regB.Append("us")
	}
	idArr := idB.NewArray()
	defer idArr.Release()
	valArr := valB.NewArray()
	defer valArr.Release()
	regArr := regB.NewArray()
	defer regArr.Release()
	rec := array.NewRecord(schema, []arrow.Array{idArr, valArr, regArr}, int64(n))
	defer rec.Release()
	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	tx := tbl.NewTransaction()
	if err := tx.Append(ctx, rdr, nil); err != nil {
		return nil, err
	}
	return tx.Commit(ctx)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func must(k string) string {
	v := os.Getenv(k)
	if v == "" {
		fmt.Fprintf(os.Stderr, "delete-bench: %s is required\n", k)
		os.Exit(2)
	}
	return v
}
func atoi(s string, def int) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return n
}
func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
func boolStr(b bool) string {
	if b {
		return "PASS"
	}
	return "FAIL"
}
func verdictStr(b bool) string {
	if b {
		return "PASS ✓"
	}
	return "FAIL ✗"
}
