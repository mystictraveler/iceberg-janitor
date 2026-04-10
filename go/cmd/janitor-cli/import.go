package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icebergpkg "github.com/apache/iceberg-go"
	icebergcat "github.com/apache/iceberg-go/catalog"
	icebergtable "github.com/apache/iceberg-go/table"
	"gocloud.dev/blob"

	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/janitor"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/maintenance"
)

// runImport converts a directory of existing parquet files into an
// Iceberg table in place. The parquet files stay where they are; we
// write the iceberg metadata (manifests, manifest_list, metadata.json)
// into a metadata/ directory alongside the data.
//
// The flow:
//  1. Scan for *.parquet files under <table_path>/
//  2. Read the first file's footer for schema
//  3. Infer partition spec from hive-style directory paths (col=value/)
//  4. CreateTable via DirectoryCatalog (writes v1.metadata.json)
//  5. Transaction.AddFiles registers all parquet files (reads footers
//     for stats + partition values)
//  6. Commit
//  7. If --compact is set, run maintain (expire → rewrite → compact →
//     rewrite) to optimize the files. Large files are skipped by
//     Pattern B; small files get stitched together.
//
// Usage:
//
//	janitor-cli import <table_path> [--compact] [--partition-spec col1,col2,...]
//
// The table_path is relative to JANITOR_WAREHOUSE_URL. For example:
//
//	JANITOR_WAREHOUSE_URL=s3://my-bucket janitor-cli import mydb.db/events
//
// imports the parquet files at s3://my-bucket/mydb.db/events/data/**/*.parquet
// and writes metadata to s3://my-bucket/mydb.db/events/metadata/.
func runImport(args []string) error {
	var tablePath string
	var doCompact bool
	var partCols string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--compact":
			doCompact = true
		case a == "--partition-spec" && i+1 < len(args):
			partCols = args[i+1]
			i++
		case strings.HasPrefix(a, "--partition-spec="):
			partCols = strings.TrimPrefix(a, "--partition-spec=")
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
		return fmt.Errorf("usage: janitor-cli import <table_path> [--compact] [--partition-spec col1,col2,...]")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg, err := loadConfig()
	if err != nil {
		return err
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

	// Step 1: discover parquet files.
	fmt.Printf("scanning %s for parquet files...\n", tablePath)
	bucket := cat.Bucket()
	prefix := tablePath + "/"
	var parquetFiles []string
	iter := bucket.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			break
		}
		if strings.HasSuffix(obj.Key, ".parquet") && !strings.Contains(obj.Key, "/metadata/") {
			parquetFiles = append(parquetFiles, obj.Key)
		}
	}
	if len(parquetFiles) == 0 {
		return fmt.Errorf("no .parquet files found under %s", tablePath)
	}
	fmt.Printf("found %d parquet files\n", len(parquetFiles))

	// Step 2: read first file's schema.
	fmt.Printf("reading schema from %s...\n", parquetFiles[0])
	schema, err := readParquetSchema(ctx, bucket, parquetFiles[0])
	if err != nil {
		return fmt.Errorf("reading schema from %s: %w", parquetFiles[0], err)
	}

	// Convert arrow schema to iceberg schema.
	// Use WithFreshIDs because parquet files from non-iceberg writers
	// typically don't carry arrow field IDs. Fresh IDs are safe
	// because this is a new table with no prior schema history.
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		return fmt.Errorf("converting schema: %w", err)
	}

	// Step 3: infer partition spec from directory paths or --partition-spec flag.
	var spec *icebergpkg.PartitionSpec
	if partCols != "" {
		spec, err = buildPartitionSpecFromCols(partCols, icebergSchema)
		if err != nil {
			return fmt.Errorf("building partition spec: %w", err)
		}
	} else {
		spec = inferPartitionSpec(parquetFiles, tablePath, icebergSchema)
	}
	if spec != nil {
		fmt.Printf("partition spec: %d field(s)\n", spec.NumFields())
	} else {
		fmt.Println("unpartitioned table")
	}

	// Step 4: create the table.
	fmt.Printf("creating iceberg table at %s...\n", tablePath)
	var createOpts []icebergcat.CreateTableOpt
	if spec != nil {
		createOpts = append(createOpts, icebergcat.WithPartitionSpec(spec))
	}
	tbl, err := cat.CreateTable(ctx, ident, icebergSchema, createOpts...)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	fmt.Printf("table created: uuid=%s\n", tbl.Metadata().TableUUID())

	// Step 5: register all parquet files via AddFiles.
	// Convert relative bucket keys to absolute URLs that iceberg-go
	// can open via its IO layer.
	absFiles := make([]string, len(parquetFiles))
	for i, key := range parquetFiles {
		abs, err := catalog.AbsoluteURL(cfg.Warehouse.URL, key)
		if err != nil {
			return fmt.Errorf("resolving %s: %w", key, err)
		}
		absFiles[i] = abs
	}

	fmt.Printf("registering %d files...\n", len(absFiles))
	tx := tbl.NewTransaction()
	if err := tx.AddFiles(ctx, absFiles, nil, false); err != nil {
		return fmt.Errorf("adding files: %w", err)
	}
	newTbl, err := tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("committing import: %w", err)
	}

	files, bytes, rows := janitor.SnapshotFileStats(ctx, newTbl)
	fmt.Printf("import complete: %d data files, %s, %d rows\n",
		files, humanBytes(bytes), rows)

	// Step 7: optional compact.
	if doCompact {
		fmt.Println("running maintain (expire → rewrite → compact → rewrite)...")
		result, err := maintenance.RewriteManifests(ctx, cat, ident, maintenance.RewriteManifestsOptions{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: rewrite-manifests failed: %v\n", err)
		} else {
			fmt.Printf("  rewrite-manifests: %d → %d manifests\n",
				result.BeforeManifests, result.AfterManifests)
		}

		compactResult, err := janitor.CompactTable(ctx, cat, ident, janitor.CompactOptions{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: compact failed: %v\n", err)
		} else {
			fmt.Printf("  compact: %d partitions found, %d succeeded, %d failed\n",
				compactResult.PartitionsFound, compactResult.PartitionsSucceeded, compactResult.PartitionsFailed)
		}

		rewrite2, err := maintenance.RewriteManifests(ctx, cat, ident, maintenance.RewriteManifestsOptions{})
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: post-compact rewrite-manifests failed: %v\n", err)
		} else {
			fmt.Printf("  rewrite-manifests (post-compact): %d → %d manifests\n",
				rewrite2.BeforeManifests, rewrite2.AfterManifests)
		}

		tbl, _ = cat.LoadTable(ctx, ident)
		files, bytes, rows = janitor.SnapshotFileStats(ctx, tbl)
		fmt.Printf("after compact: %d data files, %s, %d rows\n",
			files, humanBytes(bytes), rows)
	}

	return nil
}

// readParquetSchema opens a parquet file from the bucket and returns
// its Arrow schema.
func readParquetSchema(ctx context.Context, bucket *blob.Bucket, key string) (*arrow.Schema, error) {
	r, err := bucket.NewReader(ctx, key, nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	pf, err := file.NewParquetReader(bytes.NewReader(body), file.WithReadProps(parquet.NewReaderProperties(nil)))
	if err != nil {
		return nil, err
	}
	defer pf.Close()
	arrowSchema, err := pqarrow.FromParquet(pf.MetaData().Schema, nil, nil)
	if err != nil {
		return nil, err
	}
	return arrowSchema, nil
}

// inferPartitionSpec examines the parquet file paths for hive-style
// directory names (col=value/) and builds a partition spec from the
// first one found. Returns nil for unpartitioned tables.
func inferPartitionSpec(files []string, tablePrefix string, schema *icebergpkg.Schema) *icebergpkg.PartitionSpec {
	for _, f := range files {
		rel := strings.TrimPrefix(f, tablePrefix+"/")
		parts := strings.Split(filepath.Dir(rel), "/")
		var fields []icebergpkg.PartitionField
		fieldID := 1000
		for _, p := range parts {
			eq := strings.Index(p, "=")
			if eq < 0 {
				continue
			}
			colName := p[:eq]
			srcField, ok := schema.FindFieldByName(colName)
			if !ok {
				continue
			}
			fields = append(fields, icebergpkg.PartitionField{
				SourceID:  srcField.ID,
				FieldID:   fieldID,
				Name:      colName,
				Transform: icebergpkg.IdentityTransform{},
			})
			fieldID++
		}
		if len(fields) > 0 {
			spec := icebergpkg.NewPartitionSpec(fields...)
			return &spec
		}
	}
	return nil
}

// buildPartitionSpecFromCols builds a partition spec from a
// comma-separated list of column names. All columns use identity
// transforms.
func buildPartitionSpecFromCols(cols string, schema *icebergpkg.Schema) (*icebergpkg.PartitionSpec, error) {
	names := strings.Split(cols, ",")
	var fields []icebergpkg.PartitionField
	fieldID := 1000
	for _, name := range names {
		name = strings.TrimSpace(name)
		srcField, ok := schema.FindFieldByName(name)
		if !ok {
			return nil, fmt.Errorf("column %q not found in schema", name)
		}
		fields = append(fields, icebergpkg.PartitionField{
			SourceID:  srcField.ID,
			FieldID:   fieldID,
			Name:      name,
			Transform: icebergpkg.IdentityTransform{},
		})
		fieldID++
	}
	spec := icebergpkg.NewPartitionSpec(fields...)
	return &spec, nil
}
