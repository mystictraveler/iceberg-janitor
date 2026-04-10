package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
	_ "gocloud.dev/blob/s3blob"

	janitoraws "github.com/mystictraveler/iceberg-janitor/go/pkg/aws"
	"github.com/mystictraveler/iceberg-janitor/go/pkg/catalog"
	icebergloader "github.com/mystictraveler/iceberg-janitor/go/pkg/iceberg"
)

func runGlueRegister(args []string) error {
	var database string
	var prefix string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "--database" && i+1 < len(args):
			database = args[i+1]
			i++
		case strings.HasPrefix(a, "--database="):
			database = strings.TrimPrefix(a, "--database=")
		case strings.HasPrefix(a, "--"):
			return fmt.Errorf("unknown flag %q", a)
		default:
			prefix = a
		}
	}
	if database == "" {
		return fmt.Errorf("usage: janitor-cli glue-register --database <glue_db> [prefix]")
	}

	warehouseURL := os.Getenv("JANITOR_WAREHOUSE_URL")
	if warehouseURL == "" {
		return fmt.Errorf("JANITOR_WAREHOUSE_URL is not set")
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// gocloud s3blob: s3://bucket/path opens bucket "bucket" — the path
	// is NOT a key prefix. We extract it and use PrefixedBucket.
	bucketURL, keyPrefix := splitWarehousePrefix(warehouseURL)

	rawBucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return fmt.Errorf("opening bucket %q: %w", bucketURL, err)
	}
	defer rawBucket.Close()

	bucket := rawBucket
	if keyPrefix != "" {
		bucket = blob.PrefixedBucket(rawBucket, keyPrefix)
	}

	tables, err := catalog.DiscoverTables(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("discovering tables: %w", err)
	}
	if len(tables) == 0 {
		fmt.Fprintln(os.Stderr, "no Iceberg tables found")
		return nil
	}

	glue, err := janitoraws.NewGlueRegistrar(ctx, region)
	if err != nil {
		return err
	}

	props := propsFromEnv()

	registered := 0
	for _, entry := range tables {
		// Build the absolute metadata URL from the warehouse URL + discovered prefix.
		metadataKey := entry.CurrentMetadata
		if keyPrefix != "" {
			metadataKey = keyPrefix + metadataKey
		}
		absMetadataURL, err := absoluteMetadataURL(bucketURL, metadataKey)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  skip %s: %v\n", entry.Prefix, err)
			continue
		}

		// Load the table via its absolute metadata URL to get the schema.
		tbl, err := icebergloader.LoadTable(ctx, absMetadataURL, props)
		if err != nil {
			fmt.Fprintf(os.Stderr, "  skip %s: can't load: %v\n", entry.Prefix, err)
			continue
		}

		// Table name for Glue is the last path component.
		parts := strings.Split(strings.Trim(entry.Prefix, "/"), "/")
		tableName := parts[len(parts)-1]

		schema := tbl.Metadata().CurrentSchema()
		tableLocation := tbl.Location()
		metadataLoc := tbl.MetadataLocation()

		glue.DeleteTable(ctx, database, tableName)
		if err := glue.RegisterTable(ctx, database, tableName, tableLocation, metadataLoc, schema); err != nil {
			fmt.Fprintf(os.Stderr, "  error: %s: %v\n", tableName, err)
			continue
		}
		fmt.Printf("  registered %s (%d columns)\n", tableName, len(schema.Fields()))
		registered++
	}

	fmt.Printf("glue-register complete: %d/%d tables in database %q\n", registered, len(tables), database)
	return nil
}

// splitWarehousePrefix extracts the key prefix from an s3:// URL.
// s3://bucket/with?region=us-east-1 → s3://bucket?region=us-east-1, "with/"
func splitWarehousePrefix(warehouseURL string) (string, string) {
	u, err := url.Parse(warehouseURL)
	if err != nil || u.Scheme != "s3" {
		return warehouseURL, ""
	}
	path := strings.TrimPrefix(u.Path, "/")
	if path == "" {
		return warehouseURL, ""
	}
	// Rebuild URL without the path.
	u.Path = ""
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return u.String(), path
}
