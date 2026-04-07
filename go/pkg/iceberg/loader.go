// Package iceberg is a thin wrapper over apache/iceberg-go for the specific
// reads the janitor needs (snapshot history, manifest list traversal, file
// stats). It exists to keep upstream churn from leaking into the rest of the
// codebase and to make swapping in custom implementations easier later.
package iceberg

import (
	"context"
	"fmt"

	icebergio "github.com/apache/iceberg-go/io"
	icebergtable "github.com/apache/iceberg-go/table"

	// Register the file:// IO scheme (built-in to iceberg-go) and the
	// gocloud.dev-backed schemes (s3, gs, azblob, mem) so that
	// io.LoadFSFunc can dispatch on the metadata location URL.
	_ "github.com/apache/iceberg-go/io/gocloud"
)

// LoadTable opens an Iceberg table whose current metadata.json lives at
// `metadataLocation`. The location may be any URL scheme registered with
// iceberg-go's IO registry: file://, s3://, gs://, azblob://, mem://.
//
// `props` is forwarded to the IO factory and is the right place to put
// per-store credentials and endpoint overrides (e.g., for MinIO:
// {"s3.endpoint": "http://localhost:9000", "s3.path-style-access": "true",
// "s3.region": "us-east-1"}).
//
// The returned *icebergtable.Table is read-only as far as the janitor's
// loader is concerned — the loader does not interact with any catalog
// service. Mutations come later via icebergtable.Table.NewTransaction().
func LoadTable(ctx context.Context, metadataLocation string, props map[string]string) (*icebergtable.Table, error) {
	if metadataLocation == "" {
		return nil, fmt.Errorf("metadata location is empty")
	}
	// Build the IO factory from the URL scheme. This is what
	// table.NewFromLocation will call to open the metadata.json and any
	// referenced manifest list / manifest files.
	fsysF := icebergio.LoadFSFunc(props, metadataLocation)
	// nil identifier is fine: there's no catalog and we identify the table
	// by its physical location, not by a logical (namespace, name) tuple.
	tbl, err := icebergtable.NewFromLocation(ctx, nil, metadataLocation, fsysF, nil)
	if err != nil {
		return nil, fmt.Errorf("loading iceberg table at %s: %w", metadataLocation, err)
	}
	return tbl, nil
}
