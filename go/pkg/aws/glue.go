package aws

import (
	"context"
	"fmt"

	awspkg "github.com/aws/aws-sdk-go-v2/aws"
	icebergpkg "github.com/apache/iceberg-go"
)

// GlueRegistrar registers Iceberg tables in a Glue database using
// the Glue JSON API with SigV4 signing (no Glue SDK).
type GlueRegistrar struct {
	endpoint string
	region   string
	creds    awspkg.CredentialsProvider
}

// NewGlueRegistrar creates a registrar for the given region.
func NewGlueRegistrar(ctx context.Context, region string) (*GlueRegistrar, error) {
	creds, resolvedRegion, err := Creds(ctx, region)
	if err != nil {
		return nil, err
	}
	return &GlueRegistrar{
		endpoint: fmt.Sprintf("https://glue.%s.amazonaws.com", resolvedRegion),
		region:   resolvedRegion,
		creds:    creds,
	}, nil
}

// DeleteTable deletes a table from Glue (ignores errors).
func (g *GlueRegistrar) DeleteTable(ctx context.Context, database, table string) {
	_ = CallAWSJSON(ctx, g.creds, g.endpoint, "glue", g.region, "AWSGlue.DeleteTable", map[string]any{
		"DatabaseName": database,
		"Name":         table,
	})
}

// UpdateMetadataLocation updates an existing Glue table's
// metadata_location parameter. This is the fast path for
// propagating a janitor commit to Athena — one API call,
// milliseconds, no S3 discovery. The table must already exist in
// Glue (created by RegisterTable or a prior glue-register run).
func (g *GlueRegistrar) UpdateMetadataLocation(ctx context.Context, database, tableName, metadataLocation string) error {
	return CallAWSJSON(ctx, g.creds, g.endpoint, "glue", g.region, "AWSGlue.UpdateTable", map[string]any{
		"DatabaseName": database,
		"TableInput": map[string]any{
			"Name":      tableName,
			"TableType": "EXTERNAL_TABLE",
			"Parameters": map[string]string{
				"table_type":        "ICEBERG",
				"metadata_location": metadataLocation,
			},
		},
	})
}

// RegisterTable creates a Glue table pointing at an Iceberg table on S3.
// The schema columns are extracted from the Iceberg metadata so that
// Athena can resolve column references in queries.
func (g *GlueRegistrar) RegisterTable(ctx context.Context, database, tableName, tableLocation, metadataLocation string, schema *icebergpkg.Schema) error {
	columns := SchemaToGlueColumns(schema)

	return CallAWSJSON(ctx, g.creds, g.endpoint, "glue", g.region, "AWSGlue.CreateTable", map[string]any{
		"DatabaseName": database,
		"TableInput": map[string]any{
			"Name":      tableName,
			"TableType": "EXTERNAL_TABLE",
			"Parameters": map[string]string{
				"table_type":        "ICEBERG",
				"metadata_location": metadataLocation,
			},
			"StorageDescriptor": map[string]any{
				"Columns":      columns,
				"Location":     tableLocation,
				"InputFormat":  "org.apache.hadoop.mapred.FileInputFormat",
				"OutputFormat": "org.apache.hadoop.mapred.FileOutputFormat",
				"SerdeInfo":    map[string]any{},
			},
		},
	})
}
