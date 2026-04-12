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
// propagating a janitor commit to Athena — two API calls
// (GetTable + UpdateTable), milliseconds, no S3 discovery.
//
// The Glue UpdateTable API REPLACES the entire TableInput, not
// merges it. If we send only Parameters, the StorageDescriptor
// (columns, location, serde) gets wiped and Athena returns
// "StorageDescriptor.getColumns() is null". The fix: GetTable
// first, patch the metadata_location in the existing Parameters,
// and send the full TableInput back.
func (g *GlueRegistrar) UpdateMetadataLocation(ctx context.Context, database, tableName, metadataLocation string) error {
	// GetTable to read the existing table definition.
	existing, err := CallAWSJSONResult(ctx, g.creds, g.endpoint, "glue", g.region, "AWSGlue.GetTable", map[string]any{
		"DatabaseName": database,
		"Name":         tableName,
	})
	if err != nil {
		return fmt.Errorf("GetTable %s.%s: %w", database, tableName, err)
	}

	// Extract the Table object and patch metadata_location.
	table, ok := existing["Table"].(map[string]any)
	if !ok {
		return fmt.Errorf("GetTable %s.%s: unexpected response shape", database, tableName)
	}

	// Build TableInput from the existing table, preserving
	// StorageDescriptor, columns, and all other fields.
	params, _ := table["Parameters"].(map[string]any)
	if params == nil {
		params = map[string]any{}
	}
	params["table_type"] = "ICEBERG"
	params["metadata_location"] = metadataLocation

	tableInput := map[string]any{
		"Name":       tableName,
		"Parameters": params,
	}
	if sd := table["StorageDescriptor"]; sd != nil {
		tableInput["StorageDescriptor"] = sd
	}
	if tt := table["TableType"]; tt != nil {
		tableInput["TableType"] = tt
	}

	return CallAWSJSON(ctx, g.creds, g.endpoint, "glue", g.region, "AWSGlue.UpdateTable", map[string]any{
		"DatabaseName": database,
		"TableInput":   tableInput,
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
