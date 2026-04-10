package aws

import (
	"fmt"
	"strings"

	icebergpkg "github.com/apache/iceberg-go"
)

// IcebergTypeToGlue maps an Iceberg type to the corresponding
// Glue/Athena type string.
func IcebergTypeToGlue(t icebergpkg.Type) string {
	switch t := t.(type) {
	case icebergpkg.BooleanType:
		return "boolean"
	case icebergpkg.Int32Type:
		return "int"
	case icebergpkg.Int64Type:
		return "bigint"
	case icebergpkg.Float32Type:
		return "float"
	case icebergpkg.Float64Type:
		return "double"
	case icebergpkg.StringType:
		return "string"
	case icebergpkg.DateType:
		return "date"
	case icebergpkg.TimestampType:
		return "timestamp"
	case icebergpkg.TimestampTzType:
		return "timestamp"
	case icebergpkg.BinaryType:
		return "binary"
	case icebergpkg.UUIDType:
		return "string"
	case icebergpkg.FixedType:
		return "binary"
	case icebergpkg.DecimalType:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision(), t.Scale())
	case *icebergpkg.ListType:
		return fmt.Sprintf("array<%s>", IcebergTypeToGlue(t.ElementField().Type))
	case *icebergpkg.MapType:
		return fmt.Sprintf("map<%s,%s>", IcebergTypeToGlue(t.KeyField().Type), IcebergTypeToGlue(t.ValueField().Type))
	case *icebergpkg.StructType:
		var parts []string
		for _, f := range t.FieldList {
			parts = append(parts, fmt.Sprintf("%s:%s", f.Name, IcebergTypeToGlue(f.Type)))
		}
		return fmt.Sprintf("struct<%s>", strings.Join(parts, ","))
	default:
		return "string"
	}
}

// SchemaToGlueColumns converts an iceberg-go Schema into a slice of
// Glue column definitions (Name/Type maps) suitable for the Glue
// CreateTable API.
func SchemaToGlueColumns(schema *icebergpkg.Schema) []map[string]string {
	var cols []map[string]string
	for _, field := range schema.Fields() {
		cols = append(cols, map[string]string{
			"Name": field.Name,
			"Type": IcebergTypeToGlue(field.Type),
		})
	}
	return cols
}
