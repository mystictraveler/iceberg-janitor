package aws

import (
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
)

func TestIcebergTypeToGlueScalars(t *testing.T) {
	cases := []struct {
		name string
		in   icebergpkg.Type
		want string
	}{
		{"bool", icebergpkg.BooleanType{}, "boolean"},
		{"int32", icebergpkg.Int32Type{}, "int"},
		{"int64", icebergpkg.Int64Type{}, "bigint"},
		{"float32", icebergpkg.Float32Type{}, "float"},
		{"float64", icebergpkg.Float64Type{}, "double"},
		{"string", icebergpkg.StringType{}, "string"},
		{"date", icebergpkg.DateType{}, "date"},
		{"timestamp", icebergpkg.TimestampType{}, "timestamp"},
		{"timestamptz", icebergpkg.TimestampTzType{}, "timestamp"},
		{"binary", icebergpkg.BinaryType{}, "binary"},
		{"uuid", icebergpkg.UUIDType{}, "string"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IcebergTypeToGlue(tc.in); got != tc.want {
				t.Errorf("IcebergTypeToGlue(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

func TestIcebergTypeToGlueDecimal(t *testing.T) {
	d := icebergpkg.DecimalTypeOf(18, 4)
	got := IcebergTypeToGlue(d)
	want := "decimal(18,4)"
	if got != want {
		t.Errorf("decimal(18,4): got %q, want %q", got, want)
	}
}

func TestIcebergTypeToGlueList(t *testing.T) {
	list := &icebergpkg.ListType{
		ElementID: 2,
		Element:   icebergpkg.StringType{},
	}
	got := IcebergTypeToGlue(list)
	want := "array<string>"
	if got != want {
		t.Errorf("list<string>: got %q, want %q", got, want)
	}
}

func TestIcebergTypeToGlueMap(t *testing.T) {
	m := &icebergpkg.MapType{
		KeyID:   2,
		KeyType: icebergpkg.StringType{},
		ValueID: 3,
		ValueType: icebergpkg.Int64Type{},
	}
	got := IcebergTypeToGlue(m)
	want := "map<string,bigint>"
	if got != want {
		t.Errorf("map<string,bigint>: got %q, want %q", got, want)
	}
}

func TestIcebergTypeToGlueStruct(t *testing.T) {
	s := &icebergpkg.StructType{
		FieldList: []icebergpkg.NestedField{
			{ID: 1, Name: "id", Type: icebergpkg.Int64Type{}},
			{ID: 2, Name: "name", Type: icebergpkg.StringType{}},
		},
	}
	got := IcebergTypeToGlue(s)
	want := "struct<id:bigint,name:string>"
	if got != want {
		t.Errorf("struct: got %q, want %q", got, want)
	}
}

func TestSchemaToGlueColumns(t *testing.T) {
	schema := icebergpkg.NewSchema(1,
		icebergpkg.NestedField{ID: 1, Name: "user_id", Type: icebergpkg.Int64Type{}, Required: true},
		icebergpkg.NestedField{ID: 2, Name: "event_time", Type: icebergpkg.TimestampType{}},
		icebergpkg.NestedField{ID: 3, Name: "tags", Type: &icebergpkg.ListType{ElementID: 4, Element: icebergpkg.StringType{}}},
	)
	cols := SchemaToGlueColumns(schema)
	if len(cols) != 3 {
		t.Fatalf("got %d cols, want 3", len(cols))
	}
	wants := []struct{ name, typ string }{
		{"user_id", "bigint"},
		{"event_time", "timestamp"},
		{"tags", "array<string>"},
	}
	for i, want := range wants {
		if cols[i]["Name"] != want.name {
			t.Errorf("col[%d].Name = %q, want %q", i, cols[i]["Name"], want.name)
		}
		if cols[i]["Type"] != want.typ {
			t.Errorf("col[%d].Type = %q, want %q", i, cols[i]["Type"], want.typ)
		}
	}
}

func TestSha256HashEmpty(t *testing.T) {
	// Empty body should produce the AWS-standard empty SHA-256.
	got := sha256Hash(nil)
	want := "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got != want {
		t.Errorf("sha256Hash(nil) = %q, want %q", got, want)
	}
	if got2 := sha256Hash([]byte{}); got2 != want {
		t.Errorf("sha256Hash([]byte{}) = %q, want %q", got2, want)
	}
}

func TestSha256HashKnownVector(t *testing.T) {
	// SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
	got := sha256Hash([]byte("abc"))
	want := "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Errorf("sha256Hash(\"abc\") = %q, want %q", got, want)
	}
}
