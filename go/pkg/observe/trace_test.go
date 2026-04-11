package observe

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func TestInitAndShutdown(t *testing.T) {
	shutdown := Init("test-service")
	if shutdown == nil {
		t.Fatal("Init returned nil shutdown func")
	}
	// Calling shutdown should not panic or error.
	shutdown()
}

func TestTracerReturnsNonNil(t *testing.T) {
	tr := Tracer("test-tracer")
	if tr == nil {
		t.Fatal("Tracer returned nil")
	}
}

func TestAttributeHelpers(t *testing.T) {
	cases := []struct {
		name string
		kv   attribute.KeyValue
		key  string
		val  any
	}{
		{"table", Table("tpcds", "store_sales"), "iceberg.table", "tpcds.store_sales"},
		{"job_id", JobID("abc-123"), "job.id", "abc-123"},
		{"files", Files(42), "iceberg.files", int64(42)},
		{"bytes", Bytes(1024), "iceberg.bytes", int64(1024)},
		{"rows", Rows(1_000_000), "iceberg.rows", int64(1_000_000)},
		{"attempt", Attempt(3), "compact.attempt", int64(3)},
		{"phase", Phase("stitch"), "compact.phase", "stitch"},
		{"manifests", Manifests(128), "iceberg.manifests", int64(128)},
		{"duration_ms", DurationMs(250), "duration_ms", int64(250)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if string(tc.kv.Key) != tc.key {
				t.Errorf("Key = %q, want %q", string(tc.kv.Key), tc.key)
			}
			// attribute.Value has different accessors per type.
			switch want := tc.val.(type) {
			case string:
				if got := tc.kv.Value.AsString(); got != want {
					t.Errorf("Value = %q, want %q", got, want)
				}
			case int64:
				if got := tc.kv.Value.AsInt64(); got != want {
					t.Errorf("Value = %d, want %d", got, want)
				}
			}
		})
	}
}
