// Package observe provides OpenTelemetry tracing backed by slog.
// Spans are exported as structured JSON log lines to stderr (→ CloudWatch),
// carrying trace_id, span_id, parent_span_id, duration_ms, and attributes.
// Zero external infrastructure required.
//
// # Production note: trace export is OFF by default
//
// The default Init() returns a NoOp tracer provider — no spans are
// emitted, no exporter goroutine runs. This is the right choice for
// production: under heavy parallel work the synchronous span export
// path saturates the BatchSpanProcessor's queue (default 2048) and
// blocks the producers. The MinIO Run 19 bench observed this exact
// pathology — parallel CompactHot's 19,000+ span fan-out exhausted
// the queue and the workers spent 95%+ of wall time waiting on
// trace export instead of doing real stitch work. With NoOp, the
// same workload runs at full CPU.
//
// To enable tracing for debugging, set the environment variable
// JANITOR_TRACE=stdout (pretty-printed) or JANITOR_TRACE=stdout-compact
// (no pretty-print, smaller output) at server startup.
package observe

import (
	"context"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Init sets up the global TracerProvider. By default the exporter
// is a NoOp — see the package docs for the rationale. Set
// JANITOR_TRACE to enable a real exporter:
//
//	JANITOR_TRACE=stdout         pretty-printed JSON spans (debug only)
//	JANITOR_TRACE=stdout-compact compact JSON spans (smaller output)
//
// Returns a shutdown closer the caller invokes on process exit.
func Init(serviceName string) func() {
	mode := os.Getenv("JANITOR_TRACE")
	if mode == "" {
		// NoOp tracer: spans created via the global provider are
		// no-op, no exporter goroutine runs, no queue to fill.
		// This is the production default.
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func() {}
	}

	var opts []stdouttrace.Option
	if mode == "stdout" {
		opts = append(opts, stdouttrace.WithPrettyPrint())
	}
	exp, err := stdouttrace.New(opts...)
	if err != nil {
		slog.Error("failed to create trace exporter", "err", err)
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func() {}
	}

	// Use a large batch processor queue + frequent flush to keep
	// the producer from blocking under parallel load. With the
	// NoOp default this code path is debug-only, but operators
	// who turn tracing on for an investigation get a higher-
	// throughput configuration than the SDK default.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp,
			sdktrace.WithMaxQueueSize(65536),
			sdktrace.WithMaxExportBatchSize(8192),
			sdktrace.WithBatchTimeout(2*time.Second),
		),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)
	otel.SetTracerProvider(tp)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tp.Shutdown(ctx)
	}
}

// Tracer returns a named tracer from the global provider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// Common attribute helpers for the janitor domain.
func Table(ns, name string) attribute.KeyValue {
	return attribute.String("iceberg.table", ns+"."+name)
}

func JobID(id string) attribute.KeyValue {
	return attribute.String("job.id", id)
}

func Files(n int) attribute.KeyValue {
	return attribute.Int("iceberg.files", n)
}

func Bytes(n int64) attribute.KeyValue {
	return attribute.Int64("iceberg.bytes", n)
}

func Rows(n int64) attribute.KeyValue {
	return attribute.Int64("iceberg.rows", n)
}

func Attempt(n int) attribute.KeyValue {
	return attribute.Int("compact.attempt", n)
}

func Phase(name string) attribute.KeyValue {
	return attribute.String("compact.phase", name)
}

func Manifests(n int) attribute.KeyValue {
	return attribute.Int("iceberg.manifests", n)
}

func DurationMs(ms int64) attribute.KeyValue {
	return attribute.Int64("duration_ms", ms)
}
