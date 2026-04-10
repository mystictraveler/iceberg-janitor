// Package observe provides OpenTelemetry tracing backed by slog.
// Spans are exported as structured JSON log lines to stderr (→ CloudWatch),
// carrying trace_id, span_id, parent_span_id, duration_ms, and attributes.
// Zero external infrastructure required.
package observe

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

// Init sets up the global TracerProvider with a slog-friendly exporter.
// Call the returned function on shutdown.
func Init(serviceName string) func() {
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		slog.Error("failed to create trace exporter", "err", err)
		return func() {}
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
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
