// Package observe — metrics companion to trace.go.
//
// # Why a metrics signal on top of traces
//
// Spans tell you "what happened on this one request"; metrics tell
// you "how often and how long across all requests." Compaction's
// failure modes (CAS retries, CB trips, mixed-schema skips) are
// frequency-domain signals — operators want counters and histograms
// they can chart and alert on, not individual span timelines.
//
// # Production posture (mirrors trace.go)
//
// Default meter provider is NoOp: counter.Add and histogram.Record
// are wait-free calls into a provider that discards the value. No
// exporter goroutine runs, no batch queue can fill up, no network
// I/O happens. Instrumentation code calls Counter.Add unconditionally
// without measurable CPU cost — the same enabling property that
// lets us instrument hot paths with spans.
//
// To turn metrics ON for debugging, set JANITOR_METRICS:
//
//	JANITOR_METRICS=stdout   compact JSON metric records to stderr
//	JANITOR_METRICS=otlp     reserved — OTLP wiring is intentionally
//	                         not compiled in (see below).
//
// Unset or empty means NoOp (production default).
//
// # Why no OTLP by default
//
// The OTLP exporter pulls in google.golang.org/grpc and its
// dependency fan-out (~30 MB of vendored code). We deliberately
// do not import it here to keep the binary small and the dep
// graph tight — consistent with the user's preference for zero-
// new-dep solutions (see feedback_deps_and_libs in the project
// memory). Operators who need OTLP export can wire it at deploy
// time by setting their own MeterProvider via
// otel.SetMeterProvider before calling janitor code; our metrics
// pick up whatever provider is globally registered.
//
// # Instruments
//
// The instruments are organized by maintenance op:
//
//	compact.attempts           counter, tags: table, outcome, skip_reason
//	compact.cb_trips           counter, tags: cb_id, table
//	compact.wall_ms            histogram, buckets sized for 100ms..60s
//	compact.cas_retries        histogram, buckets 1,2,3,5,10,25
//	compact.file_reduction     histogram, buckets 1,2,5,10,50,100,500,1000
//	master_check.wall_ms       histogram, buckets sized for a few hundred ms
//	expire.attempts            counter, tags: table, outcome
//	rewrite_manifests.attempts counter, tags: table, outcome
//
// Constructors are idempotent: the global `instruments` is lazy-
// initialized once per process. Instruments are retained as
// package-level vars so recording sites don't repeatedly call
// Int64Counter / Float64Histogram on the Meter.
package observe

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

// InitMetrics configures the global MeterProvider. Follows the same
// shape as Init in trace.go: NoOp by default, real exporter behind
// an env var. Returns a shutdown closer the caller invokes on exit.
//
// Operators who want OTLP export should register their own provider
// BEFORE calling InitMetrics (or skip InitMetrics entirely and rely
// on their deploy-time wiring).
func InitMetrics(serviceName string) func() {
	mode := os.Getenv("JANITOR_METRICS")
	if mode == "" {
		// Default: no explicit MeterProvider is set on otel.
		// otel.GetMeterProvider() returns the global noop provider
		// until something registers otherwise. Our instruments
		// record through otel.Meter(...) which resolves through
		// the global getter at call time, so they pick up any
		// late-bound provider the embedder installs.
		return func() {}
	}

	switch mode {
	case "stdout":
		exp, err := stdoutmetric.New()
		if err != nil {
			slog.Error("failed to create stdout metrics exporter", "err", err)
			return func() {}
		}
		reader := sdkmetric.NewPeriodicReader(exp,
			// A short export interval keeps the dev-loop feedback
			// tight (operator runs one compact, looks at stderr,
			// sees the metrics on the next tick). In a real
			// deployment the operator is expected to register
			// their own OTLP/Prometheus reader with deploy-sized
			// intervals; this path is debug-only.
			sdkmetric.WithInterval(10*time.Second),
		)
		mp := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithResource(resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String(serviceName),
			)),
		)
		otel.SetMeterProvider(mp)
		return func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = mp.Shutdown(ctx)
		}
	case "otlp":
		// Intentionally not compiled in — see the package doc for
		// the dep-surface rationale. Log so operators who set the
		// flag see why nothing's happening.
		slog.Warn("JANITOR_METRICS=otlp requested but OTLP exporter is not compiled in; register your own MeterProvider via otel.SetMeterProvider instead")
		return func() {}
	default:
		slog.Warn("unknown JANITOR_METRICS mode", "mode", mode)
		return func() {}
	}
}

// instruments holds every metric instrument the janitor emits. Lazy-
// initialized on first access via metricsOnce so the creation cost
// (a handful of Meter.Int64Counter / Float64Histogram calls) happens
// exactly once per process and is amortized over every recording
// site. Recording sites call the helper functions below rather than
// reaching into this struct directly.
type instruments struct {
	compactAttempts      metric.Int64Counter
	compactCBTrips       metric.Int64Counter
	compactWallMs        metric.Float64Histogram
	masterCheckWallMs    metric.Float64Histogram
	compactCASRetries    metric.Int64Histogram
	compactFileReduction metric.Float64Histogram
	expireAttempts       metric.Int64Counter
	rewriteAttempts      metric.Int64Counter
}

var (
	metricsOnce sync.Once
	inst        *instruments
)

// meter returns the package's lazily-initialized instrument set.
// Initialization is idempotent so concurrent first-callers share a
// single construction.
func meter() *instruments {
	metricsOnce.Do(func() {
		m := otel.Meter("iceberg-janitor")
		i := &instruments{}

		compactAttempts, _ := m.Int64Counter(
			"janitor.compact.attempts",
			metric.WithDescription("Compaction attempts labeled by outcome and optional skip_reason"),
		)
		i.compactAttempts = compactAttempts

		cbTrips, _ := m.Int64Counter(
			"janitor.compact.cb_trips",
			metric.WithDescription("Circuit-breaker trip events, labeled by cb_id (CB2..CB11) and table"),
		)
		i.compactCBTrips = cbTrips

		// Compaction wall time histogram. Buckets tuned for the
		// observed range in production: fast compacts finish in
		// ~100 ms; the slowest (partition-scoped, heavy writer
		// contention, full CAS budget) are tens of seconds.
		wallMs, _ := m.Float64Histogram(
			"janitor.compact.wall_ms",
			metric.WithDescription("Wall time per Compact call, in milliseconds"),
			metric.WithExplicitBucketBoundaries(
				50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 120000,
			),
		)
		i.compactWallMs = wallMs

		// Master check wall time — component of compact.wall_ms.
		// Dashboard separates this so operators can see whether the
		// mandatory safety pass dominates the retry wait.
		mcWall, _ := m.Float64Histogram(
			"janitor.master_check.wall_ms",
			metric.WithDescription("Wall time spent in the master check per Compact call, in milliseconds"),
			metric.WithExplicitBucketBoundaries(5, 10, 25, 50, 100, 250, 500, 1000),
		)
		i.masterCheckWallMs = mcWall

		// CAS retries histogram. Buckets are integer attempt counts,
		// so we record via Int64Histogram. 1 = first-try success
		// (the usual case). Buckets above 10 signal a writer-fight
		// pathology operators should investigate.
		retries, _ := m.Int64Histogram(
			"janitor.compact.cas_retries",
			metric.WithDescription("Total CAS attempts per Compact call (1 = first-try success)"),
			metric.WithExplicitBucketBoundaries(1, 2, 3, 5, 10, 25),
		)
		i.compactCASRetries = retries

		// File reduction ratio. Ratio = before_files / after_files.
		// A ratio of 1 means compaction was a no-op; higher is
		// better. Buckets span the typical range from ineffective
		// (1-2x) to the Run 20 192x result.
		reduction, _ := m.Float64Histogram(
			"janitor.compact.file_reduction_ratio",
			metric.WithDescription("File reduction ratio per Compact call, before/after"),
			metric.WithExplicitBucketBoundaries(1, 2, 5, 10, 50, 100, 500, 1000),
		)
		i.compactFileReduction = reduction

		expireAttempts, _ := m.Int64Counter(
			"janitor.expire.attempts",
			metric.WithDescription("Expire attempts labeled by outcome"),
		)
		i.expireAttempts = expireAttempts

		rewriteAttempts, _ := m.Int64Counter(
			"janitor.rewrite_manifests.attempts",
			metric.WithDescription("RewriteManifests attempts labeled by outcome"),
		)
		i.rewriteAttempts = rewriteAttempts

		inst = i
	})
	return inst
}

// RecordCompactOutcome emits the counter + histograms for one
// completed Compact call. outcome is "pass" / "fail" / "skip".
// skipReason is non-empty only when outcome == "skip". beforeFiles /
// afterFiles drive the reduction histogram; zero afterFiles is
// treated as 1 to avoid infinite ratios on fully-collapsed
// partitions.
//
// Safe to call with zero values — every argument is an atomic int
// or string so the call is non-blocking. NoOp provider makes the
// recording cost effectively zero in production.
func RecordCompactOutcome(ctx context.Context, table, outcome, skipReason string, wallMs int64, attempts int, beforeFiles, afterFiles int) {
	i := meter()
	attrs := []attribute.KeyValue{
		attribute.String("table", table),
		attribute.String("outcome", outcome),
	}
	if skipReason != "" {
		attrs = append(attrs, attribute.String("skip_reason", skipReason))
	}
	i.compactAttempts.Add(ctx, 1, metric.WithAttributes(attrs...))
	i.compactWallMs.Record(ctx, float64(wallMs), metric.WithAttributes(
		attribute.String("table", table),
		attribute.String("outcome", outcome),
	))
	if attempts > 0 {
		i.compactCASRetries.Record(ctx, int64(attempts), metric.WithAttributes(
			attribute.String("table", table),
		))
	}
	if beforeFiles > 0 {
		after := afterFiles
		if after <= 0 {
			after = 1
		}
		ratio := float64(beforeFiles) / float64(after)
		i.compactFileReduction.Record(ctx, ratio, metric.WithAttributes(
			attribute.String("table", table),
		))
	}
}

// RecordMasterCheckWall is called by the master-check emitter inside
// executeStitchAndCommit. Split from RecordCompactOutcome so the
// dashboard can chart the master check's wall time separately from
// the rest of the compact.
func RecordMasterCheckWall(ctx context.Context, table string, wallMs int64) {
	meter().masterCheckWallMs.Record(ctx, float64(wallMs), metric.WithAttributes(
		attribute.String("table", table),
	))
}

// RecordCBTrip emits one cb_trips counter bump. cbID is one of
// "CB2".."CB11" — the set enumerated in pkg/safety/circuitbreaker.go's
// top-level doc comment. table is the fully-qualified iceberg table
// name when available.
func RecordCBTrip(ctx context.Context, cbID, table string) {
	meter().compactCBTrips.Add(ctx, 1, metric.WithAttributes(
		attribute.String("cb_id", cbID),
		attribute.String("table", table),
	))
}

// RecordExpireOutcome increments the expire counter for one completed
// Expire call. outcome is "pass" / "fail". Mirrors the minimum
// metric surface we give compact (expire's shape is simpler — no
// file reduction, no CAS histogram yet).
func RecordExpireOutcome(ctx context.Context, table, outcome string) {
	meter().expireAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", table),
		attribute.String("outcome", outcome),
	))
}

// RecordRewriteManifestsOutcome mirrors RecordExpireOutcome for the
// rewrite-manifests op.
func RecordRewriteManifestsOutcome(ctx context.Context, table, outcome string) {
	meter().rewriteAttempts.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", table),
		attribute.String("outcome", outcome),
	))
}
