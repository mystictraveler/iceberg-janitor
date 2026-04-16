# iceberg-janitor — Observability Spec

The design contract for the observability track (OpenTelemetry
tracing + structured logs + metrics + pprof profiling). Every PR in
the track is gated on the rule in [Hot-path overhead](#hot-path-overhead)
below: **observability must not regress the MinIO TPC-DS A/B bench by
more than 1%.**

---

## Scope

1. **Tracing** — OpenTelemetry spans covering every public entry
   point in `pkg/janitor`, `pkg/maintenance`, `pkg/safety`,
   `pkg/catalog`, `pkg/lease`, `pkg/jobrecord`, `pkg/analyzer`, plus
   the HTTP handler mux in `cmd/janitor-server` and the Lambda
   handler in `cmd/janitor-lambda`.
2. **Structured logs** — slog-based, JSON output, stable field names
   the CloudWatch dashboard (and future Grafana/Loki setups) can
   query. Every feature that ships must extend the compact/expire/
   rewrite/maintain completion log lines with the new fields it
   produces; dashboard widgets consume the same names.
3. **Metrics** — OTel metrics (counters + histograms) alongside the
   traces. Counters for attempts / failures / skips by reason;
   histograms for wall times, CAS retries, file-reduction ratios.
4. **pprof** — `net/http/pprof` behind a `--debug-addr` flag on
   `janitor-server`; one-shot profile captures for SIGQUIT-like
   investigations without process kill.
5. **Continuous profiling** (optional, later phase) — Pyroscope /
   Parca agent scraping the pprof endpoint.

---

## Hot-path overhead

**Rule: The observability layer must not add measurable overhead to
the main path of maintenance ops.**

Concrete budget: with the OTel SDK configured but NO exporter
attached (the default production posture — exports flow to an
external collector only when the deployment wires one up), the
MinIO TPC-DS A/B bench's wall time on the with-janitor side must
remain within **±1%** of the pre-observability baseline.

### What that requires

- **Span creation is already near-free when nothing consumes them.**
  OTel's default noop tracer returns a span that discards attributes
  and events. Instrumentation code can call `tr.Start(...)` and
  `span.SetAttributes(...)` unconditionally without measurable CPU
  cost. This is the *enabling* property — we don't have to gate span
  creation behind a flag.
- **Do NOT compute expensive attribute values unconditionally.** If
  an attribute requires walking a slice, formatting a big struct, or
  allocating, gate it with `if span.IsRecording()` so it's skipped
  when the sampler decided not to record this span:
  ```go
  if span.IsRecording() {
      span.SetAttributes(attribute.Int("files.expensive_count", countSlow()))
  }
  ```
  Attributes whose values are already in hand (ints, short strings
  from the CompactResult) are free to set unconditionally.
- **Do NOT synchronously serialize logs in the hot loop.** slog's
  JSON handler is efficient but still allocates. In a tight
  per-file worker, emit slog calls at the batch boundary (once per
  partition or per source-file group), not per row.
- **Metrics must be async (OTel observable instruments) or batched.**
  Using `counter.Add(ctx, 1)` in a tight loop is fine because the
  counter is wait-free atomic; but don't emit per-row metrics — emit
  per-file or per-compact.
- **No new synchronous I/O in the hot path.** Observability must not
  introduce network calls on the CAS-commit or stitch path. Exports
  go through the OTel Collector via batched, async OTLP — a slow
  collector does not stall the janitor.

### Enforcement

Every observability PR:

1. **Bench gate**: runs `bash go/test/bench/bench.sh minio`
   (default `WORKLOAD=tpcds`) on the branch, and compares
   maintain-round wall time + query-parity results vs `main`. ±1%
   wall-time drift is acceptable; more requires explanation.
2. **Microbenchmark where relevant**: if the PR touches a hot
   loop (stitch, manifest walk, per-file worker), add a Go
   benchmark against a local fileblob fixture and show
   before/after.
3. **PR body** lists the observability-overhead check as a
   separate checkbox from the existing MinIO TPC-DS gate, with
   numbers.

Violation = regression. If an observability PR makes the janitor
slower by more than 1% on the bench, it doesn't merge until the
overhead source is isolated and either fixed or deferred.

---

## Phasing

| Phase | Scope | Risk | Bench gate |
|---|---|---|---|
| 1 | Attributes + `span.RecordError` on the 11 existing spans; log-enrichment on compact-completed (already merged as part of the observability-track branch) | Very low — zero new code paths, just annotations on existing ones | ±0.3% expected |
| 2 | Span coverage for `pkg/maintenance` + `pkg/safety` + `pkg/catalog` | Low — new spans at call boundaries, no hot-loop instrumentation | ±0.5% expected |
| 3 | HTTP + Lambda handler wrapping (`otelhttp.NewHandler`, `otellambda`) | Low — request-level, not per-op | ±0.2% expected |
| 4 | Hot-path spans inside `stitch.go` + the per-source-file worker in `executeStitchAndCommit` | Medium — these are tight loops; use `span.IsRecording()` gating aggressively | ±0.8% expected; the one phase most likely to hit the 1% ceiling |
| 5 | Metrics signal (OTel counters/histograms) alongside traces | Low — async + wait-free | ±0.2% expected |
| 6 | `net/http/pprof` behind `--debug-addr` + docker-compose overlay for Jaeger/Tempo/Pyroscope dev stack | Zero — opt-in, disabled by default | 0% |

Each phase is its own PR. Each PR carries its own bench rows in the
body. The Phase 4 bench is the most important — it's where the
1% ceiling is actually load-bearing.

---

## Stable field names (contract)

The CloudWatch dashboard (and future consumers — Prometheus metric
filters, Grafana Loki queries, OpenSearch alerts) queries these
fields by name. Once a field ships it becomes part of the wire
contract: you can APPEND fields, but you cannot rename or remove
without coordinating with every consumer.

Emitted today (compact-completed):

- `job_id`, `table`, `elapsed_ms`
- `before_files`, `after_files`, `before_rows`, `after_rows`, `before_bytes`, `after_bytes`
- `attempts`
- `skipped` (bool), `skipped_reason` (string)
- `master_overall`, `master_I1`, `master_I2`, `master_I3`, `master_I4`, `master_I5`, `master_I7`
- `dvs_applied`

Reserved for later phases (DO NOT reuse these names for other
purposes — the dashboard already queries them and will populate
once the emission lands):

- `cb_tripped` (string: CB2..CB11), `cb_reason` (string)
- `eq_delete_refused` (bool), `eq_delete_file` (string), `eq_delete_column_type` (string)
- `stitch_path` (enum: `byte_copy` | `pqarrow_fallback`)
- `master_check_ms` (int — wall time for the master check phase alone)
- `cas_retry_count` (alias for `attempts - 1`)

---

## How to verify overhead locally

```
# On main
git checkout main
bash go/test/bench/bench.sh minio
cp bench-results/bench-results-<ts>.csv /tmp/baseline.csv

# On your observability branch
git checkout feature/observability-track
bash go/test/bench/bench.sh minio
diff /tmp/baseline.csv bench-results/bench-results-<ts>.csv
```

The CSV emits per-iteration query times and a header line with the
file-reduction outcome. Eyeball or `awk` the wall-time columns; if
any row regresses >1% consistently across iterations, the gate
fails.
