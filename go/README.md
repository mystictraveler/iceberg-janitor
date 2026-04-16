# iceberg-janitor (Go)

Catalog-less, multi-cloud, serverless Iceberg table maintenance.

This is a sibling Go implementation of the Python `iceberg-janitor` in this repo. It targets sub-200ms cold starts on Knative scale-to-zero and AWS Lambda by going directly against object storage (S3, MinIO, Azure Blob, GCS) — no Iceberg REST Catalog, no Glue, no metastore.

See `/Users/jp/.claude/plans/async-plotting-cake.md` for the design and phased delivery plan.

## Layout

```
pkg/
  blob/          multi-cloud object store abstraction (gocloud.dev/blob)
  fileio/        Iceberg FileIO bridging blob -> apache/iceberg-go
  catalog/       FileIO catalog: discovery + atomic metadata commit
  iceberg/       wrapper over apache/iceberg-go for the reads we use
  analyzer/      HealthReport (port of Python analyzer)
  policy/        TablePolicy + evaluation engine
  strategy/      triggers, scheduler, access tracker, feedback loop
  maintenance/   compact, expire snapshots, remove orphans, rewrite manifests
  orchestrator/  pure-function state machine
  janitor/       top-level core: ProcessTable(ctx, table)
  state/         persistent janitor state in object storage
cmd/
  janitor-cli/    local CLI (analyze / maintain / inspect)
  janitor-server/ HTTP server — one container image, portable to
                  Fargate / EKS / Cloud Run / Knative / Lambda (via
                  AWS Lambda Web Adapter). Orchestrator choice is a
                  deployment decision, not a binary decision.
internal/
  metrics/  Prometheus exporter
  log/      slog setup
test/
  integration/  end-to-end tests against MinIO / Azurite / fake-gcs-server
  benchmark/    parity benchmark vs the Python implementation
```

## Status

**22/30 design decisions fully shipped, 3 partially shipped, 5 pending.** The core maintenance pipeline (compact + expire + rewrite-manifests + all 11 circuit breakers) is proven end-to-end with bench evidence on bursty streaming workloads. The server's `/maintain` endpoint is now **zero-knob**: every call auto-classifies the table and dispatches to one of three per-partition modes — **hot** (delta stitch with time-based round-robin anchor), **cold** (trigger-based full compaction, one partition at a time), or **full** (legacy all-partition parallel, retained as an explicit override). Per-partition state is persisted under `_janitor/state/<table_uuid>/partitions.json`. AWS deployment on ECS Fargate is operational with a single-script bench harness (`test/bench/bench.sh`) that runs identically in `local` / `minio` / `aws` modes.

**[Executive Summary](EXECUTIVE_SUMMARY.md)** — one-page overview: what iceberg-janitor is, what it provides, architectural principles, bench evidence, deployment, and how it compares to Spark `rewriteDataFiles`, AWS Glue auto-compaction, and Confluent Tableflow.

**[Full project scorecard](https://gist.github.com/mystictraveler/1c075afc793e3507ada484f3153cdf27)** — status of every design decision, bench results, architecture overview.

**Measured benchmark results live at [BENCHMARKS.md](BENCHMARKS.md)** and are updated every time a new build phase lands.

**For an architectural comparison against [Confluent Tableflow](https://www.confluent.io/product/tableflow/)'s compaction subsystem**, see [TABLEFLOW_COMPARISON.md](TABLEFLOW_COMPARISON.md). TL;DR: same correctness story with stronger guarantees (mandatory master check, snapshot-internal audit), no managed control plane, multi-cloud by construction, zero idle cost, and an open-source compaction algorithm you can audit in an afternoon.

## Workload classification and orchestration

`pkg/strategy/classify` auto-detects each table's workload class from its
foreign-commit rate over the last 24 hours:

- **streaming** — high commit rate; needs frequent small-file compaction
- **batch** — periodic large commits; benefits from manifest consolidation
- **slow_changing** — sporadic edits; long quiet periods
- **dormant** — no recent commits; touch only for safety

The class is exposed via `GET /v1/tables/{ns}/{name}/health` (and the
analyzer's `HealthReport.Workload` field). **It is now consumed by the
maintenance pipeline.** On every call to `POST /v1/tables/{ns}/{name}/
maintain` the server re-classifies the table, maps the class to a
`MaintainOptions` struct (`pkg/strategy/classify/options.go`), and
dispatches to one of three modes: **hot** (delta stitch for streaming),
**cold** (per-partition full compact when a trigger fires, for
batch / slow_changing / dormant), or **full** (legacy all-partition
parallel, retained as an explicit override). The caller passes no
compaction options; the server decides.

**An external orchestrator is still required** to decide *when* to call
`maintain` for each table — the server is a stateless worker, not a
scheduler. Recommended cadences:

| class | maintain cadence |
|---|---|
| streaming | every 5 min |
| batch | every 1 hr |
| slow_changing | daily |
| dormant | weekly |

The orchestrator is intentionally outside the janitor-server: the server
is a stateless, request-response maintenance worker, and scheduling
belongs to a higher layer (a cron, an EventBridge rule, a Knative
PingSource, a Lambda Function URL on a schedule, or a custom
controller). This separation keeps the server cold-start fast and
its memory model trivial.

## MVP test loop

See [test/mvp/MVP.md](test/mvp/MVP.md) for the runbook. TL;DR:

```bash
make mvp-seed-local MVP_NUM_BATCHES=20 MVP_ROWS_PER_BATCH=5000
make mvp-discover-local
make mvp-analyze-local
cd go && JANITOR_WAREHOUSE_URL=file:///tmp/janitor-mvp go run ./cmd/janitor-cli compact mvp.db/events
make mvp-analyze-local
make mvp-query-local       # DuckDB round-trip verification
```

For the same loop against MinIO over docker compose, replace `-local` with the docker variants (`mvp-up`, `mvp-seed`, `mvp-discover`, `mvp-analyze`).

## Schema evolution and compaction

Iceberg tables can evolve their schema over time — columns added, renamed, widened, or dropped. Each schema version is recorded in the table metadata, and each data file is implicitly associated with the schema that was current when it was written. Compaction must handle files written under different schema versions within the same partition.

### How iceberg-janitor handles schema evolution today

**Master check I2 (schema identity)** prevents the compactor from silently changing the table's current schema. If a foreign writer evolves the schema between the compactor's load and commit, the CAS requirement validation fails and the compactor retries against the new schema.

**The stitch path (byte-copy)** preserves each source file as a separate row group in the output. Files written under different schemas produce an output file with heterogeneous row groups — some row groups may have fewer columns than others. This is valid Parquet (each row group has its own column set) but readers must handle missing columns per-row-group. No data is decoded or re-encoded, so column data is preserved byte-identically.

**The pqarrow fallback path** normalizes everything to the table's current Arrow schema. Missing columns in older files are null-filled during the Arrow decode. The output file has uniform columns across all row groups. This is the correct behavior but introduces nulls that weren't in the original data (the I4 null-count check still passes because the nulls are real in the output).

### Planned improvement: schema-version grouping

The right long-term approach — and what Spark's `rewriteDataFiles` does with `partial-progress.enabled` — is to group files by schema version during `CompactTable`'s partition discovery:

1. **Same-schema files** → stitch (byte-copy, no decode, fast)
2. **Older-schema files** → upgrade via pqarrow (decode under old schema, re-encode under current schema with null-filled new columns)
3. **Never mix schemas in a single stitch** — the output file should have uniform columns across all row groups

This ensures:
- The stitch fast path fires for the common case (uniform schema within a partition)
- Schema-evolved files get a one-time upgrade to the current schema (slower, but only happens once per file per schema evolution)
- The output is always a well-formed Parquet file with a single schema, which is what every downstream reader expects

### What this means for operators

- **No action needed for tables that don't evolve.** The stitch path handles uniform-schema tables at full speed.
- **For tables that evolve infrequently** (e.g. a column added once a quarter): the first compact after a schema evolution will be slightly slower on partitions that contain files from both schema versions. Subsequent compacts on those partitions are fast (all files now under the new schema).
- **For tables that evolve frequently**: consider running compact with `--compact` after each schema evolution to upgrade all files, rather than waiting for the natural maintenance cycle to encounter the mixed-schema case.
