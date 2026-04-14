# iceberg-janitor

Catalog-less, multi-cloud maintenance for Apache Iceberg tables. Drop it onto any S3, MinIO, GCS, or Azure Blob warehouse and it maintains every table it finds: compaction, snapshot expiration, manifest consolidation, with mandatory pre-commit verification and zero operator configuration in normal operation.

## What it does

Streaming engines (Spark Structured Streaming, Kafka Connect, custom writers) produce thousands of small files per table per hour. Left unattended, these accumulate into a metadata and data-file sprawl that degrades query performance, inflates storage costs, and eventually breaks query planners that can't handle 50K+ manifest entries.

iceberg-janitor fixes this automatically:

- **Compact** small data files into target-sized files via byte-copy stitching (no Arrow decode/encode, no Spark, no Flink)
- **Expire** old snapshots from the parent chain, freeing orphaned metadata
- **Rewrite manifests** from per-commit micro-manifests into partition-organized layout
- **Classify** each table's workload (streaming / batch / slow-changing / dormant) and dispatch the right maintenance mode automatically
- **Master check** every commit against invariants I1-I9 (row count, schema, column stats, manifest refs) before writing metadata.json

No catalog service required. No managed control plane. No per-GB pricing.

## Architecture

```
                    Iceberg warehouse on object storage
                    (S3, MinIO, GCS, Azure Blob, local file://)

                    <warehouse>/tpcds.db/store_sales/
                    +-- data/        parquet files (written by any producer)
                    +-- metadata/    metadata.json + manifest-list + manifests
                    +-- _janitor/    lease files, job records, partition state
                        +-- state/leases/<ns>.<table>/<op>.lease
                        +-- state/jobs/<job_id>.json
                        +-- state/<table_uuid>/partitions.json
                                |
                                | gocloud.dev/blob (s3, gs, azblob, file)
                                |
            +-------------------+--------------------+
            |                                        |
    janitor-server (ECS/Knative)          janitor-cli (operator tool)
    POST /v1/tables/{ns}/{name}/maintain  janitor-cli compact <warehouse-url>
    GET  /v1/jobs/{id}                    janitor-cli expire <warehouse-url>
    |                                     janitor-cli analyze <warehouse-url>
    | auto-classifies table on every call
    | dispatches: hot / cold / full
    |
    pkg/janitor       compaction (byte-copy stitch + fallback pqarrow)
    pkg/maintenance   expire + manifest rewrite
    pkg/safety        master check (I1-I9) + circuit breaker (CB8)
    pkg/lease         S3-backed cross-replica lock (If-None-Match CAS)
    pkg/jobrecord     persistent async job records on warehouse bucket
    pkg/analyzer      per-partition health assessment
    pkg/strategy      workload classifier (streaming/batch/dormant)
    pkg/catalog       directory catalog (LoadTable + atomic CommitTable)
```

### Key design choices

- **No catalog service.** The directory catalog reads `metadata/` directly from object storage and commits atomically via conditional write (`If-None-Match: *` on S3, `IfNotExist` on GCS/Azure). Works with any Iceberg table regardless of how it was created.
- **Two-phase compaction: byte-copy stitch + automatic row group merge.** Phase 1 copies parquet column chunks byte-for-byte between files (zero decode, zero CPU per row). Phase 2 automatically merges row groups when the stitched output has >4 per file — re-reads via pqarrow and rewrites with 1 merged row group, fresh column statistics. Result: query-optimal output (42% faster on Athena) without always paying the decode/encode cost. No other tool does this.
- **Single-snapshot batched commit.** CompactHot stitches N partitions in parallel (PartitionConcurrency=16), then commits ALL replacements in one transaction with one CAS write. The table gains exactly one snapshot per CompactHot call, not N.
- **Cross-replica safety.** The lease primitive + persistent job records let multiple server replicas coexist without duplicate work. Concurrent maintain requests for the same table return the existing job's ID (HTTP 202) instead of spawning a duplicate.
- **Mandatory master check.** Every CAS commit goes through `safety.VerifyCompactionConsistency` (compact) or `safety.VerifyExpireConsistency` (expire). Non-bypassable. Failures are recorded in the job result.
- **Dry-run mode.** Every maintenance endpoint accepts `?dry_run=true`. The server runs the full planning phase (manifest walk, staging, master check), then stops before any side effects. The result reports projected counts plus a `contention_detected` flag computed by reloading the table and comparing snapshot IDs.

## Workload classification and compaction behavior

Every `POST /v1/tables/{ns}/{name}/maintain` call reads the table's
snapshot history, classifies the workload, and dispatches the right
maintenance pipeline — no operator configuration needed. The
classifier is in [`pkg/strategy/classify`](go/pkg/strategy/classify).

### The four classes

Class is a function of commit rate, computed from the table's snapshot
history (`foreign_commits_last_15m`, `foreign_commits_last_24h`,
`foreign_commits_last_7d`). Janitor-authored commits are excluded so
compaction itself doesn't influence classification.

| Class | When | Intuition |
|---|---|---|
| **streaming** | 15-min rate > 6/h OR 24h rate > 6/h | Kafka → Flink/Spark streaming job; micro-batches every few seconds to minutes |
| **batch** | 0.04/h < 24h rate ≤ 6/h | Scheduled ETL; hourly / daily batch loads |
| **slow_changing** | 0 < 24h rate ≤ 0.04/h | Dimension-like tables; updates occasional |
| **dormant** | no commits in last 7d | Historical data; rarely touched |

The 15-minute short-window fast path means a fresh streaming table
classifies correctly within minutes of its first burst — no waiting
hours for a rolling 24h average to catch up.

### Per-class compaction plan

| Class | Mode | Target file size | Keep snapshots | Keep within | Stale-rewrite age |
|---|---|---:|---:|---|---|
| streaming | **hot** | 64 MiB | 5 | 1 hour | 30 min |
| batch | **cold** | 128 MiB | 10 | 24 hours | 6 hours |
| slow_changing | **cold** | 256 MiB | 20 | 7 days | 7 days |
| dormant | **cold** | 512 MiB | 50 | 30 days | 30 days |

Target file sizes scale up with coldness — dormant data benefits more
from fewer, larger files because each scan amortizes over more rows.
Snapshot retention widens with coldness because infrequent writes
mean fewer legitimately-removable snapshots anyway.

### Hot vs Cold compaction

**hot (streaming tables):** `pkg/janitor.CompactHot` — parallel
delta-stitch across actively-written partitions. Up to
`PartitionConcurrency` (default 16) partitions stitched in parallel;
all replacements committed in a single-snapshot batched transaction.
Optimized to minimize the writer-fight CAS window — streaming
writers are committing at sub-second intervals and the janitor must
finish its commit before the next writer's snapshot races in.

**cold (batch / slow_changing / dormant):** `pkg/janitor.CompactCold`
— per-partition trigger-based full compaction. Each partition is
tested against three triggers before being compacted:

- **Small files trigger** — partition has > N small files (class-dependent)
- **Metadata ratio trigger** — partition's metadata bytes / data bytes exceeds a threshold (the H1 axiom, CB3)
- **Stale rewrite trigger** — partition hasn't been rewritten in longer than the class's stale age

Only triggered partitions are compacted. Runs sequentially — no CAS
urgency because foreign writers aren't actively competing.

### Explicit override

The classifier always runs and picks a mode, but operators can force
a specific mode via `?mode=hot|cold|full` on the maintain endpoint.
`full` is a legacy single-pass compaction over every partition (no
trigger filtering); it exists for tests and operator one-offs and is
never chosen by the classifier.

## Quick start

### Local (MinIO via Docker)

```bash
cd go/test/mvp
docker compose up -d          # starts MinIO on :9000
go run ./cmd/janitor-server   # starts server on :8080

# seed a table, run maintenance
go run ./cmd/janitor-cli seed s3://warehouse?endpoint=http://localhost:9000
curl -X POST http://localhost:8080/v1/tables/tpcds.db/store_sales/maintain
```

### Bench (MinIO end-to-end)

```bash
./go/test/bench/bench.sh minio
# streams TPC-DS data for 5 min, runs 2 maintain rounds, queries via DuckDB
# output: bench-results/bench-summary-<ts>.txt
```

### AWS (ECS Fargate)

```bash
cd go/deploy/aws/terraform
terraform apply                 # deploys 3-replica server + NLB + S3 buckets
aws ecs run-task \
  --cluster iceberg-janitor \
  --task-definition iceberg-janitor-bench \
  --network-configuration '{"awsvpcConfiguration":{"subnets":["<private-subnet-1a>"],...}}'
```

## Bench results (Run 18.6 -- MinIO, 2026-04-12)

| Metric | Without janitor | With janitor |
|---|---:|---:|
| store_sales files | 10,399 | 50 |
| store_returns files | 9,024 | 50 |
| catalog_sales files | 2,070 | 10 |
| **File reduction** | | **208x** |

Maintain wall time: 75s for 2 full rounds (expire + rewrite-manifests + CompactHot + post-rewrite). Zero partition failures. See [`go/BENCHMARKS.md`](go/BENCHMARKS.md) for the full history from Run 1 through Run 18.6.

## Server API

```
POST /v1/tables/{ns}/{name}/maintain       zero-knob full pipeline
POST /v1/tables/{ns}/{name}/compact        compact only
POST /v1/tables/{ns}/{name}/expire         expire only
POST /v1/tables/{ns}/{name}/rewrite-manifests
GET  /v1/jobs/{id}                         poll async job
GET  /v1/healthz                           NLB health check
```

All maintenance endpoints return `202 Accepted` with a job envelope. Poll `GET /v1/jobs/{id}` for completion. Per-table in-flight dedup prevents duplicate jobs. Optional `?dry_run=true` on every endpoint.

Full spec: [`go/openapi/janitor-server-v1.yaml`](go/openapi/janitor-server-v1.yaml)

## Repository layout

```
go/
  cmd/
    janitor-server/    HTTP server (ECS/Knative/Cloud Run)
    janitor-cli/       operator CLI (analyze, compact, expire, seed)
    janitor-lambda/    AWS Lambda adapter (stub)
  pkg/
    janitor/           compaction core (Compact, CompactHot, CompactCold, CompactTable)
    maintenance/       expire snapshots, rewrite manifests
    safety/            master check (I1-I9), circuit breaker (CB8)
    lease/             S3-backed cross-replica lock primitive
    jobrecord/         persistent async job records
    catalog/           directory catalog (LoadTable + atomic CommitTable)
    analyzer/          per-partition health + hot/cold classification
    strategy/          workload classifier (streaming/batch/dormant)
    observe/           OpenTelemetry tracing (NoOp by default)
    state/             persistent partition state on warehouse bucket
    testutil/          fileblob-backed test warehouse fixture
    tpcds/             TPC-DS schema + data generator (bench only)
  deploy/
    aws/terraform/     ECS Fargate + NLB + S3 + ECR + Athena + CloudWatch
  test/
    bench/             end-to-end bench harness (local/minio/aws modes)
    mvp/               docker-compose MinIO + runbook
  openapi/             OpenAPI 3.1 spec
  BENCHMARKS.md        measured numbers from every bench run
```

The initial Python reference implementation is preserved at [`reference/python-v0`](https://github.com/mystictraveler/iceberg-janitor/tree/reference/python-v0).

## Comparison

| Capability | iceberg-janitor | Spark rewriteDataFiles | AWS Glue auto-compact | Confluent Tableflow |
|---|---|---|---|---|
| Catalog required | No | Yes (Spark catalog) | Yes (Glue) | Yes (Confluent) |
| Compute | Stateless Go binary | Spark cluster | Glue ETL | Kafka Connect |
| Cold start | <200ms | Minutes | Minutes | N/A (always on) |
| Pre-commit verification | Mandatory I1-I9 | None | None | None |
| Cross-replica safety | S3 lease + CAS | N/A | N/A | N/A |
| Workload classification | Auto (streaming/batch/dormant) | Manual | Heuristic | N/A |
| Byte-copy stitch | Yes | No (decode/encode) | No | No |
| Dry-run mode | Yes (?dry_run=true) | No | No | No |
| **Cost at 1 PB** | **$539/mo** | **$5,980/mo** | ~$5,000/mo (est.) | N/A |

Full 3-way cost analysis with Spark and Amazon Managed Flink at 1 TB / 100 TB / 1 PB scale: [`go/COMPACTION_COST_COMPARISON.md`](go/COMPACTION_COST_COMPARISON.md)

## What makes this different

Six capabilities no other open source Iceberg compaction tool provides:

1. **Two-phase compaction** — byte-copy stitch (fast, zero decode) + automatic row group merge (only when needed). Spark/Flink always decode/encode every row. The janitor only pays that cost when the output has >4 row groups.

2. **Mandatory pre-commit master check** — every commit verifies 9 invariants (row count, schema, per-column stats, manifest refs). Non-bypassable. No `--force` flag. No other tool does this.

3. **Catalog-less** — reads metadata.json directly from object storage. No REST catalog, no Glue, no Hive metastore. Drop it onto any bucket with Iceberg tables.

4. **Automatic workload classification** — classifies each table as streaming/batch/slow_changing/dormant from its commit history. Per-class thresholds. Zero operator configuration.

5. **Cross-replica dedup** — S3 conditional-write leases prevent duplicate jobs across multiple server instances. Concurrent maintain requests for the same table return the existing job.

6. **Dry-run with contention detection** — `?dry_run=true` runs the full manifest walk, reports what would happen, and detects if a foreign writer is actively committing (snapshot ID drift).

## Invariants and Circuit Breakers

Every commit the janitor produces must be provably correct, and every table the janitor touches must be protected from runaway maintenance. These two guarantees are the core tenets of the project.

### Master check invariants (I1–I9)

The master check runs on every commit — compact and expire — before the CAS write to `metadata.json`. It is mandatory and non-bypassable. There is no `--force` flag. A failed invariant aborts the commit, releases the lease, and emits a structured `Verification` record.

| Invariant | Name | What it checks |
|---|---|---|
| **I1** | Row count | Total rows in = total rows out. Byte-copy stitch and pqarrow fallback must both conserve every row. |
| **I2** | Schema identity | Current schema ID must not change. Maintenance ops are forbidden from silently evolving the schema. |
| **I3** | Per-column value counts | Sum of value counts for every column across all data files must match before and after. |
| **I4** | Per-column null counts | Sum of null counts for every column must match before and after. |
| **I5** | Column bounds presence | The set of column IDs with bounds in the input must equal the set in the staged output. No missing, no spurious. |
| **I6** | V3 row lineage | *(Planned)* Row-level lineage tracking for Iceberg V3 tables. |
| **I7** | Manifest reference existence | Every data file added by this transaction must exist in object storage (HEAD check). Scoped to new files only — not the entire table. |
| **I8** | Manifest set equality | *(Planned)* Full manifest-level set comparison between input and staged. |
| **I9** | Content hash | *(Planned)* Byte-level content hash verification for compacted output. |

For expire operations, additional checks enforce that the current snapshot ID is unchanged (expire cannot reseat the main branch) and that the staged snapshot set is a strict subset of the input set (an expire that adds snapshots is a programming bug).

### Circuit breakers (CB2–CB11)

Circuit breakers protect tables from the janitor itself. They evaluate after every maintenance attempt and either **pause** the table (fatal — requires operator `resume`) or **skip** the current run (soft — next run retries).

| CB | Name | Type | What it does |
|---|---|---|---|
| **CB2** | Loop detection | Fatal | Pauses when the same file set is compacted N consecutive times (default 3). Detects feedback loops with external writers. |
| **CB3** | Metadata-to-data ratio | Warn / Fatal | Warns at 5%, critical at 10%, pauses at 50%. Enforces the H1 axiom: metadata should be a small fraction of data. |
| **CB4** | Effectiveness floor | Soft | Skips when compaction produces no file reduction. More files may accumulate to make the next run worthwhile. |
| **CB5** | Expire-before-orphan | Guard | Enforces ordering: expire snapshots before orphan removal, so orphan removal never deletes files still referenced by a live snapshot. |
| **CB6** | Rewrite-before-compact | Guard | Enforces ordering: rewrite manifests before compaction, so compaction reads a clean manifest layout. |
| **CB7** | Daily byte budget | Soft | Caps total bytes read + written per table per day. Resets at midnight. Prevents runaway I/O on hot tables. |
| **CB8** | Consecutive failure pause | Fatal | Pauses after 3 consecutive failed runs (default). Catches structural problems: writer faster than compactor, broken table, missing IAM permissions. |
| **CB9** | Lifetime rewrite ratio | Fatal | Pauses when cumulative bytes rewritten exceed a multiple of total table data. Catches misconfigured aggressive compaction. |
| **CB10** | Recursion guard | Guard | Refuses to operate on `_janitor/` internal paths. Prevents the janitor from compacting its own state files. |
| **CB11** | ROI estimate | Soft | Skips compaction whose estimated cost exceeds its benefit. Files reduced × query overhead must justify the bytes read. |

**Fatal** breakers write a pause file to `_janitor/state/<uuid>/pause.json`. The table stays paused until an operator explicitly resumes it. **Soft** breakers return a `SkippedError` — the table is not paused, and the next scheduled run retries normally. All breaker state is persisted to the warehouse bucket, so it survives server restarts and replica failovers.

## License

Apache-2.0
