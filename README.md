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

## How it works

[**Watch the animated explainer (1:17)**](https://github.com/mystictraveler/iceberg-janitor/releases/download/v0.1.0/iceberg_janitor_algorithm.mp4) — covers the stitching binpack algorithm, row group merge, delete handling, safety gates, CAS commit protocol, cost comparison vs Spark/Flink, and Hive import.

## Architecture

The janitor is one Go core (`pkg/janitor`) driven from three runtime
tiers. Every maintenance call flows through the same pipeline: classify
→ safety gate → manifest walk → stitch/merge → master check → CAS
commit → circuit breakers. Everything persists on the warehouse bucket
under a reserved `_janitor/` prefix — no external state store, no
catalog service.

```
  Producers (Spark Structured Streaming / Flink / Kafka Connect / iceberg-go / ...)
                                    │
                                    ▼
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │  Iceberg warehouse on object storage   (S3 | MinIO | GCS | Azure | file://)  │
 │  <warehouse>/<ns>.db/<table>/                                                 │
 │    ├── data/              parquet data + V2 pos/eq delete files               │
 │    ├── metadata/          v<N>.metadata.json, manifest-list, manifests        │
 │    └── _janitor/          reserved prefix — all janitor state                 │
 │         ├── state/<uuid>.json           TableState (CB2/CB7/CB8/CB9)          │
 │         ├── state/<uuid>/partitions.json   per-partition bookkeeping          │
 │         ├── state/leases/<ns>.<table>/<op>.lease   cross-replica CAS lock     │
 │         ├── state/jobs/<job_id>.json    persistent async job records          │
 │         ├── control/paused/<uuid>.json  CB-trip auto-pause markers            │
 │         └── results/<run_id>.json       per-run outcome reports               │
 └──────────────────────────────┬────────────────────────────────────────────────┘
                                │  gocloud.dev/blob   (s3 | gs | azblob | file)
                                │
     ┌──────────────────────────┼──────────────────────────┐
     ▼                          ▼                          ▼
 ┌─────────────────┐  ┌───────────────────┐  ┌───────────────────────────────┐
 │ janitor-server  │  │ janitor-lambda    │  │ janitor-cli                   │
 │ Fargate/Knative │  │ AWS Lambda        │  │ local | SSM | container       │
 │                 │  │ (one-shot)        │  │                               │
 │ POST …/maintain │  │ handler wraps     │  │ compact │ expire │ rewrite-   │
 │ POST …/compact  │  │ the same          │  │ manifests │ maintain │        │
 │ POST …/expire   │  │ pkg/janitor core  │  │ analyze │ glue-register       │
 │ POST …/rewrite  │  │                   │  │                               │
 │ GET  /v1/jobs/… │  │                   │  │                               │
 │ (?dry_run=true) │  │                   │  │                               │
 └────────┬────────┘  └─────────┬─────────┘  └───────────────┬───────────────┘
          │                     │                            │
          └─────────────────────┴───── shared ───────────────┘
                                │
                                ▼
 ┌───────────────────────────────────────────────────────────────────────────────┐
 │                            pkg/janitor  core                                  │
 │                                                                               │
 │   1. CLASSIFY  pkg/strategy/classify                                          │
 │      commit-rate windows → {streaming, batch, slow_changing, dormant}         │
 │      per-class plan: hot vs cold, target size, parallelism                    │
 │                                                                               │
 │   2. SAFETY GATE  pkg/janitor/safety_guards.go                                │
 │      refuse loudly: V3 deletion vectors, mixed partition spec-ids,            │
 │      equality deletes on complex column types                                 │
 │                                                                               │
 │   3. MANIFEST WALK  pkg/janitor/compact_replace.go                            │
 │      collect data files + V2 pos/eq delete refs  (partition-matched)          │
 │      Pattern B: skip files ≥ target size  (skipped for delete entries)        │
 │                                                                               │
 │   4. EXECUTE  pkg/janitor/{compact,compact_hot,compact_cold}.go               │
 │     ┌─────────────────────────────────────────────────────────────────┐       │
 │     │  Phase 1: byte-copy stitch  pkg/janitor/stitch.go               │       │
 │     │    parquet-go CopyRows — zero Arrow decode, zero CPU per row    │       │
 │     │    skipped when V2 deletes apply                                │       │
 │     │                                                                 │       │
 │     │  Phase 2: decode/encode merge  pkg/janitor/merge_rowgroups.go   │       │
 │     │    fires when: row_groups > 4  OR  sort order defined  OR       │       │
 │     │                V2 deletes apply                                 │       │
 │     │    apply row mask (pkg/janitor/deletes.go BuildRowMask)         │       │
 │     │    sort rows by table's default sort order                      │       │
 │     │    rewrite to 1 merged row group with fresh stats               │       │
 │     └─────────────────────────────────────────────────────────────────┘       │
 │                                                                               │
 │   5. MASTER CHECK  pkg/safety/verify.go   (mandatory, non-bypassable)         │
 │      I1 row count (w/ WithDeletedRows hint for V2 deletes)                    │
 │      I2 schema identity   I3 per-column values   I4 per-column nulls          │
 │      I5 bounds presence   I7 manifest refs exist   I8 file-set invariant      │
 │                                                                               │
 │   6. CAS COMMIT  pkg/catalog                                                  │
 │      If-None-Match:*  (S3, Azure) | IfNotExist  (GCS)                         │
 │      single snapshot per CompactHot call, all partitions batched              │
 │                                                                               │
 │   7. CIRCUIT BREAKERS  pkg/safety/circuitbreaker.go  (post-commit outcome)    │
 │      CB2 loop  CB3 meta-ratio  CB4 no-effectiveness  CB7 daily byte budget    │
 │      CB8 consecutive failures  CB9 lifetime rewrite ratio  CB10 recursion     │
 │      CB11 low-ROI     — trips write pause file, next call refuses             │
 │                                                                               │
 │   Cross-replica:   pkg/lease (TTL'd CAS per op)                               │
 │                    pkg/jobrecord (persistent async job state)                 │
 │                                                                               │
 │   Support:         pkg/catalog     directory catalog (LoadTable + CommitTable)│
 │                    pkg/analyzer    per-partition health                       │
 │                    pkg/maintenance expire + manifest rewrite + maintain orch. │
 │                    pkg/observe     OpenTelemetry tracing                      │
 │                    pkg/state       TableState / PauseFile / PartitionState    │
 └───────────────────────────────────────────────────────────────────────────────┘

              ┌─────────────────────────────┐
  optional →  │  AWS Glue (for Athena/EMR)  │ ← janitor-cli glue-register, or
              └─────────────────────────────┘     metadata_location returned in
                                                   server job result for external
                                                   registration (sandbox NACL fix)
```

### Key design choices

- **No catalog service.** The directory catalog reads `metadata/` directly from object storage and commits atomically via conditional write (`If-None-Match: *` on S3, `IfNotExist` on GCS/Azure). Works with any Iceberg table regardless of how it was created.
- **Two-phase compaction: byte-copy stitch + automatic row group merge.** Phase 1 copies parquet column chunks byte-for-byte between files (zero decode, zero CPU per row). Phase 2 automatically merges row groups when the stitched output has >4 per file — re-reads via pqarrow and rewrites with 1 merged row group, fresh column statistics. Result: query-optimal output (42% faster on Athena) without always paying the decode/encode cost. No other tool does this.
- **Single-snapshot batched commit.** CompactHot stitches N partitions in parallel (PartitionConcurrency=16), then commits ALL replacements in one transaction with one CAS write. The table gains exactly one snapshot per CompactHot call, not N.
- **Cross-replica safety.** The lease primitive + persistent job records let multiple server replicas coexist without duplicate work. Concurrent maintain requests for the same table return the existing job's ID (HTTP 202) instead of spawning a duplicate.
- **Mandatory master check.** Every CAS commit goes through `safety.VerifyCompactionConsistency` (compact) or `safety.VerifyExpireConsistency` (expire). Non-bypassable. Failures are recorded in the job result.
- **Dry-run mode.** Every maintenance endpoint accepts `?dry_run=true`. The server runs the full planning phase (manifest walk, staging, master check), then stops before any side effects. The result reports projected counts plus a `contention_detected` flag computed by reloading the table and comparing snapshot IDs.

## Maintenance operations

The janitor performs four maintenance operations on Iceberg tables.
Each is exposed as an async endpoint on `janitor-server` and as a
subcommand on `janitor-cli`. Every op supports `?dry_run=true` which
runs the full planning phase (manifest walk, staging, master check
where applicable) and reports projected outcomes without committing.

### compact

Replaces small data files with larger target-sized files. Two phases:

1. **Byte-copy stitch.** Parquet column chunks are copied
   byte-for-byte from source files into a new output file — no Arrow
   decode, no CPU per row. Preserves row order and original column
   statistics.
2. **Row group merge (conditional).** If the stitched output has more
   than 4 row groups, re-reads it via pqarrow and rewrites with a
   single merged row group. Produces tighter per-column stats and
   query-optimal layout. Also honors the table's default sort order
   if one is defined: rows are sorted by the sort-order columns
   during the decode/encode pass.

Every commit goes through `safety.VerifyCompactionConsistency` (the
master check) which validates row count, schema, per-column value
and null counts, column bounds presence, and manifest reference
existence. Non-bypassable. Failures are recorded on the job record
and no commit happens.

Dispatched automatically by the `maintain` pipeline based on
workload class (see below): hot mode uses parallel delta-stitch
with single-snapshot batched commit; cold mode uses per-partition
trigger-based full compaction.

### expire

Drops snapshots from the metadata snapshot chain beyond `keep_last` /
`keep_within` limits. The current snapshot is always retained. No
data files are rewritten — expire only edits metadata.

The master check for expire has different invariants from compact:

- Current snapshot ID must not change
- Current snapshot's row count must not change
- Schema must not change
- Snapshot set must shrink (staged ⊆ before; no new snapshot IDs)

When expired snapshots reference data files that are not referenced
by any retained snapshot, those data files become orphaned. Orphan
cleanup is a separate op not yet shipped — today, operators can run
a manual scan + delete or wait for it to land.

### rewrite-manifests

Consolidates per-commit micro-manifests into a partition-organized
layout. Streaming tables accumulate one manifest per commit — a
table with thousands of commits has thousands of micro-manifests,
each scanned on every read. rewrite-manifests reads the current
snapshot's manifest list, groups entries by partition tuple, and
writes new larger manifests each containing entries from a small
number of partitions.

Master check invariants:

- Total data file count is preserved exactly (I8: file set equality)
- Row count is preserved
- Schema is preserved
- No data files are touched — only manifests are rewritten

After this op, subsequent compactions' manifest walks open
dramatically fewer manifests because each manifest's partition
bounds are tighter and readers can skip non-matching manifests via
partition filter pushdown.

Run automatically as pre-compact and post-compact steps inside the
`maintain` pipeline. Can also be invoked standalone for tables that
need manifest consolidation without compaction.

### maintain

The zero-knob load-bearing entry point. Runs the full maintenance
pipeline in sequence:

1. **expire** — drop snapshots beyond `keep_last` / `keep_within`
2. **rewrite-manifests (pre-compact)** — fold micro-manifests so the
   next compaction reads a clean manifest layout
3. **compact** — dispatched per the workload classifier:
   streaming → `CompactHot` (parallel delta-stitch),
   batch / slow_changing / dormant → `CompactCold`
   (per-partition trigger-based)
4. **rewrite-manifests (post-compact)** — fold the compaction's
   micro-manifest back into the partition layout

Each step's result is surfaced in the job record, including which
steps ran, the wall time per step, and the final
`metadata_location` path so the orchestration layer can update
external catalogs (Glue, Unity, Polaris) with a direct pointer.

### Cross-cutting behavior

All mutating maintenance ops share:

- **Async execution.** HTTP POST returns `202 Accepted` with a job
  envelope immediately; work runs in a background goroutine. Poll
  `GET /v1/jobs/{id}` for completion.
- **Per-table in-flight dedup.** If a maintain/compact/expire job is
  already running for a given table when a second client POSTs the
  same endpoint, the server returns the EXISTING job's ID (still
  `202`) instead of spawning a duplicate. Works both in-process
  (within a replica) and cross-replica (via S3-backed lease files).
- **Persistent job records.** Every job has a warehouse-backed record
  at `_janitor/state/jobs/<job_id>.json` so any replica can answer
  poll requests for any job.
- **Atomic commits via CAS.** The directory catalog writes new
  metadata.json with conditional-create semantics (`If-None-Match: *`
  on S3, equivalent on GCS/Azure). If a foreign writer committed
  concurrently, the CAS fails and the janitor retries from the new
  current state.
- **Dry-run mode.** `?dry_run=true` runs the full planning phase
  and reports projected outcomes plus a `contention_detected` flag
  (computed by reloading the table at end of planning and comparing
  snapshot IDs). Use cases: operator sanity check before a risky
  maintain, CI gating, continuous "compactable-small-file tail"
  estimation.

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

## Bench results

### Run 20 — AWS S3 with row group merge (2026-04-12)

End-to-end bench on AWS: 3-replica janitor-server on ECS Fargate,
bench task streams TPC-DS data for 5 min to two warehouses, then
compacts one with the janitor while leaving the other untouched.
Athena queries both.

**File reduction: 192× on 3 fact tables.** store_sales: 9,599 → 50
files. store_returns: 8,335 → 50. catalog_sales: 1,910 → 10.

**Athena query performance** (same queries, both warehouses):

| Query | Uncompacted | Janitor (stitch + merge) | Change |
|---|---:|---:|---:|
| q1 | 3,266 ms | 2,515 ms | **-23%** |
| q3 | 2,618 ms | 1,908 ms | **-27%** |
| q7 | 2,356 ms | 2,285 ms | -3% |

**Maintain wall time:** 5m47s for store_sales CompactHot (50
partitions, 2 rounds, PartitionConcurrency=16). Zero partition
failures.

### Head-to-head: janitor vs Spark EMR Serverless (same data, 2026-04-12)

Tuned Spark (`maxExecutors=8`, `fs.s3.maxConnections=500`) compacts
the same data with identical output (50 files per table). Comparable
query performance (±4%). But the compute profile differs:

| Metric | iceberg-janitor | Spark EMR Serverless |
|---|---:|---:|
| Wall time (3 tables) | 353s | 256s |
| Compute | 0.10 vCPU-hrs | 0.636 vCPU-hrs (**6.3× more**) |
| Tuning required | None | `maxExecutors` + `maxConnections` |
| Cold start | 0s | ~90s |
| Safety | I1–I9 master check | None |

Spark wins 27% on wall time by throwing 6.3× more compute at it.
The janitor trades wall time for compute efficiency and safety.

**Caveat:** This comparison used a small streaming workload
(~68 MB / 3,000 files across 3 tables). Neither tool has been
benchmarked at TB+ scale or across all four workload classes. The
jury is still out on larger scales.

See [`go/BENCHMARKS.md`](go/BENCHMARKS.md) for the full history from Run 1 through Run 20 plus the empirical Spark comparison.

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
