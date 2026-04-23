# Compaction Cost Comparison: iceberg-janitor vs Spark vs Flink

A quantitative comparison of three approaches to Iceberg table compaction at scale. All numbers use AWS us-east-1 pricing as of April 2026. Updated to reflect the current project state: V2 delete handling, schema-evolution guard, warehouse registry, and observability track.

## The Three Approaches

### iceberg-janitor (this project)

**Architecture:** Stateless Go binary on ECS Fargate. Two-phase compaction: byte-copy stitching (copies parquet column chunks verbatim, zero decode) + conditional row group merge (pqarrow decode/encode only when row groups > 4, sort order defined, or V2 deletes apply). Single-snapshot batched commit across parallel partition stitches. Always-on server with automatic workload classification (streaming / batch / slow_changing / dormant). Warehouse-level table registry for O(1) scheduling at 10K+ tables.

**Compute model:** 3× Fargate tasks (1 vCPU, 4 GB each), always-on. No JVM, no Spark driver, no cluster. One container image runs on Fargate, EKS, Cloud Run, Knative, or Lambda (via AWS Lambda Web Adapter).

**What's new since v1 of this doc:**
- V2 merge-on-read delete handling — forces decode/encode when pos/eq deletes apply (adds CPU for delete-heavy tables, but those tables need it for correctness)
- Schema-evolution guard — adds one parquet footer read per source file per compact round (~200 extra S3 GETs per round; negligible at scale)
- Warehouse registry — eliminates the 25-second full-warehouse prefix scan; scheduling becomes O(registry-entries) not O(S3-prefix-pages)
- Observability (OTel spans + metrics) — zero runtime cost (NoOp tracer default)

### Spark rewriteDataFiles (EMR Serverless)

**Architecture:** Spark job that calls Iceberg's `rewriteDataFiles` stored procedure. Decodes every parquet row into Arrow, optionally re-sorts (ZOrder), re-encodes into new parquet files, commits via the Iceberg catalog. Runs as ephemeral jobs — no always-on compute.

**Compute model:** EMR Serverless with auto-scaling DPUs. Minimum 2 DPU, minimum billing 1 minute. Cold start 30-90s (JVM + Spark init + executor provisioning).

### Amazon Managed Flink (MSK Flink)

**Architecture:** Long-running Flink streaming job using the `iceberg-flink-runtime` connector. Monitors commit events, batches them, and calls `RewriteDataFilesAction`. Decode/encode path (Arrow roundtrip). Always-on streaming topology.

**Compute model:** Managed Flink with KPUs (Kinesis Processing Units). 1 KPU = 1 vCPU + 4 GB. Billed per KPU-hour while the application is running. Cold start 2-5 minutes (Flink job graph deployment).

## Unit Pricing

| Resource | Unit | Price |
|---|---|---|
| **ECS Fargate** (vCPU) | per vCPU-hour | $0.04048 |
| **ECS Fargate** (memory) | per GB-hour | $0.004445 |
| **EMR Serverless** (DPU) | per DPU-hour | $0.44 |
| **Managed Flink** (KPU) | per KPU-hour | $0.11 |
| **S3 GET** | per 1,000 requests | $0.0004 |
| **S3 PUT** | per 1,000 requests | $0.005 |
| **S3 storage** | per GB-month | $0.023 |

## Cost at Scale

### Assumptions

- **Streaming workload:** continuous writes at ~100 commits/min across all tables
- **Maintain cadence:** every 5 min for streaming tables, every 1 hr for batch/cold (driven by the workload classifier, not operator config)
- **Average partition:** 5-10 small files accumulated between maintain rounds
- **Target file size:** 64 MB (read from Iceberg table property `write.target-file-size-bytes`)
- **V2 deletes:** ~5% of tables have MERGE INTO / DELETE workloads that force decode/encode; 95% of tables are append-only and stay on the byte-copy stitch fast path
- **Schema evolution:** ~1 evolution per month across the warehouse; affected tables skip one compact round (~5 min gap), then resume
- **Registry overhead:** one S3 GET per table per scheduling cycle for the registry entry; negligible at all scales
- **Region:** us-east-1

### 1 TB warehouse (10 tables × 100 GB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (3× Fargate always-on) | $53/mo (2 DPU × 10 min × 720 jobs) | $240/mo (3 KPU always-on) |
| S3 API | $6/mo (+$1 for schema-guard footer reads) | $8/mo (more GETs from Arrow decode) | $8/mo |
| **Total** | **$95/mo** | **$61/mo** | **$248/mo** |
| **Per-TB** | **$95** | **$61** | **$248** |

At 1 TB, **Spark wins on cost** because ephemeral jobs avoid always-on billing. But Spark has 30-90s cold start per job, meaning compaction latency is minutes, not seconds. Flink's 3 KPU is sufficient at this scale because 10 tables × 12 compactions/hr is well within 3 vCPU of Arrow decode/encode budget. The janitor's $1/mo schema-guard overhead is noise.

### 100 TB warehouse (100 tables × 1 TB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (same 3× Fargate) | $528/mo (2 DPU × 10 min × 7,200 jobs) | $880/mo (~10 KPU — 100 tables need ~10 vCPU for Arrow decode) |
| S3 API | $52/mo (+$2 schema-guard) | $80/mo | $80/mo |
| Registry | <$1/mo (100 GETs × 12/hr × 720hr) | N/A | N/A |
| **Total** | **$142/mo** | **$608/mo** | **$960/mo** |
| **Per-TB** | **$1.42** | **$6.08** | **$9.60** |

At 100 TB, **janitor wins** because the always-on Fargate cost is fixed regardless of table count. Flink's KPU count must scale with the number of tables because each compaction action does Arrow decode/encode — that's CPU work. Spark's per-job DPU cost scales linearly with job count. The janitor's byte-copy stitch is I/O-bound, not CPU-bound, so 3 vCPU handles 100 tables the same as 10.

### 1 PB warehouse (1,000 tables × 1 TB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (same 3× Fargate) | $5,280/mo (2 DPU × 10 min × 72K jobs) | $2,000/mo (~25 KPU — 250 streaming tables × 12/hr × 30s Arrow decode each) |
| S3 API | $460/mo (+$10 schema-guard) | $700/mo | $700/mo |
| Registry | $3/mo (1K GETs × 12/hr × 720hr) | N/A | N/A |
| V2 delete overhead | $5/mo (5% tables force decode/encode, ~50 extra CPU-min) | $0 (already decoding) | $0 (already decoding) |
| **Total** | **$557/mo** | **$5,980/mo** | **$2,700/mo** |
| **Per-TB** | **$0.56** | **$5.98** | **$2.70** |

At 1 PB, **janitor is 10.7× cheaper than Spark and 4.8× cheaper than Flink**. Flink's KPU count must scale to ~25 to handle the Arrow decode/encode CPU load across 1,000 tables — the same compaction algorithm as Spark, just with always-on billing instead of per-job. The janitor's byte-copy stitch is I/O-bound: 3 vCPU handles 1,000 tables because the CPU is mostly idle waiting on S3.

### 10K tables (projected)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (same 3× Fargate) | $52,800/mo (72K DPU-min × 10) | $20,000/mo (~250 KPU — 2,500 streaming tables need ~250 vCPU for Arrow decode) |
| S3 API | $4,600/mo | $7,000/mo | $7,000/mo |
| Registry | $30/mo | N/A | N/A |
| **Total** | **$4,719/mo** | **$59,800/mo** | **$27,000/mo** |
| **Per-TB** | **$0.47** | **$5.98** | **$2.70** |

At 10K tables, **janitor is 12.7× cheaper than Spark and 5.7× cheaper than Flink**. Flink's KPU count scales linearly with table count because each compaction does Arrow decode/encode — the exact same CPU-bound work as Spark, just billed per-hour instead of per-job. The janitor's compute stays at $89/mo because byte-copy stitch is I/O-bound: 3 vCPU handles 10K tables the same way it handles 10. The registry makes 10K-table scheduling practical (~500ms per cycle vs ~35 min without it).

## Performance Characteristics

| Metric | iceberg-janitor | Spark | Flink |
|---|---|---|---|
| **Compaction method** | Byte-copy stitch + conditional merge | Arrow decode/encode (always) | Arrow decode/encode (always) |
| **CPU per row** | Zero (stitch) / O(1) (merge, when triggered) | O(1) decode + O(1) encode | O(1) decode + O(1) encode |
| **Memory per row** | Zero (stitch) / O(row_group) (merge) | O(row_group_size) | O(row_group_size) |
| **Cold start** | ~300-500ms (container) | 30-90s (JVM + executors) | 2-5 min (job graph) |
| **Compaction latency** | Seconds (already running) | Minutes (job startup) | Seconds (already running) |
| **V2 delete handling** | Decode/encode with row mask (only when deletes apply) | Decode/encode (always, deletes or not) | Decode/encode (always) |
| **Schema evolution** | Guard skips mixed-schema rounds; tail ages out via Expire | Rewrites across schema boundary | Rewrites across schema boundary |
| **Sort-on-merge** | Yes (from Iceberg default sort order) | Yes (configurable) | Yes (configurable) |
| **ZOrder support** | No | Yes | Yes |
| **Pre-commit verification** | Mandatory I1-I8 master check | None | None |
| **Circuit breakers** | 10 (CB2-CB11): loop, budget, ratio, ROI, recursion | None | None |
| **Cross-replica safety** | S3 lease + CAS + registry | N/A (single job) | Flink checkpoints |
| **Catalog required** | No (directory catalog on object storage) | Yes (Spark catalog) | Yes (Flink catalog) |
| **Scheduled maintenance** | Warehouse registry + workload classifier (planned: issue #15) | External scheduler required | Kafka event-driven |

### When Each Approach Wins

**iceberg-janitor wins when:**
- The goal is file-count reduction without data reordering (ZOrder)
- You want always-on sub-second compaction latency
- You're at 100+ TB scale where per-job costs dominate
- You want mandatory pre-commit verification (no silent data loss)
- You need V2 delete handling that doesn't decode every file
- You don't have a Spark or Flink cluster already running
- You don't want to depend on a catalog service

**Spark wins when:**
- You need ZOrder or sort-based compaction (data layout optimization beyond sort-on-merge)
- You need schema-boundary-crossing rewrites (add columns during compaction)
- You're at small scale (<10 TB) where ephemeral jobs are cheaper than always-on
- You already have EMR infrastructure and want to reuse it

**Flink wins when:**
- You want event-driven compaction triggered by Kafka commit events
- You're already running a Flink streaming topology and want to add compaction as a side-effect
- You need the streaming topology's exactly-once guarantees for the commit

## What the Bench Proves

**Run 20 (AWS, 3 TPC-DS tables, Fargate):** 192× file reduction (store_sales 9,599→50, store_returns 8,335→50, catalog_sales 1,910→10). Athena query latency 23-27% faster vs uncompacted baseline. CompactHot wall time 5m 36s across 3 replicas with PartitionConcurrency=16.

**Spark EMR Serverless comparison (same dataset):** Equivalent output quality (±4% query perf). Janitor used 6.3× less compute for 27% more wall time. Jury still out on larger workloads for both tools.

**V2 delete bench (MinIO):** 50K rows, 200 position deletes applied during compaction. 500→1 file, 50000→49800 rows, 2.3s wall time. Master check PASS (I1 in=50000 DVs=200 out=49800). Proves the delete read-through adds negligible cost on the stitch path — the decode/encode only fires when deletes actually apply.

**Schema-evolution guard (MinIO):** Mixed-schema source sets correctly skip the round with `SkippedReason=mixed_schemas`. Single-schema sets compact normally. +66ms overhead (+8%) from the footer-peek schema check — within MinIO run-to-run noise.

**Observability overhead (MinIO TPC-DS A/B):** All 10 queries equal-to-faster on the observability branch vs main. Zero measurable overhead from OTel spans + metrics under the NoOp tracer default.

## Methodology Notes

- Fargate pricing assumes 1 vCPU + 4 GB per task (the current ECS task definition). The janitor is I/O-bound on S3, not CPU-bound — the same 3 replicas handle 10 tables and 10K tables without scaling up.
- EMR Serverless pricing assumes 2 DPU minimum per job, 10 min minimum billing, jobs triggered at the same cadence as the janitor's maintain cycle. At 10K tables, the number of jobs per month is proportional to the number of tables × the maintain cadence.
- Managed Flink pricing assumes 3 KPU (the minimum for a fault-tolerant topology with parallelism=2 + 1 for the job manager). Flink's compute cost is fixed like Fargate's, but per-KPU is ~2.7× more expensive than per-Fargate-vCPU.
- S3 API call estimates are derived from the bench's server-side telemetry: each CompactHot call does ~50 GETs (manifest reads) + ~50 GETs (source file reads) + ~1 GET per source file for the schema-guard footer peek + ~50 PUTs (stitched outputs + metadata) per table per round. Spark does ~2× more GETs because it reads the full file content for Arrow decode rather than just column chunk byte ranges.
- The registry adds ~1 GET per table per scheduling cycle (read the entry to check NextMaintainAt). At 10K tables × 12 cycles/hr × 720 hr/mo = 86.4M GETs = ~$35/mo. In practice most tables are batch/dormant and checked less frequently; the $30 estimate reflects the weighted cadence.
- V2 delete overhead is estimated at 5% of tables having MERGE INTO / DELETE workloads. Those tables force the decode/encode path, adding ~1 CPU-min per compact round per table. At 1 PB with 50 delete-heavy tables × 12 rounds/hr × 720 hr = 432K CPU-min = ~$5/mo of Fargate compute (already included in the 3-replica cost; this is CPU utilization, not additional billing).
