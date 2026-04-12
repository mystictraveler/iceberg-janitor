# Compaction Cost Comparison: iceberg-janitor vs Spark vs Flink

A quantitative comparison of three approaches to Iceberg table compaction at scale. All numbers use AWS us-east-1 pricing as of April 2026.

## The Three Approaches

### iceberg-janitor (this project)

**Architecture:** Stateless Go binary on ECS Fargate. Byte-copy stitching — copies parquet column chunks verbatim between files with no Arrow decode/encode. Single-snapshot batched commit across parallel partition stitches. Always-on server with automatic workload classification.

**Compute model:** 3× Fargate tasks (1 vCPU, 4 GB each), always-on. No JVM, no Spark driver, no cluster. Cold start <200ms.

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
- **Maintain cadence:** every 5 min for hot tables, every 1 hr for cold
- **Average partition:** 5-10 small files accumulated between maintain rounds
- **Target file size:** 64 MB
- **Region:** us-east-1

### 1 TB warehouse (10 tables × 100 GB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (3× Fargate always-on) | $53/mo (2 DPU × 10 min × 720 jobs) | $240/mo (3 KPU always-on) |
| S3 API | $5/mo | $8/mo (more GETs from Arrow decode) | $8/mo |
| **Total** | **$94/mo** | **$61/mo** | **$248/mo** |
| **Per-TB** | **$94** | **$61** | **$248** |

At 1 TB, **Spark wins on cost** because ephemeral jobs avoid always-on billing. But Spark has 30-90s cold start per job, meaning compaction latency is minutes, not seconds.

### 100 TB warehouse (100 tables × 1 TB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (same 3× Fargate) | $528/mo (2 DPU × 10 min × 7,200 jobs) | $240/mo (3 KPU) |
| S3 API | $50/mo | $80/mo | $80/mo |
| **Total** | **$139/mo** | **$608/mo** | **$320/mo** |
| **Per-TB** | **$1.39** | **$6.08** | **$3.20** |

At 100 TB, **janitor wins** because the always-on Fargate cost is fixed regardless of table count, while Spark's per-job DPU cost scales linearly with the number of compaction jobs.

### 1 PB warehouse (1,000 tables × 1 TB)

| Component | iceberg-janitor | Spark (EMR Serverless) | Flink (Managed) |
|---|---:|---:|---:|
| Compute | $89/mo (same 3× Fargate) | $5,280/mo (2 DPU × 10 min × 72K jobs) | $240/mo (3 KPU) |
| S3 API | $450/mo | $700/mo | $700/mo |
| **Total** | **$539/mo** | **$5,980/mo** | **$940/mo** |
| **Per-TB** | **$0.54** | **$5.98** | **$0.94** |

At 1 PB, **janitor is 11× cheaper than Spark and 1.7× cheaper than Flink**. The janitor's cost is dominated by S3 API calls, not compute — the 3-replica Fargate service handles 1,000 tables without scaling up because each CompactHot call is I/O-bound (waiting on S3), not CPU-bound.

## Performance Characteristics

| Metric | iceberg-janitor | Spark | Flink |
|---|---|---|---|
| **Compaction method** | Byte-copy stitch | Arrow decode/encode | Arrow decode/encode |
| **CPU per row** | Zero | O(1) decode + O(1) encode | O(1) decode + O(1) encode |
| **Memory per row** | Zero (stream copy) | O(row_group_size) | O(row_group_size) |
| **Cold start** | <200ms | 30-90s (JVM + executors) | 2-5 min (job graph) |
| **Compaction latency** | Seconds (already running) | Minutes (job startup) | Seconds (already running) |
| **ZOrder support** | No (not needed for file-count reduction) | Yes | Yes |
| **Schema evolution during compact** | No | Yes | Yes |
| **Pre-commit verification** | Mandatory I1-I9 master check | None | None |
| **Cross-replica safety** | S3 lease + CAS | N/A (single job) | Flink checkpoints |
| **Catalog required** | No (directory catalog) | Yes (Spark catalog) | Yes (Flink catalog) |

### When Each Approach Wins

**iceberg-janitor wins when:**
- The goal is file-count reduction without data reordering
- You want always-on sub-second compaction latency
- You're at 100+ TB scale where per-job costs dominate
- You want mandatory pre-commit verification (no silent data loss)
- You don't have a Spark or Flink cluster already running

**Spark wins when:**
- You need ZOrder or sort-based compaction (data layout optimization)
- You need schema evolution during compaction
- You're at small scale (<10 TB) where ephemeral jobs are cheaper than always-on
- You already have EMR infrastructure and want to reuse it

**Flink wins when:**
- You want event-driven compaction triggered by Kafka commit events
- You're already running a Flink streaming topology and want to add compaction as a side-effect
- You need the streaming topology's exactly-once guarantees for the commit

## What the Bench Proves

Run 18.6 (MinIO, local) achieved **208× file reduction** (10,399 → 50 files) in **75 seconds** of maintain time across 3 tables and 2 rounds. The same workload on Spark EMR Serverless would require:

1. 3 DPU-min cold start
2. ~5 min of Arrow decode/encode per table
3. ~3 separate job submissions (one per table, or batched with partition overhead)

Estimated Spark wall time for the same work: **8-15 minutes** vs janitor's **75 seconds**. The 6-12× wall-time advantage comes from:
- No cold start (server is already running)
- No Arrow decode/encode (byte-copy is I/O-bound, not CPU-bound)
- Batched commit (one snapshot per CompactHot, not one per partition)

## Methodology Notes

- Fargate pricing assumes 1 vCPU + 4 GB per task (the current ECS task definition)
- EMR Serverless pricing assumes 2 DPU minimum per job, 10 min minimum billing, jobs triggered at the same cadence as the janitor's maintain cycle
- Managed Flink pricing assumes 3 KPU (the minimum for a fault-tolerant topology with parallelism=2 + 1 for the job manager)
- S3 API call estimates are derived from the bench's server-side telemetry: each CompactHot call does ~50 GETs (manifest reads) + ~50 GETs (source file reads) + ~50 PUTs (stitched outputs + metadata) per table per round. Spark does ~2× more GETs because it reads the full file content for Arrow decode rather than just column chunk byte ranges
- The janitor's S3 cost at 1 PB is an extrapolation from the 100 TB number. Real workloads with high partition counts may have higher manifest-walk costs; the rewrite-manifests step (which consolidates micro-manifests before compact) mitigates this
