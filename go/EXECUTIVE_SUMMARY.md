# iceberg-janitor — Executive Summary

## The problem

Streaming and batch engines (Spark Structured Streaming, Kafka Connect, scheduled ETL, custom writers) produce thousands of small Parquet files per table per hour. Every commit adds a new snapshot, manifest list, and at least one manifest. Without continuous maintenance, metadata grows with commit count — not data volume — until reading the metadata costs more than reading the data. Query planning slows, S3 request costs explode, and commits start failing under manifest bloat.

The standard fix — Spark `rewriteDataFiles` or a managed compaction service — requires a catalog, a cluster, and per-workload tuning. Lakehouses that span clouds, accounts, or catalog technologies have no good answer.

## The solution

iceberg-janitor is a stateless Go binary that maintains Iceberg tables automatically. It stitches small files into target-sized ones via byte-copy (no Spark, no Flink, no decode/encode), expires stale snapshots, and rewrites fragmented manifests — all behind a mandatory 9-invariant master check. It classifies each table automatically (streaming, batch, slow-changing, dormant) and applies the right maintenance cadence and file-size targets without operator tuning.

Catalog-less: reads `metadata/` directly from object storage and commits atomically via conditional write. Drop it onto any S3, MinIO, GCS, or Azure Blob warehouse.

## Key capabilities

| Capability | Detail |
|---|---|
| **Two-phase compaction** | Byte-copy stitch (zero decode) + automatic row group merge (only when >4 row groups, or sort order defined, or V2 deletes apply) |
| **Workload classification** | 4-class auto-detection from commit history; per-class file-size targets, snapshot retention, and maintenance cadence |
| **Master check (I1–I9)** | Mandatory, non-bypassable pre-commit verification: row count, schema, per-column stats, manifest refs. No `--force` flag |
| **11 circuit breakers** | Loop detection, metadata ratio, daily byte budget, consecutive failures, lifetime rewrite ratio, ROI estimate |
| **Cross-replica safety** | S3 conditional-write leases + persistent job records; concurrent requests return existing job |
| **Dry-run with contention detection** | Full manifest walk + projected outcomes + snapshot-ID drift check, no side effects |
| **V2 delete handling** | Position and equality deletes applied during decode/encode merge; safety gate refuses V3 deletion vectors |
| **Async job API** | `POST /v1/tables/{ns}/{name}/maintain` returns 202 + job ID; poll for completion; persistent job records survive restarts |

## Bench evidence

**Run 20 — AWS S3, 3-replica ECS Fargate, TPC-DS streaming workload (2026-04-12):**

- **192× file reduction** across 3 fact tables (19,844 → 110 files)
- **23–27% faster Trino queries** (q1 −23%, q3 −27%)
- **5m47s maintain wall time** for 50 partitions at PartitionConcurrency=16

**Head-to-head vs Spark EMR Serverless** (same data, same output):

| Metric | iceberg-janitor | Spark EMR Serverless |
|---|---:|---:|
| Compute | 0.10 vCPU-hrs | 0.636 vCPU-hrs (**6.3×**) |
| Tuning required | None | `maxExecutors` + `maxConnections` |
| Cold start | 0s | ~90s |
| Safety | I1–I9 master check | None |

**Projected cost at 1 PB:** $539/mo (janitor) vs $5,980/mo (Spark) vs ~$5,000/mo (Glue).

## Deployment

One `terraform apply` deploys to AWS: ECS Fargate (3 replicas, ~30 MB distroless image), NLB, S3 warehouse buckets, ECR, CloudWatch dashboard with maintain pipeline timings and file-count reduction. Same container image runs on Fargate, EKS, Cloud Run, Knative, or Lambda.

## What's different

Six capabilities no other open-source Iceberg compaction tool provides: two-phase compaction (byte-copy + row group merge), mandatory pre-commit master check, catalog-less operation, automatic workload classification, cross-replica dedup, and dry-run with contention detection.
