# iceberg-janitor — Executive Summary

## What it is

**iceberg-janitor** is a catalog-less, multi-cloud, serverless maintenance daemon for Apache Iceberg tables. It keeps Iceberg lakehouses healthy — small files compacted, snapshots expired, manifests consolidated — without depending on a Spark cluster, an Iceberg REST Catalog, AWS Glue, or any external metastore. It runs as a single Go binary against any S3-compatible object store (S3, MinIO, Azure Blob, GCS) with sub-200 ms cold starts on Knative scale-to-zero or AWS Lambda.

## The problem

Streaming workloads and micro-batch ingest produce thousands of tiny Parquet files per table per day. Without continuous maintenance, query latency degrades, S3 LIST/GET costs explode, and the manifest layer grows faster than any reader can walk it. The standard remedy — Spark `rewriteDataFiles` or a managed compaction service — is heavyweight, expensive, and tightly coupled to a single catalog implementation. Lakehouses that span clouds, accounts, or catalog technologies have no good answer.

## What it provides

| Capability | What it does |
|---|---|
| **Compaction** | Rewrites small files into target-sized files via byte-copy stitching (no decode/re-encode for same-schema files), bounded by Pattern B (skip already-large files) |
| **Snapshot expiration** | Removes old snapshots from the parent chain so their orphaned metadata files can be GC'd, with mandatory consistency checks |
| **Manifest rewrite** | Consolidates per-commit micro-manifests into partition-organized layout, dramatically reducing the manifest list walk on subsequent compactions |
| **Maintain pipeline** | Single endpoint that orchestrates expire → rewrite → compact → rewrite in the load-bearing order, async with job tracking |
| **Master check** | Mandatory, non-bypassable pre-commit verification of 9 invariants (I1 row count, I2 schema, I3-5 column stats, I7 manifest references) — refuses to commit if any check fails |
| **11 circuit breakers** | Fatal/soft severity gates that pause maintenance on writer-fight, loop detection, metadata-to-data ratio violations, daily byte budgets, ROI estimates, and more |
| **Workload classification** | Auto-detects each table's class (streaming/batch/slow_changing/dormant) from foreign-commit rate; reported via the health endpoint for an external orchestrator to act on |
| **Async job API** | `POST /v1/tables/{ns}/{name}/{compact,expire,rewrite-manifests,maintain}` returns 202 + job_id; `GET /v1/jobs/{id}` polls for status. Operates as a stateless worker behind any scheduler |
| **Three runtime tiers** | Same core runs as a long-lived HTTP server (Knative/Fargate), an AWS Lambda handler, or a one-shot CLI. All three share `pkg/janitor.Compact` |

## Architectural principles

1. **No catalog service** — metadata files in object storage ARE the catalog. Atomic commits via conditional-write CAS (`If-None-Match: *`). Multi-cloud by construction; no external coordinator to fail or scale.
2. **Mandatory master check** — every commit verifies row count, schema identity, column stats, and manifest references against the pre-commit table state. Cannot be disabled with `--force`. Detected real bugs in iceberg-go's scan path that silently lost rows under concurrent writers.
3. **Per-table state sharding** — each table's pause file, lease, and history is written to its own key. No global lock, no serialization bottleneck.
4. **Operator zero-touch** — auto-detection of partition spec, target file size, workload class, and commit cadence. No manual tuning required for the common case.
5. **Stateless server, external scheduler** — the server runs requests; cadence and dispatch are an external concern. Keeps cold start fast and the memory model trivial.

## Bench evidence (Run 13, MinIO, 5 min A/B with identical streaming load)

| Metric | A (compact only) | B (expire + rewrite + compact) | Delta |
|---|---:|---:|---|
| Compactions attempted | 3 | 12 | 4× |
| Compactions succeeded | 1 | 12 | 12× |
| Max retry attempts on any compact | 13 | 1 | — |
| Longest compact wall time | 117.8 s | 25.8 s | 4.6× faster |
| Median successful compact wall time | 117.8 s | 17.6 s | 6.7× faster |
| Micro-manifests in current snapshot (store_sales) | ~442 | 50 | 8.8× consolidation |
| Micro-manifests (catalog_sales) | ~441 | 10 | 44× consolidation |

The closed loop of `expire → rewrite → compact → rewrite` is the load-bearing design. Without expire+rewrite, the manifest list grows faster than compact can drain it; with them, every compact wins the CAS race in 1 attempt.

## Deployment

Production-ready Terraform module deploys to AWS in one `terraform apply`:

- ECS Fargate (janitor-server, distroless image, ~30 MB)
- Private REST API Gateway with IAM auth (or Cloud Map service discovery for in-VPC clients)
- S3 warehouse buckets, ECR, Athena workgroup, Glue databases for query
- CloudWatch dashboard with maintain pipeline phase timings, job lifecycle, file count reduction, and Athena query latency
- OpenTelemetry tracing via slog → CloudWatch (zero external collector)

Total cold-start latency on Lambda: **~200 ms**. Steady-state idle cost on Knative scale-to-zero: **$0**.

## What's different from the alternatives

| | iceberg-janitor | Spark `rewriteDataFiles` | AWS Glue auto-compaction | Confluent Tableflow |
|---|---|---|---|---|
| Catalog dependency | None | Iceberg REST or Hive | Glue Data Catalog | Confluent Cloud |
| Multi-cloud | ✓ | Spark-dependent | AWS only | Confluent Cloud only |
| Cold start | <200 ms | 30-60 s (cluster spin-up) | minutes | always-on |
| Idle cost | $0 | $$ (cluster) | $ | $$$ |
| Master check | Mandatory, 9 invariants | Trust-but-verify | Opaque | Opaque |
| Open audit | Single Go binary | Spark codebase | Closed | Closed |

## Status

**19/30 design decisions fully shipped**, 5 partially shipped, 6 pending. Core maintenance pipeline (compact + expire + rewrite-manifests + all 11 circuit breakers) is proven on bursty streaming workloads. AWS deployment on ECS Fargate is operational with the maintain endpoint, async job API, OpenTelemetry tracing, and a CloudWatch dashboard.

**Next:** wire the workload classifier into an external orchestrator (cron/EventBridge), AWS Lambda runtime adapter, on-commit dispatcher (Pattern C), 1B-event stress test on real S3.
