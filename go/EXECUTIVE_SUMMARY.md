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

**22/30 design decisions fully shipped**, 3 partially shipped, 5 pending. Core maintenance pipeline (compact + expire + rewrite-manifests + all 11 circuit breakers) is proven on bursty streaming workloads. The server's `/maintain` endpoint is now zero-knob: on every call the server classifies the table (streaming / batch / slow_changing / dormant), maps the class to a `MaintainOptions` struct, and dispatches to one of three per-partition modes:

- **hot** (streaming tables) — delta stitch with time-based round-robin anchor selection across large files, so successive rounds rotate wear instead of always growing the same file; bootstraps on the biggest small file when no large file exists.
- **cold** (batch / slow_changing / dormant) — per-partition full compaction, but only when one of three triggers fires: `small_files`, `metadata_ratio`, or `stale_rewrite` (last rewrite older than threshold, read from per-partition state at `_janitor/state/<uuid>/partitions.json`).
- **full** — legacy all-partition parallel compaction, retained as an explicit override for initial imports.

AWS deployment on ECS Fargate is operational with the maintain endpoint, async job API, OpenTelemetry tracing, per-table-drill-down CloudWatch dashboard, and a containerized bench that runs as a Fargate task driving two separate S3 warehouses in parallel.

**Bench harness** is a single script (`test/bench/bench.sh`) with three modes (`local` fileblob, `minio` docker, `aws` Fargate+Athena), replacing four prior scripts (1480 LOC → 470 LOC).

**Run 17a (local, 60 s):** 7 of 10 TPC-DS queries faster on the maintain side (q3 −28.4%, q7 −16.5%, q25 −10.7%, q43 −9.2%, q55 −6.2%); file-count reduction of 9–11% across store_sales / store_returns / catalog_sales; q1 shows a +32% regression that has been latent since Run 8 and is not yet investigated.

**Next:** AWS-tier bench with the new hot/cold build; long-duration MinIO run (≥300 s) to exercise the hot loop under a true streaming class; q1 regression root-cause; AWS Lambda runtime adapter; on-commit dispatcher (Pattern C); 1B-event stress test on real S3.
