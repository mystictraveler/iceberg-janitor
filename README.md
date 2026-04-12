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

## License

Apache-2.0
