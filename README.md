# iceberg-janitor

**Knative-powered, catalog-only Iceberg table maintenance with adaptive scheduling, Flink-based compaction, and a self-correcting feedback loop.**

Solves the small-file, snapshot explosion, and metadata bloat problems that occur when streaming data (Flink, Spark, etc.) continuously writes to Apache Iceberg tables. The janitor acts as a **maintenance cluster** — orchestrating health assessment, policy evaluation, and compaction execution — with the only hard dependency being the Iceberg catalog (REST, AWS Glue, Hive, or any PyIceberg-supported backend).

---

## Table of Contents

- [The Problem](#the-problem)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
- [Benchmark Results](#benchmark-results)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Configuration](#configuration)
- [Execution Model](#execution-model)
- [Adaptive Feedback Loop](#adaptive-feedback-loop)
- [REST API](#rest-api)
- [TPC-DS Test Suite](#tpc-ds-test-suite)
- [Project Structure](#project-structure)
- [Design Decisions](#design-decisions)
- [Research](#research)

---

## The Problem

When streaming engines write to Iceberg tables, each micro-batch commit creates:

| Symptom | Cause | Impact |
|---|---|---|
| **Thousands of tiny files** | Each Flink checkpoint / Spark batch creates new Parquet files | Defeats columnar read benefits, explodes S3 API calls |
| **Snapshot explosion** | Every commit = new snapshot | Metadata trees grow unbounded |
| **Manifest bloat** | Deep manifest chains from frequent commits | Query planning slows to a crawl |
| **Orphan files** | Failed writes, interrupted compaction | Storage costs grow silently |

Without automated maintenance, **query performance degrades continuously** until manual intervention.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                    Janitor Maintenance Cluster                      │
│                       (Knative Serving)                             │
│                                                                    │
│  ┌──────────┐  ┌────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │ REST API │  │ CloudEvent │  │   Analyzer    │  │  Policy     │  │
│  │ /v1/...  │  │ Handler    │  │ (catalog-only)│  │  Engine     │  │
│  └──────────┘  └────────────┘  └──────────────┘  └────────────┘  │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    Execution Router                           │  │
│  │                                                              │  │
│  │  table metadata ──→ size estimate ──→ route decision         │  │
│  │                        │                    │                │  │
│  │                   < threshold          > threshold           │  │
│  │                        │                    │                │  │
│  │                   LocalExecutor        FlinkExecutor         │  │
│  │                   (in-process)         (REST API submit)     │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                   Orchestrator (State Machine)                │  │
│  │                                                              │  │
│  │  IDLE → ANALYZING → SUBMITTING → RUNNING → POST_ANALYSIS    │  │
│  │                                         → FAILED → RETRY    │  │
│  │                                                              │  │
│  │  + AccessTracker  + AdaptivePolicyEngine  + FeedbackLoop     │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                    │
│  ┌──────────┐  ┌────────────┐  ┌──────────────┐                  │
│  │ Triggers │  │ Scheduler  │  │  Prometheus   │                  │
│  │ (commit, │  │ (priority, │  │  Metrics      │                  │
│  │  time,   │  │  rate limit│  │               │                  │
│  │  size)   │  │  windows)  │  │               │                  │
│  └──────────┘  └────────────┘  └──────────────┘                  │
└──────────────────────────┬─────────────────────────────────────────┘
                           │
          ┌────────────────┼────────────────────┐
          │                │                    │
     PingSource       KafkaSource          Manual CLI
     (scheduled)      (Kafka commits)      (on-demand)
                           │
                           ▼
              ┌─────────────────────────┐
              │   Flink Cluster (K8s)    │
              │   + Karpenter autoscale  │
              │                         │
              │  FlinkCompactionJob.java │
              │  - rewriteDataFiles      │
              │  - binpack/sort/zorder   │
              │  - partition filters     │
              └─────────────────────────┘
                           │
                           ▼
              ┌─────────────────────────┐
              │    Iceberg Catalog       │
              │  (REST / Glue / Hive)    │
              └──────────┬──────────────┘
                         │
                         ▼
              ┌─────────────────────────┐
              │   S3 / MinIO / GCS      │
              └─────────────────────────┘
```

---

## How It Works

### 1. Assess (catalog-only)
The analyzer queries table health through the **Iceberg catalog API** — `table.scan().plan_files()` and `table.metadata.snapshots`. No DuckDB, no direct S3 access. Works identically with REST Catalog, AWS Glue, or Hive Metastore.

### 2. Decide (policy + triggers)
The policy engine evaluates per-table thresholds. Triggers fire based on commit count, file count, elapsed time, or data size. The adaptive layer adjusts thresholds based on access patterns — hot tables compact sooner.

### 3. Route (smart execution)
The execution router checks table size against a configurable threshold:
- **Small tables** (< 1 GB default): compacted **in-process** within the janitor pod using PyIceberg
- **Large tables** (> 1 GB): submitted to a **Flink cluster** via REST API with full execution context (catalog URI, credentials, strategy, partition filters)

### 4. Measure (feedback loop)
After compaction, the feedback loop records before/after metrics and computes effectiveness. High effectiveness → boost priority for next cycle. Low effectiveness → reduce priority to avoid wasting compute.

---

## Benchmark Results

TPC-DS benchmark with 24 Iceberg tables, 7 streaming fact tables, 10 complex join queries:

```
PERFORMANCE COMPARISON: BEFORE vs AFTER COMPACTION
======================================================================
  Query                                        Before      After     Change
  ---------------------------------------- ---------- ---------- ----------
  q7_promo_impact (5-table join)              134.2ms     72.3ms   -46.1% faster
  q25_cross_channel_returns (7-table join)    142.4ms    103.5ms   -27.4% faster
  q1_top_return_customers (4-table join)       76.5ms     61.9ms   -19.1% faster
  q3_brand_revenue (3-table join)              53.8ms     44.4ms   -17.5% faster
  q13_demo_store_sales (6-table join)          86.4ms     72.9ms   -15.6% faster
  q43_weekly_store_sales (3-table join)        54.3ms     46.1ms   -15.0% faster
  ---------------------------------------- ---------- ---------- ----------
  TOTAL                                       858.4ms    685.6ms   -20.1%
```

[Full results (gist)](https://gist.github.com/mystictraveler/190206e73d6c8cca29aac40211673b38)

**51 files → 1 file** per table after compaction. Average file size increased from **~50KB to ~2MB**, reducing S3 API calls by 98%.

---

## Quick Start

### Prerequisites
- Docker, kubectl, minikube or kind
- Python 3.11+

### Local Development

```bash
# 1. Cluster with Knative
make kind-setup-knative    # or: minikube start + make knative-setup

# 2. Build and deploy
make docker-build && make docker-load
make dev-up

# 3. Port-forward services
kubectl -n iceberg-janitor port-forward svc/minio 9000:9000 &
kubectl -n iceberg-janitor port-forward svc/rest-catalog 8181:8181 &

# 4. Run TPC-DS benchmark (creates tables, streams data, compacts, compares)
pip install -e ".[query,dev]"
NUM_BATCHES=50 pytest tests/test_tpcds_benchmark.py -v -s

# 5. Run feedback loop validation
pytest tests/test_feedback_loop.py -v -s
```

### Catalog Configuration

```bash
# REST Catalog (local dev)
janitor analyze db.events --catalog-uri http://rest-catalog:8181 --warehouse s3://warehouse/

# AWS Glue
janitor analyze db.events --catalog-uri glue:// --warehouse s3://my-bucket/

# Hive Metastore
janitor analyze db.events --catalog-uri thrift://hive:9083 --warehouse s3://my-bucket/
```

---

## CLI Reference

```bash
# Health assessment
janitor analyze <table_id> --catalog-uri ... --warehouse ...

# Full policy-based maintenance
janitor maintain <table_id> --catalog-uri ... --warehouse ... [--dry-run]

# Manual overrides (immediate execution)
janitor compact <table_id> --catalog-uri ... --warehouse ... [--partition X] [--dry-run]
janitor expire-snapshots <table_id> --catalog-uri ... [--retention-hours 168]
janitor cleanup-orphans <table_id> --catalog-uri ... [--dry-run]

# Inspect trigger state
janitor trigger-status <table_id> --catalog-uri ... --warehouse ...

# Long-running controller
janitor controller config.yaml
```

---

## Configuration

### Policy (ConfigMap / Helm values)

```yaml
policy:
  default_policy:
    max_small_file_ratio: 0.3
    small_file_threshold_bytes: 8388608     # 8 MB
    target_file_size_bytes: 134217728       # 128 MB
    max_snapshots: 100
    snapshot_retention_hours: 168           # 7 days
    trigger_mode: auto                     # auto | manual | scheduled
    commit_count_trigger_threshold: 50
    time_trigger_interval_minutes: 30

  table_overrides:
    high_volume.events:
      trigger_mode: auto
      max_small_file_ratio: 0.2
      commit_count_trigger_threshold: 20
```

### Execution Routing

```yaml
execution:
  executor: auto              # auto | local | flink
  local_max_data_bytes: 1073741824  # 1 GB — below this, compact in-process
  flink_rest_url: http://flink-jobmanager:8081
  flink_default_parallelism: 4
  flink_max_parallelism: 32
  max_concurrent_jobs: 5
  job_timeout_seconds: 3600
```

### Adaptive Policy

```yaml
adaptive:
  enabled: true
  hot_threshold_queries_per_hour: 100
  warm_threshold_queries_per_hour: 10
  hot_multiplier: 0.5       # Hot tables: compact 2x sooner
  cold_multiplier: 3.0      # Cold tables: compact 3x less often
  feedback_loop_enabled: true
```

---

## Execution Model

### The Janitor as Maintenance Cluster

The janitor is not just an orchestrator — it's a **maintenance cluster** that does real work:

| Table Size | Where Compaction Runs | Why |
|---|---|---|
| **< 1 GB** | In the janitor pod (PyIceberg `table.overwrite()`) | Fast, no cluster overhead, sub-second latency |
| **> 1 GB** | Flink cluster (`rewriteDataFiles` action) | Distributed compute, handles TB-scale tables |

The execution router makes this decision automatically based on table metadata (no data scan needed — file sizes are in the catalog).

### Flink Cluster Sizing (with Karpenter)

The Flink cluster auto-scales via Karpenter:

| Scenario | TaskManagers | TM Memory | Parallelism | Time |
|---|---|---|---|---|
| 10 GB table | 2 | 4 GB | 4 | ~2 min |
| 100 GB table | 4 | 8 GB | 8 | ~10 min |
| 1 TB table | 8-16 | 8 GB | 16-32 | ~30 min |

**Karpenter strategy:**
- Pending Flink TaskManager pods trigger node scale-up
- Compute-optimized instances (c5/c6i) for compaction workloads
- Nodes scale down after 5 min idle (consolidation policy)
- Zero TaskManagers when no compaction is running → zero cost

### Execution Context

The janitor passes full execution context to Flink:

```json
{
  "table_id": "analytics.events",
  "catalog_uri": "http://rest-catalog:8181",
  "warehouse": "s3://warehouse/",
  "catalog_type": "rest",
  "catalog_properties": {"s3.endpoint": "http://minio:9000", ...},
  "strategy": "binpack",
  "target_file_size_bytes": 134217728,
  "partition_filter": "dt >= '2026-04-01'",
  "estimated_data_size_bytes": 53687091200
}
```

Flink executors receive this as program arguments, connect to the catalog independently, and execute `Actions.forTable(table).rewriteDataFiles()`.

---

## Adaptive Feedback Loop

The self-correcting cycle that gets smarter over time:

```
┌─────────────┐     ┌──────────────────┐     ┌────────────────┐
│ Query Engine │────→│  AccessTracker   │────→│ Classification │
│ (events API) │     │ (heat scoring)   │     │ hot/warm/cold  │
└─────────────┘     └──────────────────┘     └───────┬────────┘
                                                      │
                    ┌──────────────────┐              │
                    │  Adaptive Policy │◄─────────────┘
                    │  (adjust thresh) │
                    └───────┬──────────┘
                            │
                    ┌───────▼──────────┐
                    │  Compaction      │
                    │  (local / Flink) │
                    └───────┬──────────┘
                            │
                    ┌───────▼──────────┐     ┌──────────────────┐
                    │  FeedbackLoop    │────→│ Priority Adjust  │
                    │  (effectiveness) │     │ (back to tracker)│
                    └──────────────────┘     └──────────────────┘
```

| Access Pattern | Policy Adjustment | Compaction Behavior |
|---|---|---|
| **Hot** (>100 QPH) | 0.5x thresholds | Compact sooner, higher priority |
| **Warm** (10-100 QPH) | 1.0x (default) | Normal maintenance |
| **Cold** (<10 QPH) | 3.0x thresholds | Compact less, save resources |
| **Effective compaction** | Boost priority | Keep compacting aggressively |
| **Ineffective compaction** | Reduce priority | Skip next cycle, save compute |

---

## REST API

Simplified to 9 endpoints, mapped 1:1 to Iceberg's canonical maintenance actions:

| Endpoint | Iceberg Action |
|---|---|
| `GET /v1/tables/{id}/health` | Health assessment (no Iceberg equivalent) |
| `POST /v1/tables/{id}/rewrite-data-files` | `rewriteDataFiles` (binpack/sort/zorder) |
| `POST /v1/tables/{id}/expire-snapshots` | `expireSnapshots` |
| `POST /v1/tables/{id}/delete-orphan-files` | `deleteOrphanFiles` |
| `POST /v1/tables/{id}/rewrite-manifests` | `rewriteManifests` |
| `POST /v1/tables/{id}/rewrite-position-delete-files` | `rewritePositionDeleteFiles` (V2/V3) |
| `POST /v1/tables/{id}/maintain` | All actions in recommended order |
| `GET /v1/health` | Service health |
| `GET /v1/metrics` | Prometheus metrics |

Full OpenAPI 3.1 spec: [`openapi/iceberg-janitor-api-v2.yaml`](openapi/iceberg-janitor-api-v2.yaml)

---

## TPC-DS Test Suite

24 TPC-DS tables as Iceberg tables, with streaming micro-batch writes to 7 fact tables:

```bash
# Run with 50 micro-batches per fact table (~87K total rows)
NUM_BATCHES=50 pytest tests/test_tpcds_benchmark.py -v -s

# Run with 200 micro-batches (~10M total rows)
NUM_BATCHES=200 pytest tests/test_tpcds_benchmark.py -v -s
```

**10 benchmark queries** with 3-7 table joins:
- `q1`: Top customers by returns (4-table join)
- `q7`: Promotion impact on sales (5-table join)
- `q13`: Store sales by demographics (6-table join)
- `q25`: Cross-channel return analysis (7-table join)
- `q43`: Weekly store sales breakdown (3-table join)
- `q46`: Customer spend by city (6-table join)
- And 4 more...

**Feedback loop tests** (22 tests):
```bash
pytest tests/test_feedback_loop.py -v -s
```

---

## Project Structure

```
iceberg-janitor/
├── src/iceberg_janitor/
│   ├── analyzer/          # Health assessment via catalog API (no DuckDB)
│   ├── maintenance/       # Compaction, snapshot expiry, orphan cleanup
│   ├── execution/         # Executor backends: local (PyIceberg), Flink (REST)
│   │   ├── base.py        # Abstract CompactionExecutor + CompactionJob
│   │   ├── local.py       # In-process executor for small tables
│   │   ├── flink.py       # Flink REST API executor for large tables
│   │   ├── router.py      # Smart routing based on table size
│   │   └── orchestrator.py # State machine: analyze → submit → monitor → feedback
│   ├── strategy/          # Triggers, scheduler, access tracker, feedback loop
│   ├── policy/            # Per-table thresholds, trigger config, execution config
│   ├── api/               # FastAPI REST API + CloudEvents handler
│   ├── runner/            # Shared executor, cron entrypoint, controller
│   ├── metrics/           # Prometheus instrumentation
│   ├── cli/               # Click CLI with manual overrides
│   └── catalog.py         # Catalog connection factory
├── flink-jobs/            # Java: FlinkIcebergSink + FlinkCompactionJob
├── manifests/dev/         # K8s: MinIO, Kafka, REST Catalog, Knative, Flink, Karpenter
├── helm/                  # Helm chart (CronJob mode + Knative mode)
├── openapi/               # OpenAPI 3.1 spec (v1 + v2 simplified)
├── jmeter/                # DuckDB query benchmark suite
├── scripts/               # TPC-DS schema, data generators, setup scripts
├── tests/                 # TPC-DS benchmark + feedback loop + policy + analyzer tests
└── docs/                  # Research: Tableflow comparison, Iceberg spec, Flink sizing
```

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **Catalog-only metadata** | Portability across REST, Glue, Hive. No S3 credentials in the maintenance layer. |
| **DuckDB as optional extra** | Core maintenance doesn't need a query engine. DuckDB available for benchmarking via `pip install .[query]`. |
| **K8s → Knative evolution** | Scale-to-zero saves cost. Event-driven (KafkaSource) beats polling. Helm supports both modes. |
| **Janitor as maintenance cluster** | Small tables compact in-process (fast, no overhead). Large tables go to Flink (distributed, handles TBs). |
| **Flink for heavy compute** | Iceberg's `rewriteDataFiles` Action API is production-proven. Karpenter scales compute to zero when idle. |
| **Adaptive feedback loop** | Static thresholds fail in diverse enterprise environments. Self-correcting prioritization allocates compute where it matters. |
| **API names match Iceberg actions** | `rewrite-data-files` not `compact`. Self-documenting for Iceberg users. |
| **TPC-DS as benchmark** | Real-world query complexity with multi-table joins. Proves compaction impact on actual workloads. |

---

## Evolution

This project evolved through several architectural stages:

1. **K8s CronJob** — Simple scheduled maintenance every 15 minutes
2. **Long-running controller** — Poll-based catalog watching with trigger evaluation
3. **Knative migration** — Scale-to-zero Serving + event-driven PingSource/KafkaSource
4. **Catalog-only refactor** — Removed DuckDB/S3 dependencies from core, everything through PyIceberg
5. **Adaptive scheduling** — Access frequency tracking, heat classification, feedback loop
6. **Flink execution** — Smart routing: small tables in-process, large tables on Flink with Karpenter autoscaling
7. **TPC-DS validation** — 24-table benchmark proving 15-20% query improvement after compaction

---

## Research

- [Tableflow Comparison](docs/research-tableflow-comparison.md) — vs. Confluent Cloud's managed Iceberg maintenance
- [Iceberg Maintenance API Analysis](docs/research-iceberg-maintenance-api.md) — Gap analysis of the REST Catalog spec, V3 changes, API simplification
- [Flink Cluster Sizing](docs/flink-sizing.md) — TaskManager memory, parallelism, Karpenter autoscaling strategy

---

## License

Apache-2.0
