# iceberg-janitor

**Knative-powered, catalog-only Iceberg table maintenance for streaming workloads.**

Solves the small-file, snapshot explosion, and metadata bloat problems that occur when streaming data continuously writes to Apache Iceberg tables — without requiring direct S3 access or DuckDB. The only external dependency is the Iceberg catalog (REST, AWS Glue, Hive, or any PyIceberg-supported catalog).

> **Evolution:** This project started as a K8s-native implementation using CronJobs and Deployments, then migrated to a fully **Knative** architecture — Knative Serving for scale-to-zero API/maintenance execution, Knative Eventing PingSource for scheduled maintenance, and KafkaSource for event-driven triggers on Kafka commits. The Helm chart still supports plain K8s mode (`knative.enabled: false`) for environments without Knative.

---

## The Problem

When streaming engines (Flink, Spark Structured Streaming, etc.) write to Iceberg tables, each micro-batch commit creates:

- **Small files** — thousands of tiny Parquet files (often < 1 MB) that defeat columnar read benefits
- **Snapshot explosion** — hundreds of snapshots per hour, bloating metadata
- **Manifest bloat** — deep manifest trees that slow query planning
- **Orphan files** — unreferenced files from failed writes consuming storage

These compound over time: the more data streams in, the worse queries get. Without automated maintenance, table performance degrades until someone manually intervenes.

## The Solution

iceberg-janitor provides automated, policy-driven table maintenance that:

1. **Assesses health** via the Iceberg catalog API (file counts, size distributions, snapshot stats, partition hotspots)
2. **Evaluates policies** with configurable per-table thresholds and tunable triggers
3. **Executes maintenance** — compaction, snapshot expiration, orphan cleanup, manifest rewriting
4. **Adapts automatically** — access-frequency tracking prioritizes hot tables; a feedback loop measures compaction effectiveness and self-adjusts

---

## Design Philosophy

### Catalog-only architecture

Every operation goes through the Iceberg catalog interface. No direct S3/GCS/HDFS access for metadata reads. This means:

```
                          PyIceberg Catalog API
                                  │
             ┌────────────────────┼────────────────────┐
             │                    │                     │
        REST Catalog        AWS Glue Catalog      Hive Metastore
```

Swap your catalog, swap your storage, swap your query engine — iceberg-janitor works the same way. The access pattern does not change.

**Why not DuckDB for metadata?** The original design used DuckDB's `iceberg_scan()` to query metadata by scanning S3 directly. This created a hard dependency on S3 credentials and the DuckDB iceberg extension — bypassing the catalog entirely. The refactored design uses `table.scan().plan_files()` and `table.metadata.snapshots` through PyIceberg, which routes through whatever catalog you've configured. DuckDB is still available as an optional dependency (`pip install iceberg-janitor[query]`) for the JMeter benchmarking suite, but is not required for any maintenance operation.

### From K8s-native to Knative

The original implementation used standard K8s primitives — a CronJob for scheduled maintenance and a Deployment for the long-running controller. This worked but had drawbacks: CronJob pods stay scheduled even when idle, and the controller polls the catalog continuously. The migration to Knative replaced these with event-driven, scale-to-zero components:

| Concern | Implementation |
|---|---|
| **API + maintenance execution** | Knative Serving — scales to zero between maintenance runs |
| **Scheduled maintenance** | Knative PingSource — sends CloudEvent every 15 min |
| **Event-driven maintenance** | Knative KafkaSource — reacts to Kafka commit events |
| **Infrastructure** (MinIO, Kafka, REST Catalog) | Standard K8s Deployments |

**Why the migration?** Three reasons:
1. **Cost** — Knative Serving scales to zero. You pay only for actual maintenance compute, not idle pods.
2. **Latency** — Event-driven triggers (KafkaSource) react to commits immediately instead of waiting for the next 15-minute poll cycle.
3. **Simplicity** — One Knative Service replaces both the CronJob and the controller Deployment. CloudEvents provide a uniform interface for all trigger types.

The Helm chart supports both modes via `knative.enabled: true|false` for environments without Knative. The same Docker image works for CLI, CronJob, and Knative — only the entrypoint differs.

### Maps 1:1 to Iceberg spec actions

The REST API uses the exact Iceberg action vocabulary:

| API Endpoint | Iceberg Action |
|---|---|
| `POST /v1/tables/{id}/rewrite-data-files` | `rewriteDataFiles` (binpack/sort/zorder) |
| `POST /v1/tables/{id}/expire-snapshots` | `expireSnapshots` |
| `POST /v1/tables/{id}/delete-orphan-files` | `deleteOrphanFiles` |
| `POST /v1/tables/{id}/rewrite-manifests` | `rewriteManifests` |
| `POST /v1/tables/{id}/rewrite-position-delete-files` | `rewritePositionDeleteFiles` (V2/V3) |
| `POST /v1/tables/{id}/maintain` | All of the above in recommended order |
| `GET /v1/tables/{id}/health` | No Iceberg equivalent — unique value |

The Iceberg REST Catalog spec has **zero maintenance endpoints** (it's a metadata protocol only). Every vendor builds proprietary maintenance. This API fills that gap as an open standard.

### Adaptive maintenance with feedback loop

Static thresholds don't work in enterprise environments where tables have vastly different access patterns. The adaptive system creates a self-correcting loop:

```
Query engine reports access → AccessTracker classifies heat (hot/warm/cold)
→ AdaptivePolicyEngine adjusts thresholds → Scheduler prioritizes
→ Compaction runs → FeedbackLoop measures effectiveness
→ Priority adjusted for next cycle
```

- **Hot tables** (high query frequency) get 0.5x thresholds — compact sooner
- **Cold tables** (rarely queried) get 3x thresholds — compact less, save resources
- **Ineffective compaction** (didn't improve query latency) gets deprioritized automatically

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Knative Serving                          │
│  ┌──────────┐  ┌───────────┐  ┌────────────┐  ┌────────────┐  │
│  │ REST API │  │ CloudEvent│  │  Analyzer   │  │Maintenance │  │
│  │ /v1/...  │  │ Handler   │  │ (catalog   │  │ (compact,  │  │
│  │          │  │ /ce       │  │  API only)  │  │  expire,   │  │
│  └──────────┘  └───────────┘  └────────────┘  │  orphans)  │  │
│                                                └────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Policy Engine + Strategy Layer               │  │
│  │  Triggers │ Scheduler │ AccessTracker │ FeedbackLoop     │  │
│  └──────────────────────────────────────────────────────────┘  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
              ┌────────────┼────────────────┐
              │            │                │
   ┌──────────────┐ ┌────────────┐ ┌──────────────┐
   │ PingSource   │ │ KafkaSource│ │ Manual CLI   │
   │ (scheduled)  │ │ (events)   │ │ (on-demand)  │
   └──────────────┘ └────────────┘ └──────────────┘
```

---

## Quick Start

### Prerequisites

- Docker
- minikube or kind
- kubectl
- (Optional) Knative CLI (`kn`)

### Local Development

```bash
# 1. Create cluster with Knative
make kind-setup-knative

# 2. Build and load image
make docker-build
make docker-load

# 3. Deploy the full stack (MinIO + Kafka + REST Catalog + Knative services)
make dev-up

# 4. Generate test data (creates the small-file problem)
make generate-data

# 5. Check table health via CLI
janitor analyze default.events \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://warehouse/ \
  --s3-endpoint http://localhost:9000

# 6. Run maintenance
janitor maintain default.events \
  --catalog-uri http://localhost:8181 \
  --warehouse s3://warehouse/ \
  --s3-endpoint http://localhost:9000 \
  --dry-run
```

### CLI Commands

```bash
# Health assessment
janitor analyze <table_id> --catalog-uri ... --warehouse ...

# Full policy-based maintenance
janitor maintain <table_id> --catalog-uri ... --warehouse ... [--dry-run]

# Manual overrides
janitor compact <table_id> --catalog-uri ... --warehouse ... [--partition X] [--dry-run]
janitor expire-snapshots <table_id> --catalog-uri ... --warehouse ... [--retention-hours 168]
janitor cleanup-orphans <table_id> --catalog-uri ... --warehouse ... [--dry-run]

# Inspect trigger state
janitor trigger-status <table_id> --catalog-uri ... --warehouse ...

# Start long-running controller
janitor controller config.yaml
```

### Catalog Configuration

```python
# REST Catalog (local dev)
janitor analyze db.events --catalog-uri http://rest-catalog:8181 --warehouse s3://warehouse/

# AWS Glue
janitor analyze db.events --catalog-uri glue:// --warehouse s3://my-bucket/

# Hive Metastore
janitor analyze db.events --catalog-uri thrift://hive:9083 --warehouse s3://my-bucket/
```

---

## Project Structure

```
iceberg-janitor/
├── src/iceberg_janitor/
│   ├── analyzer/          # Health assessment via catalog API (no DuckDB)
│   ├── maintenance/       # Compaction, snapshot expiry, orphan cleanup
│   ├── policy/            # Per-table thresholds, trigger configuration
│   ├── strategy/          # Triggers, scheduler, access tracker, feedback loop
│   ├── api/               # FastAPI REST API + CloudEvents handler
│   ├── runner/            # Executor, cron entrypoint, controller
│   ├── metrics/           # Prometheus instrumentation
│   ├── cli/               # Click CLI
│   └── catalog.py         # Catalog connection factory
├── manifests/             # Kustomize manifests (base + dev)
│   └── dev/               # MinIO, Kafka, REST Catalog, Knative services
├── helm/                  # Helm chart (supports both CronJob and Knative modes)
├── flink-jobs/            # Java Flink job: Kafka → Iceberg streaming
├── jmeter/                # Performance benchmarking (before/after compaction)
├── openapi/               # OpenAPI 3.1 spec (v1 original + v2 simplified)
├── scripts/               # kind-setup, dev-stack-up/down, data generator
├── docker/                # Dockerfiles
├── tests/                 # pytest suite
└── docs/                  # Research: Tableflow comparison, Iceberg spec analysis
```

---

## Policy Configuration

Policies are defined in a ConfigMap (or Helm values) with per-table overrides:

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
    default.high_volume_events:
      trigger_mode: auto
      max_small_file_ratio: 0.2
      max_snapshots: 50
      commit_count_trigger_threshold: 20
```

### Adaptive Policy (Enterprise)

When enabled, thresholds are dynamically adjusted based on access patterns:

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

## Research

- **[Tableflow Comparison](docs/research-tableflow-comparison.md)** — Detailed comparison with Confluent Cloud's Tableflow maintenance strategy
- **[Iceberg Maintenance API Analysis](docs/research-iceberg-maintenance-api.md)** — Gap analysis of the Iceberg REST Catalog spec, V3 format changes, and API simplification rationale

---

## Key Decisions

| Decision | Rationale |
|---|---|
| **Catalog-only metadata access** | Portability across REST, Glue, Hive. No S3 credential leakage to the maintenance layer. |
| **DuckDB as optional dependency** | Core maintenance should not require a query engine. DuckDB is available for benchmarking. |
| **Knative over CronJobs** | Scale-to-zero saves resources. Event-driven > polling. Helm chart supports both modes. |
| **Endpoint names match Iceberg actions** | `rewrite-data-files` not `compact`. Self-documenting for Iceberg users. |
| **Adaptive policy with feedback loop** | Static thresholds don't work across diverse enterprise workloads. |
| **V2 simplified API (9 endpoints)** | V1 had 17 endpoints duplicating catalog responsibilities. V2 focuses on what the catalog can't do. |

---

## License

Apache-2.0
