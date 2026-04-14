# Flink Cluster Sizing Guide for Iceberg Compaction

This guide covers how to size and operate the Flink Session Cluster used by iceberg-janitor for compaction workloads.

## Architecture overview

The janitor orchestrator remains the control plane.  Flink is a pure execution engine:

```
┌─────────────────────┐       REST API        ┌──────────────────────┐
│  iceberg-janitor    │ ───────────────────▶   │  Flink JobManager    │
│  (orchestrator)     │   POST /jars/run       │  (1 replica, 1Gi)   │
│                     │   GET  /jobs/{id}      │                      │
│  ExecutionRouter    │   PATCH /jobs/{id}     │                      │
│  CompactionOrch.    │ ◀───────────────────   │                      │
└─────────────────────┘                        └──────────┬───────────┘
                                                          │ RPC
                                               ┌──────────▼───────────┐
                                               │  Flink TaskManagers  │
                                               │  (N replicas, 2Gi+)  │
                                               │                      │
                                               │  Iceberg read/write  │
                                               │  S3 I/O              │
                                               └──────────────────────┘
```

## Memory formula

TaskManager heap must accommodate the data that flows through each slot during compaction.

```
TaskManager process memory = 2 × (largest partition size / slots per TM)
```

**Why 2x?**  Iceberg compaction reads data files and writes new ones.  At peak, both the read buffer and the write buffer exist simultaneously, so the JVM needs roughly twice the working data set per slot.

### Example calculation

- Largest partition: 4 GB
- Slots per TaskManager: 2
- Per-slot data: 4 GB / 2 = 2 GB
- Required heap: 2 GB × 2 = 4 GB
- Recommended `taskmanager.memory.process.size`: **4608m** (4 GB + overhead)

## Parallelism

Parallelism determines how many file groups are compacted simultaneously.

| Rule of thumb | Formula |
|---|---|
| Minimum | `max(4, file_count / 250)` |
| Maximum | Capped at `flink_max_parallelism` (default 32) |
| Sweet spot | One slot per ~1 GB of data |

Higher parallelism speeds up compaction but requires more TaskManagers.  The orchestrator auto-calculates parallelism from `estimated_file_count` in the `CompactionJob`.

## Karpenter autoscaling strategy

The Karpenter NodePool (`flink-compaction`) provisions compute-optimized nodes on demand.

### Scale-up trigger

When Flink submits a compaction job that requires more TaskManager pods than currently scheduled, Kubernetes marks those pods as `Pending`.  Karpenter detects pending pods and provisions new nodes:

1. Janitor submits compaction job with parallelism=16.
2. Flink JobManager requests 8 TaskManagers (2 slots each).
3. Kubernetes cannot schedule 6 of them (only 2 existing nodes).
4. Karpenter launches 3 new `c6i.2xlarge` nodes within ~90 seconds.
5. TaskManagers start, Flink begins compaction.

### Scale-down trigger

After compaction completes, TaskManager pods terminate.  Karpenter consolidates:

- **`consolidationPolicy: WhenUnderutilized`** — nodes with low utilisation are drained and terminated.
- **`consolidateAfter: 5m`** — waits 5 minutes after pods leave before removing the node, avoiding churn if another compaction starts soon.
- **`expireAfter: 24h`** — hard TTL ensures nodes rotate for AMI freshness.

### Instance type selection

Karpenter picks the cheapest available instance from the allowed list:

| Instance | vCPUs | Memory | Best for |
|---|---|---|---|
| c5.xlarge | 4 | 8 GiB | Small compactions (< 10 GB) |
| c5.2xlarge | 8 | 16 GiB | Medium compactions (10-50 GB) |
| c5.4xlarge | 16 | 32 GiB | Large compactions (50-200 GB) |
| c6i.xlarge | 4 | 8 GiB | Small, graviton available |
| c6i.2xlarge | 8 | 16 GiB | Medium, graviton available |
| c6i.4xlarge | 16 | 32 GiB | Large, graviton available |

Spot instances are enabled for cost savings.  Compaction is idempotent, so spot interruptions only cause a retry (handled by the orchestrator).

## Scenario-based sizing

### Scenario 1: 10 GB table (500 small files)

| Parameter | Value |
|---|---|
| Parallelism | 4 |
| TaskManagers | 2 (2 slots each) |
| TM memory | 2 GiB each |
| Estimated duration | 2-5 minutes |
| Instance type | 1 × c5.xlarge (if not already running) |
| Estimated cost | ~$0.01 (spot) |

This table falls in the overlap zone.  The router may run it locally if Flink is unavailable or if configured with `executor: local`.

### Scenario 2: 100 GB table (5,000 small files)

| Parameter | Value |
|---|---|
| Parallelism | 20 |
| TaskManagers | 10 (2 slots each) |
| TM memory | 4 GiB each |
| Estimated duration | 10-20 minutes |
| Instance type | 3 × c5.2xlarge |
| Estimated cost | ~$0.15-0.30 (spot) |

Karpenter provisions nodes in ~90 seconds.  Total wall-clock including scale-up is ~12-22 minutes.

### Scenario 3: 1 TB table (50,000 small files)

| Parameter | Value |
|---|---|
| Parallelism | 32 (max) |
| TaskManagers | 16 (2 slots each) |
| TM memory | 8 GiB each |
| Estimated duration | 45-90 minutes |
| Instance type | 4 × c5.4xlarge |
| Estimated cost | ~$1.50-3.00 (spot) |

For tables this large, ensure `job_timeout_seconds` is set to at least 7200 (2 hours).  The orchestrator will cancel and retry if the job exceeds the timeout.

## Cost estimates by cluster profile

| Profile | Nodes | Monthly cost (spot) | Monthly cost (on-demand) | Throughput |
|---|---|---|---|---|
| Minimal | 1 × c5.xlarge always-on | ~$50 | ~$125 | ~50 GB/hour |
| Standard | 2 × c5.2xlarge on-demand base + Karpenter burst | ~$100-200 | ~$250-500 | ~200 GB/hour |
| High throughput | 4 × c5.4xlarge Karpenter-only | ~$200-600 | ~$600-1500 | ~800 GB/hour |

The recommended setup for most deployments is **Standard**: keep the Flink cluster at zero TaskManagers when idle and let Karpenter scale from 0 to N based on pending pods.  Set the TaskManager Deployment to `replicas: 0` and rely entirely on Karpenter for capacity.

## Configuration reference

These settings live in `PolicyConfig.execution` (see `src/iceberg_janitor/policy/models.py`):

| Setting | Default | Description |
|---|---|---|
| `executor` | `auto` | `auto`, `local`, `flink` |
| `flink_rest_url` | `http://flink-jobmanager:8081` | Flink REST API endpoint |
| `flink_jar_id` | (empty) | Pre-uploaded compaction JAR ID |
| `local_max_data_bytes` | 1 GiB | Tables below this use local executor |
| `flink_default_parallelism` | 4 | Default job parallelism |
| `flink_max_parallelism` | 32 | Parallelism cap |
| `max_concurrent_jobs` | 5 | Global concurrent job limit |
| `job_timeout_seconds` | 3600 | Per-job timeout |
| `retry_max_attempts` | 3 | Max retries on failure |
| `retry_backoff_seconds` | 60 | Base retry backoff |

## Operational tips

1. **Upload the compaction JAR once** after building:
   ```bash
   cd flink-jobs && mvn clean package -DskipTests
   curl -X POST -H "Expect:" \
     -F "jarfile=@target/flink-iceberg-sink-0.1.0.jar" \
     http://flink-jobmanager:8081/jars/upload
   ```
   Note the returned `id` and set it as `flink_jar_id`.

2. **Monitor via Flink Web UI**: port-forward `flink-jobmanager-rest:8081` for the dashboard.

3. **Graceful shutdown**: the orchestrator cancels in-flight Flink jobs on SIGTERM.  Cancelled compactions leave the table in its pre-compaction state (Iceberg's atomic commits ensure safety).

4. **Flink metrics**: the `FlinkCompactionJob` reports file counts and bytes processed via Flink accumulators, visible in the Flink Web UI and exportable to Prometheus via the Flink metrics reporter.
