# Iceberg Table Maintenance Strategies: iceberg-janitor vs. Confluent Tableflow

> **Date:** 2026-04-05
> **Context:** Evaluating managed vs. self-managed approaches to Iceberg table maintenance under streaming workloads

---

## 1. The Problem

When streaming data continuously writes to Iceberg tables, metadata and file management degrade rapidly:

- **Small file proliferation** — Micro-batch commits from Flink/Spark/PyIceberg create thousands of tiny Parquet files (often < 1 MB each)
- **Snapshot explosion** — Every commit creates a new snapshot; high-frequency streaming can produce hundreds of snapshots per hour
- **Manifest bloat** — Each snapshot adds manifest files, slowing query planning as engines must parse deep metadata trees
- **Orphan files** — Failed writes, interrupted compaction, or expired snapshots leave unreferenced files consuming storage
- **Query degradation** — Small files defeat Parquet columnar benefits, increase S3 API calls, and balloon query latency

These problems compound: the more data streams in, the worse queries get, and the more urgent maintenance becomes.

---

## 2. Approach Comparison

### 2.1 Confluent Tableflow

Tableflow is a fully managed Confluent Cloud feature that materializes Kafka topics as Iceberg tables. Announced mid-2024, GA in late 2024/early 2025.

**Architecture:**
- Kafka topic → Tableflow (managed service) → Iceberg table (S3 + REST catalog)
- Schema derived automatically from Confluent Schema Registry (Avro, Protobuf, JSON Schema)
- Single writer — Tableflow is the sole process writing to the Iceberg table

**Maintenance model:**
- **Zero-ops** — all maintenance is handled internally with no user-facing configuration
- Compaction is time-and-size triggered by internal heuristics
- Snapshot commits are batched (not per-message) to limit snapshot count
- Manifest files are consolidated during compaction
- Orphan files are a non-issue since Tableflow controls the entire write path
- Schema evolution is automatic when the upstream Kafka topic schema changes

**Compaction specifics:**
- Target file sizes are in the 128–256 MB range (standard Iceberg best practice)
- Internal scheduling runs compaction periodically; exact intervals are opaque
- Data may be sorted within files during compaction to improve predicate pushdown
- Parquet files include column statistics, bloom filters, and dictionary encoding

**Limitations:**
- Only works with Confluent Cloud Kafka as the data source
- No user-configurable compaction thresholds, schedules, or strategies
- No per-table policy differentiation
- No manual override for on-demand maintenance
- Each topic is maintained independently — no cross-table coordination
- Vendor lock-in to Confluent Cloud

### 2.2 iceberg-janitor

iceberg-janitor is a self-managed, K8s-native Iceberg maintenance system built for enterprise streaming environments where multiple writers, heterogeneous workloads, and varying SLAs coexist.

**Architecture:**
- Catalog-agnostic (REST, Glue, Hive, SQL catalogs)
- Storage-agnostic (S3, MinIO, GCS, HDFS)
- Writer-agnostic (works with tables written by Flink, Spark, Trino, PyIceberg, or any combination)
- Deployed as K8s CronJobs (periodic) or a long-running controller (continuous)

**Maintenance model:**
- **DuckDB-based health analysis** — queries Iceberg metadata to assess file counts, size distributions, snapshot stats, and per-partition health without loading the data itself
- **Policy engine** — per-table configurable thresholds with profiles (standard, high-throughput, low-volume)
- **Trigger system** — pluggable triggers: commit count, file count, time interval, data size, with composite AND/OR logic
- **Maintenance operations** — compaction, snapshot expiration, orphan cleanup, manifest rewriting via PyIceberg
- **Partition-aware** — analyzes and compacts individual partitions rather than full tables

**Adaptive intelligence (unique):**
- **Access-frequency tracking** — monitors per-table query rates, classifies tables as hot/warm/cold
- **Adaptive policy engine** — dynamically adjusts compaction thresholds based on access patterns (hot tables compact sooner, cold tables less often)
- **Self-correcting feedback loop** — measures compaction effectiveness (before/after query latency, file counts), auto-adjusts future priority based on actual impact
- **Priority-based scheduling** — coordinates maintenance across all tables, processes most degraded/most accessed tables first

**Manual overrides:**
- CLI commands for on-demand compaction, snapshot expiration, orphan cleanup on any table
- Partition-scoped operations (compact only the partitions that need it)
- Dry-run mode for all operations
- Trigger status inspection

---

## 3. Side-by-Side Comparison

| Dimension | Confluent Tableflow | iceberg-janitor |
|---|---|---|
| **Operational model** | Fully managed SaaS | Self-managed on K8s |
| **Data source** | Confluent Cloud Kafka topics only | Any Iceberg table, any writer |
| **Compaction trigger** | Internal heuristics (opaque) | Configurable: commit count, file count, time, size, composite |
| **Per-table policies** | No | Yes — full per-table overrides, profiles |
| **Adaptive priority** | No | Yes — access-frequency heat scoring |
| **Feedback loop** | No | Yes — measures effectiveness, self-adjusts |
| **Snapshot management** | Managed internally | Configurable retention, min-keep, per-table |
| **Orphan cleanup** | N/A (single writer) | Detects and removes from multi-writer failures |
| **Partition awareness** | Auto-partitioning from Kafka key/timestamp | Per-partition analysis, targeted compaction, sort order suggestions |
| **Schema evolution** | Automatic from Schema Registry | Out of scope (handled by upstream writers) |
| **Manual overrides** | None | Full CLI: compact, expire-snapshots, cleanup-orphans |
| **Maintenance windows** | Not configurable | UTC windows, rate limiting, concurrent compaction slots |
| **Multi-table coordination** | Independent per topic | Priority-based scheduler across all tables |
| **Monitoring** | Confluent Cloud metrics | Prometheus metrics, DuckDB health reports, REST API |
| **Query performance insight** | None | DuckDB-based benchmarking (JMeter suite) before/after compaction |
| **Vendor lock-in** | Confluent Cloud required | Any Iceberg deployment, any catalog, any cloud |
| **Cost model** | Per-GB Confluent Cloud pricing | Compute costs for K8s jobs (your infrastructure) |

---

## 4. Architectural Tradeoffs

### 4.1 Single Writer vs. Multi-Writer

Tableflow's core advantage is **controlling the entire write path**. As the sole writer:

- No write conflicts or locking needed
- No orphan files by design (no failed partial writes from other processes)
- Can coordinate snapshot creation with compaction (expose only compacted data)
- Can batch commits precisely (one snapshot per N messages or T seconds)

iceberg-janitor operates in the **multi-writer reality** where Flink, Spark, Trino, and ad-hoc scripts all write to the same tables. This is messier but more common in enterprise environments. The tradeoff:

- Must detect and clean orphans from failed writes
- Must handle concurrent compaction safely (locking, retry)
- Cannot control commit frequency (that's the writers' responsibility)
- But supports any combination of writers and workloads

### 4.2 Opaque Heuristics vs. Tunable Policies

Tableflow optimizes for **simplicity**: no knobs to turn, no policies to write. This is ideal for teams that want Iceberg tables from Kafka with zero operational burden.

iceberg-janitor optimizes for **control**: enterprise data teams with different SLAs for different tables need different maintenance strategies. A real-time fraud detection table (query SLA < 200ms) needs much more aggressive compaction than a monthly analytics rollup.

### 4.3 Static vs. Adaptive Maintenance

Tableflow applies the same internal heuristics to every table. There is no concept of a table being "hotter" or more critical than another.

iceberg-janitor's adaptive system creates a **self-correcting feedback loop**:

```
Query engine reports access → AccessTracker classifies heat →
AdaptivePolicyEngine adjusts thresholds → Scheduler prioritizes →
Compaction runs → FeedbackLoop measures improvement →
Priority adjusted for next cycle
```

This means:
- A table that suddenly becomes critical (e.g., incident investigation, new dashboard) automatically gets more aggressive maintenance
- A table that was hot but goes cold (deprecated feature, seasonal data) naturally gets deprioritized
- Compaction that doesn't actually improve performance gets deprioritized (avoids wasting resources)

### 4.4 Cost Considerations

**Tableflow:**
- Confluent Cloud pricing per GB materialized
- No infrastructure to manage
- Cost scales with data volume and is less controllable
- Additional Confluent Cloud costs for Kafka, Schema Registry, etc.

**iceberg-janitor:**
- Runs on your existing K8s infrastructure
- Cost is the compute for CronJobs/controller (typically small — analysis is lightweight, compaction is the main cost)
- Can schedule maintenance during spot instance windows or off-peak hours
- No per-GB fees beyond your own storage costs

---

## 5. When to Use Which

### Use Confluent Tableflow when:
- Your data source is exclusively Confluent Cloud Kafka
- You want zero operational overhead for table maintenance
- All tables have similar access patterns and SLAs
- You are already invested in the Confluent ecosystem
- Schema evolution from Kafka is a priority

### Use iceberg-janitor when:
- Tables are written by multiple engines (Flink, Spark, Trino, etc.)
- Different tables have different SLAs and access patterns
- You need per-table policy control and manual overrides
- You want adaptive maintenance based on actual query patterns
- You need to run on your own infrastructure (cost control, data sovereignty)
- You need partition-level maintenance granularity
- You want observability into maintenance effectiveness

### Use both when:
- Some tables are fed by Confluent Kafka (let Tableflow handle those)
- Other tables are written by Flink/Spark (let iceberg-janitor handle those)
- iceberg-janitor can still monitor Tableflow-managed tables via DuckDB for independent health assessment

---

## 6. Gaps and Future Directions

### Tableflow gaps (that iceberg-janitor addresses):
- No user-configurable maintenance policies
- No access-frequency-based prioritization
- No feedback loop for compaction effectiveness
- No manual override for incident response
- No cross-table coordination
- Kafka-only data source

### iceberg-janitor gaps (that Tableflow addresses):
- No automatic schema evolution
- No built-in Kafka-to-Iceberg streaming (relies on Flink job)
- Requires operational knowledge to deploy and tune
- PyIceberg's compaction API is still maturing (may need Spark fallback for complex rewrites)
- No managed manifest rewriting yet (PyIceberg limitation)

### Shared future directions:
- **Sort order optimization** — Both could benefit from automatic Z-order or Hilbert curve sorting based on query patterns
- **Predictive maintenance** — Using query pattern trends to pre-emptively compact before degradation is noticeable
- **Cost-aware scheduling** — Scheduling compaction during cheaper compute windows (spot instances, off-peak)
- **Cross-catalog coordination** — Maintaining tables across multiple catalogs as a single fleet

---

## 7. References

- Confluent Tableflow documentation: `docs.confluent.io/cloud/current/topics/tableflow/`
- Apache Iceberg specification: `iceberg.apache.org/spec/`
- PyIceberg documentation: `py.iceberg.apache.org/`
- DuckDB Iceberg extension: `duckdb.org/docs/extensions/iceberg`
- iceberg-janitor source: `github.com/mystictraveler/iceberg-janitor`
