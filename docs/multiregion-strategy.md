# Multi-Region Compaction Strategy

> **Date:** 2026-04-05
> **Context:** Evaluating compaction strategies when Iceberg tables are replicated across regions via S3 CRR

---

## Problem Statement

When Iceberg tables are replicated across regions (e.g., us-east-1 and eu-west-1) using S3 Cross-Region Replication (CRR), compaction creates a decision point:

1. **Compact independently** in each region (duplicate compute)
2. **Compact in one region**, let CRR ship the compacted files (transfer cost)
3. **Compact in one region only**, let the other region query uncompacted data (latency penalty)

## How Iceberg + S3 CRR Interact

```
Region A (us-east-1)                    Region B (eu-west-1)
┌─────────────────────┐                ┌─────────────────────┐
│ s3://warehouse-east/ │ ──── CRR ────→│ s3://warehouse-west/ │
│                     │                │                     │
│ metadata/v1.json    │ ──replicate──→ │ metadata/v1.json    │
│ data/file-001.parquet│ ──replicate──→ │ data/file-001.parquet│
│ data/file-002.parquet│ ──replicate──→ │ data/file-002.parquet│
│ ...                 │                │ ...                 │
└─────────────────────┘                └─────────────────────┘
```

When Region A compacts:
```
Region A: 100 small files → 1 large file (new-compacted.parquet)
          metadata/v2.json points to new-compacted.parquet

CRR copies: new-compacted.parquet → Region B
CRR copies: metadata/v2.json → Region B

Region B now has:
  - 100 old small files (still on disk, unreferenced by v2)
  - 1 new compacted file (replicated from A)
  - metadata/v2.json pointing to the compacted file
```

**Key insight:** After CRR completes, Region B automatically benefits from Region A's compaction — queries use the new metadata which points to the compacted file. The old small files become orphans in Region B.

**The catch:** CRR has latency (seconds to minutes). During replication, Region B serves queries against the old uncompacted data.

---

## Cost Model (AWS, April 2026)

| Operation | Cost |
|---|---|
| S3 PUT/COPY (per 1K requests) | $0.005 |
| S3 GET (per 1K requests) | $0.0004 |
| S3 Storage (per GB/month) | $0.023 |
| S3 CRR Transfer (per GB) | $0.02 |
| Cross-region data transfer (per GB) | $0.02 |
| Compaction compute (per GB, approx) | $0.001 |

**The fundamental ratio: transfer is 20x more expensive than compute.**

$0.02/GB (transfer) vs. $0.001/GB (compute) = 20:1

---

## Strategy Comparison

### Strategy 1: Compact Independently in Both Regions

```
Region A: Janitor → compact locally → done
Region B: Janitor → compact locally → done
```

**Cost:** 2x compaction compute
**Latency:** Both regions serve compacted data immediately
**Complexity:** Simple — no coordination needed

| Data Size | Cost per Region | Total Cost |
|---|---|---|
| 1 GB | $0.001 | $0.002 |
| 10 GB | $0.010 | $0.020 |
| 100 GB | $0.100 | $0.200 |
| 1 TB | $1.000 | $2.000 |

### Strategy 2: Compact in Region A, Ship to Region B via CRR

```
Region A: Janitor → compact → CRR replicates compacted files → Region B
Region B: Waits for CRR (no local compute)
```

**Cost:** 1x compute + 1x cross-region transfer
**Latency:** Region B has CRR lag (seconds to minutes of stale data)
**Complexity:** Medium — need to detect "already compacted via CRR"

| Data Size | Compute | Transfer | Total Cost |
|---|---|---|---|
| 1 GB | $0.001 | $0.020 | $0.021 |
| 10 GB | $0.010 | $0.200 | $0.210 |
| 100 GB | $0.100 | $2.000 | $2.100 |
| 1 TB | $1.000 | $20.000 | $21.000 |

### Strategy 3: Compact in Region A Only

```
Region A: Janitor → compact → fast queries
Region B: No compaction, queries against small files (slow)
```

**Cost:** 1x compute only
**Latency:** Region A fast, Region B 15-45% slower
**Complexity:** Simple but creates regional performance asymmetry

---

## Decision Matrix

| Data Size | Winner | Reason |
|---|---|---|
| **< 10 GB** | **Compact both** | Compute is $0.02 total. Not worth coordinating. |
| **10-100 GB** | **Compact both** | Transfer cost ($0.20-$2.00) > compute ($0.02-$0.20) |
| **> 100 GB** | **Compact both** | Transfer at $2+ far exceeds $0.20 compute. No contest. |
| **Any (latency-sensitive)** | **Compact both** | CRR lag means Region B serves stale data during transfer |

**The answer is almost always: compact independently in each region.**

The only exception: Region B has no compute capacity (no Flink cluster) AND the table is < 10 GB AND latency is not critical. In that case, rely on CRR.

---

## Recommended Architecture

```
                    ┌─────────────────────────────────┐
                    │         GitOps / Config          │
                    │    (shared policy, replicated)   │
                    └────────────┬────────────────────┘
                                 │
              ┌──────────────────┼──────────────────────┐
              │                                         │
              ▼                                         ▼
┌──────────────────────────┐          ┌──────────────────────────┐
│     Region A (us-east)    │          │     Region B (eu-west)    │
│                          │          │                          │
│  ┌────────────────────┐  │          │  ┌────────────────────┐  │
│  │  Janitor Cluster   │  │          │  │  Janitor Cluster   │  │
│  │  (Knative)         │  │          │  │  (Knative)         │  │
│  │  + Flink Cluster   │  │          │  │  + Flink Cluster   │  │
│  │  + Karpenter       │  │          │  │  + Karpenter       │  │
│  └────────┬───────────┘  │          │  └────────┬───────────┘  │
│           │              │          │           │              │
│  ┌────────▼───────────┐  │   CRR   │  ┌────────▼───────────┐  │
│  │ REST Catalog (A)   │  │ ◄─────► │  │ REST Catalog (B)   │  │
│  └────────┬───────────┘  │  meta   │  └────────┬───────────┘  │
│           │              │  sync   │           │              │
│  ┌────────▼───────────┐  │         │  ┌────────▼───────────┐  │
│  │ S3 (us-east)       │  │ ◄─────► │  │ S3 (eu-west)       │  │
│  └────────────────────┘  │   CRR   │  └────────────────────┘  │
└──────────────────────────┘  (data)  └──────────────────────────┘
```

### Design Principles

1. **Each region has its own janitor + Flink cluster.** Independent compaction, no cross-region coordination.

2. **Shared policy via GitOps.** Both janitors read the same policy config (replicated via git/ConfigMap sync). Same thresholds, same triggers.

3. **Compaction is idempotent.** If Region B's janitor detects the table was already compacted (few files, recent snapshot from CRR), it skips compaction. No wasted work.

4. **Orphan cleanup handles CRR artifacts.** When Region A compacts, the old small files in Region B become orphans after CRR delivers the new metadata. Each region's janitor cleans its own orphans.

5. **No cross-region compaction coordination.** The janitors are completely independent. This avoids distributed locking, network partitions, and cross-region API calls.

### Handling CRR-Compacted Tables

When Region B's janitor evaluates a table that was already compacted by Region A (and CRR delivered the compacted files + metadata):

```python
# In the janitor's assess_table():
table = catalog.load_table("analytics.events")
files = list(table.scan().plan_files())

# If file count is low and avg size is large → already compacted
if len(files) < policy.max_file_count and file_stats.small_file_ratio < policy.max_small_file_ratio:
    # Skip compaction — table is healthy (compacted via CRR from other region)
    return "no_action_needed"
```

This is already how the policy engine works — it checks table health before deciding to compact. If the table is healthy (because CRR delivered compacted data), no compaction runs. **Zero wasted compute.**

---

## Cost Summary at Scale

For a production deployment with 100 tables across 2 regions:

| Tables | Avg Size | Compact Both | Compact + Ship | Savings |
|---|---|---|---|---|
| 100 | 1 GB each | $0.20/cycle | $2.10/cycle | **90% cheaper** to compact both |
| 100 | 10 GB each | $2.00/cycle | $21.00/cycle | **90% cheaper** to compact both |
| 100 | 100 GB each | $20.00/cycle | $210.00/cycle | **90% cheaper** to compact both |

**Conclusion:** Always compact locally. The 20:1 transfer-to-compute cost ratio makes cross-region shipping uneconomical at any meaningful scale.
