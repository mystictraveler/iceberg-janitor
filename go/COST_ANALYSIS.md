# Cost analysis: prevention vs remediation vs the middle path

A quantitative comparison of three architectural approaches to the small-file problem in streaming-writes-to-Iceberg, with public AWS pricing line items so operators can re-run the math against their own deployments.

This document is the canonical source of the cost story. It's referenced by [`MOONCAKE_COMPARISON.md`](MOONCAKE_COMPARISON.md), [`TABLEFLOW_COMPARISON.md`](TABLEFLOW_COMPARISON.md), [`MIDDLE_PATH.md`](MIDDLE_PATH.md), and the root [`README.md`](../README.md).

---

## TL;DR

**NVMe storage is not cheap. Continuous compute is not cheap. The right architectural choice depends entirely on what fraction of the time your workload is actually active.** For workloads active <50% of the time (the vast majority of real-world Iceberg deployments), the janitor's serverless cost model wins regardless of the architectural elegance argument. For workloads active >75% of the time on a small number of hot tables, moonlink's prevent-at-write avoids the read-rewrite double pass and wins.

A mature data platform might run **both**: moonlink for the few tables where its cost premium pays for itself, the middle-path janitor for everything else.

---

## The three approaches

| | Owns the writer? | Compute pattern | Storage requirement | Catalog story |
|---|---|---|---|---|
| **moonlink** (prevent at write) | yes (Postgres CDC, Kafka, REST, OTEL) | always-on workers, NVMe-resident buffer + index | NVMe SSD on hot path; S3 for committed tables | their own / managed |
| **janitor polling** (remediate after) | no | serverless, scheduled (every 5-15 min) | S3 only | none (directory catalog) |
| **janitor middle path** (event-driven on-commit) | no | serverless, event-triggered (per writer commit) | S3 only | none (directory catalog) |

---

## The reference workload

A real-time analytics deployment typical for both projects:

| Parameter | Value |
|---|---|
| Tables monitored | 50 |
| Writers per table | 1 (Postgres CDC, Kafka, etc.) |
| Commits per minute per table | 10 (every 6 seconds, typical Flink/CDC checkpoint cadence) |
| Bytes per commit | 1 MB (small batch) |
| Total ingest rate | 50 tables × 10 commits/min × 1 MB = 500 MB/min ≈ 30 GB/hr ≈ 720 GB/day |
| Active retention | 30 days = ~21.6 TB of data, plus metadata |
| Read pattern | Continuous (queries every minute) |
| Region | AWS us-east-1 |
| Pricing date | early 2026 |

All AWS line items use the public on-demand pricing. Numbers are reproducible.

---

## moonlink cost (self-hosted)

moonlink's value prop is "buffer + index on NVMe, then flush size-tuned Parquet to Iceberg." That requires **at least one always-on worker per active workload**, sized to hold the buffer + index in memory + NVMe. The published moonlink architecture diagram shows the buffer, cache, and index all NVMe-resident.

Two NVMe paths are possible:

### Path A: Instance-store NVMe (cheap, ephemeral)

| Line item | AWS SKU | Quantity | Unit cost | Monthly |
|---|---|---|---|---|
| Compute (always-on, HA) | EC2 `r6gd.xlarge` (4 vCPU, 32 GB RAM, 237 GB NVMe instance store) | 2 instances | $0.226/hr × 730 hr | **$330.00** |
| NVMe local instance store | included in `r6gd` SKU | included | $0 | $0 |
| Iceberg storage in S3 | S3 Standard | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT operations | one per moonlink flush, ~1/min/table = 50/min | ~2.2M | $0.005/1k | **$11.00** |
| S3 GET operations (queries) | reasonable estimate | ~10M | $0.0004/1k | **$4.00** |
| Cross-AZ data transfer (HA) | between the two workers | ~10 GB/day | $0.01/GB | **$3.00** |
| **Total (instance store NVMe path)** | | | | **~$845/month** |

The instance store catch: **the NVMe is ephemeral**. If an `r6gd.xlarge` instance is replaced (hardware failure, rolling upgrade, spot reclaim), the buffer + index are gone. moonlink would need to either (a) replay from the source on restart (Kafka offsets, Postgres LSN) or (b) checkpoint state to durable storage periodically. Both add complexity but are tractable.

### Path B: Provisioned-IOPS EBS NVMe (durable, expensive)

| Line item | AWS SKU | Quantity | Unit cost | Monthly |
|---|---|---|---|---|
| Compute (always-on, HA) | EC2 `r6g.xlarge` (4 vCPU, 32 GB RAM, no instance store) | 2 instances | $0.20/hr × 730 hr | **$292.00** |
| NVMe via EBS | `io2 Block Express` provisioned IOPS SSD | 500 GB × 2 instances | $0.125/GB-month + $0.065/IOPS-month × 16k IOPS | **~$2,165.00** |
| Iceberg storage in S3 | S3 Standard | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 ops + transfer | (same as above) | | | **~$18.00** |
| **Total (EBS NVMe path)** | | | | **~$2,972/month** |

The EBS path is what you pay for durability and operational simplicity. Most production-grade self-hosted deployments would land somewhere in the middle (e.g., instance store for the buffer + EBS for the index, or instance store with periodic snapshots to S3) — call it ~$1,200-2,500/month all-in.

### Managed moonlink (Mooncake's own cloud)

Mooncake's hosted offering builds in some markup over the self-hosted instance-store path. The realistic managed price for this workload is probably **$1,200-$2,500/month** depending on how much HA + SLA is included. Public per-GB or per-event pricing isn't published as of this writing.

---

## janitor cost (polling mode — current)

Same workload, same 50 tables, same 720 GB/day ingest. The janitor doesn't run continuously; it runs on a schedule. For polling-mode compaction once per 5 minutes:

| Line item | AWS SKU | Quantity | Unit cost | Monthly |
|---|---|---|---|---|
| Lambda invocations | `arm64`, 2048 MB, ~30 sec average (50 tables × 12 runs/hour × 730 hr = 438k) | 438,000 | $0.20/1M requests + $0.0000133/GB-sec × 2 GB × 30 sec × 438k | **~$354.00** |
| S3 storage | same 21.6 TB (the data the writer produced) | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT operations | janitor's compactions: ~5 PUTs per compaction × 50 tables × 12/hr × 730 = ~2.2M | 2.2M | $0.005/1k | **$11.00** |
| S3 GET operations | janitor reads: ~10 GETs per analyze × 50 × 12 × 730 = ~4.4M | 4.4M | $0.0004/1k | **$2.00** |
| S3 LIST operations | janitor discovers: ~1 LIST per table per run | ~440k | $0.005/1k | **$2.20** |
| EventBridge (scheduled trigger) | one rule × 50 tables × 12/hr × 730 hr = 438k events | 438k | $1.00/M events | **$0.44** |
| Cross-AZ data transfer | none (Lambda + S3 same region) | 0 | — | $0 |
| **Total** | | | | **~$867/month** |

Almost identical to moonlink's instance-store path for the steady-state hot case.

---

## janitor cost (middle path — event-driven on-commit)

Once the middle path lands ([`MIDDLE_PATH.md`](MIDDLE_PATH.md)), the janitor scales with **commits**, not with time. Each commit triggers one Lambda invocation; if the trigger threshold isn't met, the invocation exits in <100 ms after a single state-file read.

| Line item | Quantity | Unit cost | Monthly |
|---|---|---|---|
| Lambda invocations (commit-driven) | 50 tables × 10 commits/min × 730 hr × 60 = 21.9M | $0.20/1M | **$4.38** |
| Lambda compute — most invocations exit in <100ms after a state-file read | average ~150ms × 21.9M × 2 GB | $0.0000133/GB-sec | **~$87.00** |
| Lambda compute — the ~10% that actually compact | average 5 sec × 2.2M × 2 GB | $0.0000133/GB-sec | **~$293.00** |
| S3 storage | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT/GET/LIST (similar to polling) | | | **~$15.00** |
| S3 event notifications → SQS → Lambda | 21.9M events | included in S3 PUT cost | $0 |
| **Total** | | | **~$896/month** |

The middle path is **roughly the same total cost** as polling because the per-event compute cost ($87 + $293) is offset by the elimination of the wasted polling-interval invocations (those ran every 5 minutes regardless of activity). The win is **freshness, not cost** — small files exist for ~30 seconds instead of ~5 minutes.

---

## Where the picture diverges: bursty workloads

The above all assume steady 720 GB/day. Real workloads aren't steady — they're bursty. Consider a workload that's hot 8 hours per day (business hours) and idle the other 16:

| Cost component | moonlink (always-on) | janitor middle path |
|---|---|---|
| Compute during 8 active hours | $330 × 8/24 = $110/month | ~$130/month (computed pro-rata) |
| Compute during 16 idle hours | $330 × 16/24 = $220/month (still paying for always-on) | **$0/month** (no commits → no invocations) |
| Storage | $497/month | $497/month |
| **Total** | **$1,047/month** | **~$642/month** |

**On bursty workloads the janitor saves ~40% over moonlink** — entirely because the compute scales to zero between events. For workloads active <50% of the time (which is most analytics workloads outside of FAANG), this gap widens further.

---

## Where the picture diverges further: small / sparse warehouses

The other interesting end of the spectrum: a warehouse with 50 tables but only sporadic writes (a few per day, most tables dormant). moonlink still needs to be always-on per active stream; the janitor compacts dormant tables once a week and skips the rest:

| Cost component | moonlink (always-on) | janitor (sparse polling) |
|---|---|---|
| Compute (50 tables, mostly idle) | ~$330/month (workers can't scale down per table) | **<$5/month** (a few invocations per day per active table) |
| Storage | $50/month (much less data) | $50/month |
| **Total** | **~$380/month** | **~$55/month** |

**On small/sparse workloads the janitor is 7× cheaper.** This is where the serverless architecture's compounded scale-to-zero shows up.

---

## Where moonlink is unambiguously cost-cheaper

There IS a regime where moonlink wins on cost: **sustained high write volume on a small number of tables**. If every table is hot 24/7 and the write volume is high enough that the janitor would be running its compaction loop near-continuously anyway, moonlink's "buffer once at write time" is genuinely cheaper because it does the work once instead of twice (write small + read small + write big).

The crossover is approximately:

- Tables continuously hot at >50% utilization, AND
- Average commit size <1 MB, AND
- Write rate sustained >10 commits/min/table for >12 hours/day

In that regime, moonlink saves roughly 30-50% on compute by avoiding the read-everything-and-rewrite-it pass. Storage cost is identical (the table data lives in the same S3 either way). The janitor would be running its compaction loop near-continuously and paying for it.

For typical analytics workloads — bursty traffic, mixed-volume tables, mixed-update-frequency — the janitor's serverless cost model wins. For sustained high-volume CDC pipelines on a few hot tables, moonlink wins.

---

## The summary table

| Workload shape | moonlink/month | janitor middle path /month | Winner |
|---|---|---|---|
| 50 tables, steady ingest, 24/7 | ~$845 (instance store) ~$2,972 (EBS) | ~$896 | **roughly tied with instance-store path; janitor wins by 3× over EBS path** |
| 50 tables, bursty (8 hr active / 16 idle) | ~$1,047 | ~$642 | **janitor by ~40%** |
| 50 tables, sparse (mostly dormant) | ~$380 | ~$55 | **janitor by ~7×** |
| 5 hot tables, sustained high-volume CDC | ~$330 | ~$450-600 | **moonlink by ~30-50%** |
| Heterogeneous warehouse (some hot, some cold, some moonlink-managed, some not) | n/a (moonlink can't operate on tables it doesn't ingest) | works for all | **janitor (only option)** |

---

## The third axis: workload mix

The table above doesn't capture **workload mix**. Real warehouses have all three regimes simultaneously:

- A few hot streaming pipelines (where moonlink would win)
- A long tail of bursty mid-volume tables (where the janitor wins)
- A long-tail-of-the-long-tail of mostly-dormant tables (where the janitor wins by an order of magnitude)

The janitor handles all three with one binary at the lower end of the cost envelope. moonlink handles one of them very well at the higher end, but only the ones it's wired into as the writer.

This is the operational reason a mature data platform might run **both**: moonlink for the few tables where its cost premium is justified by sustained high-volume ingest, janitor for everything else.

---

## How to apply this to your own deployment

To replace these numbers with your own, replace these inputs:

| Variable | Your value goes here |
|---|---|
| Number of tables | _ |
| Average commit rate per table per minute | _ |
| Average commit size in bytes | _ |
| Active hours per day per table (1 = always on, 0 = dormant) | _ |
| Table data retention in days | _ |
| AWS region (changes EC2 + EBS rates) | _ |

The formulas:

```
moonlink_compute_per_month  = N_workers × (instance_hourly + EBS_monthly_per_GB × GB_per_worker × 16k_iops_premium)
janitor_compute_per_month   = invocations × (per_invocation_request_cost + per_invocation_GB_sec_cost)
storage_per_month            = total_data_TB × $0.023 × 1000

invocations (polling) = tables × (60 min/hr / polling_interval_min) × 730 hr
invocations (middle path) = tables × commits_per_min × 60 × 730
```

For the middle path's "exit-early" optimization, multiply the per-invocation GB-sec cost by ~0.10 for the 90% of invocations that don't compact + 1.0 for the 10% that do. The exact ratio depends on your trigger thresholds.

---

## What this analysis intentionally ignores

- **Egress costs to query engines.** Both approaches put the same data in the same S3; query engines pay the same egress regardless.
- **Engineering time to deploy and operate.** Real cost. Not measured here. moonlink saves on "I have to run the janitor" overhead but adds "I have to run an ingestion service." The wash depends on which side your team is more comfortable with.
- **Vendor lock-in cost.** moonlink's closed-source license (BSL for moonlink, Apache for pg_mooncake). The janitor is Apache-2.0 throughout. The cost of vendor lock-in is real but doesn't show up in monthly bills.
- **Cross-region replication.** Both architectures pay the same egress for CRR; the comparison is fair.
- **The 11 circuit breakers and three-tier kill switch the janitor adds.** Operationally valuable, but doesn't change the marginal compute cost.

---

## See also

- [`MIDDLE_PATH.md`](MIDDLE_PATH.md) — the architectural design for event-driven on-commit compaction
- [`MOONCAKE_COMPARISON.md`](MOONCAKE_COMPARISON.md) — full architectural comparison between moonlink and the janitor (this cost analysis is a section of that doc, broken out here for citation)
- [`TABLEFLOW_COMPARISON.md`](TABLEFLOW_COMPARISON.md) — architectural comparison vs Confluent Tableflow's compaction subsystem
- [`BENCHMARKS.md`](BENCHMARKS.md) — measured performance numbers from each janitor build phase
