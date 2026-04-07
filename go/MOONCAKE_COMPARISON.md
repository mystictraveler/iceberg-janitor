# iceberg-janitor (Go) vs. pg_mooncake / moonlink

A side-by-side architectural comparison against [pg_mooncake](https://github.com/Mooncake-Labs/pg_mooncake) and the [moonlink](https://github.com/Mooncake-Labs/moonlink) ingestion engine that powers it. **TL;DR:** these two projects are not competitors — they are **complementary solutions to two halves of the same problem**, and the architectural contrast is genuinely useful as a way to understand both.

- **Moonlink** is a *prevention* strategy. It owns the write path (Postgres CDC, Kafka, REST API) and **prevents the small-file pathology from ever happening** by buffering incoming events on NVMe, building indexes locally, and only flushing size-tuned Parquet + deletion vectors to Iceberg. Their README states the goal explicitly: *"managed ingestion engine for Apache Iceberg... without complex maintenance and compaction."* Compaction is unnecessary because the data was never small in the first place.
- **iceberg-janitor** is a *remediation* strategy. It owns no write path. It operates on Iceberg tables that already exist — written by Spark, Flink, Trino, dbt, Tableflow, or anything else — and rewrites the small files into well-sized files, expires old snapshots, cleans orphans, and rewrites manifests. It assumes the small-file pathology has already happened (because most Iceberg writers in the wild produce it) and fixes it.

You could deploy both: use moonlink for the ingestion paths you control (a Postgres CDC pipeline, a Kafka topic), and use the janitor for everything else (the Spark batch jobs, the dbt models, the legacy Flink writers). They would not collide. They operate on disjoint table populations, or moonlink-owned tables would simply not need the janitor.

---

## Moonlink's actual technique (what makes "no compaction needed" plausible)

Quoting from their README:

> Traditional ingestion tools write data and metadata files per update into Iceberg. That's fine for slow-changing data, but on real-time streams it causes: **Tiny data files** — frequent commits create thousands of small Parquet files. **Metadata explosion** — equality-deletes compound this problem.

> Moonlink minimizes write amplification and metadata churn by buffering incoming data, building indexes and caches on NVMe, and committing read-optimized files and deletion vectors to Iceberg.

The mechanism, in shape:

1. **Inserts get buffered** in an Arrow-format in-memory + NVMe layer. They are not flushed to Iceberg one-event-at-a-time. The buffer accumulates until it hits a size threshold, then writes a single size-tuned Parquet file to the Iceberg `data/` prefix.
2. **Deletes are mapped to deletion vectors** via a row-position index that lives on NVMe alongside the buffer. The index tracks "this row id in this data file is now deleted" and emits Puffin-format deletion vectors per data file. This is the V3 way to handle deletes, and it sidesteps the manifest-explosion problem that equality deletes cause in V2.
3. **Reads happen against the committed Iceberg state** — DuckDB / Spark / `pg_duckdb` / `pg_mooncake` query the standard Iceberg layout, but the layout was always small-file-free because moonlink never wrote a small file in the first place.

The clever part: **moonlink moves the buffering layer one step earlier in the pipeline.** Instead of "writer commits per event → maintainer fixes the result," it's "writer buffers per event → writer commits per batch." The maintainer never has to run because the writer never created the mess.

**This is genuinely a better approach when you control the write path.** It eliminates an entire operational concern (running maintenance) and avoids the compute cost of post-hoc compaction. The catch is that it only works for ingestion paths moonlink owns. Their explicit input sources today are:

- PostgreSQL CDC (logical replication)
- REST API (HTTP event ingestion)
- Kafka (coming soon)
- OTEL (on the roadmap)

If you're not on one of those, moonlink can't help you.

---

## Where the janitor lives architecturally

The janitor lives **after** the writer, not before it. It assumes:

- The warehouse already exists and is being written to by something
- That writer probably **does not** have moonlink-style buffering (Spark batch jobs, Flink Iceberg sinks, dbt, Trino INSERTs, Tableflow, ad-hoc PyIceberg scripts)
- The small-file pathology has therefore already manifested
- Some operator needs the table to be queryable now, not after a Spark maintenance job runs at 3am

The janitor's job is to **be the maintainer that doesn't suck for the writers you don't control**. Concretely:

- Walk the warehouse, find the broken tables
- Compute the metadata-to-data ratio (the H1 axiom — metadata bytes should never approach data bytes)
- Rewrite small files into well-sized files via streaming overwrite (today) and byte-level stitching binpack (planned)
- Expire stale snapshots (but never the ones referenced by tags or branches)
- Clean orphans (with a 7-day trust horizon and a recycle bin in case we're wrong)
- Rewrite manifests when they grow unbounded
- Run the mandatory pre-commit master check on every commit (6 of 9 invariants today: row count, schema, per-column value count, per-column null count, per-column bounds, manifest references)
- Do all of this with zero operator touch in normal operation, and stop itself via 11 circuit breakers + 3-tier kill switch when it detects runaway

---

## Side-by-side

### Architectural shape

| Concern | pg_mooncake / moonlink | iceberg-janitor (Go) |
|---|---|---|
| **What is it?** | Managed ingestion engine for Iceberg | Maintenance operator for existing Iceberg warehouses |
| **Compaction strategy** | Prevent small files at write time via NVMe buffering | Remediate small files after the fact via streaming rewrite (and planned byte-level stitching) |
| **Owns the write path?** | Yes (Postgres CDC, Kafka, REST, OTEL) | No |
| **Operates on existing warehouses?** | No — moonlink writes the table itself | Yes — drop on any S3/MinIO/GCS/Azure warehouse populated by anything |
| **Source coupling** | Postgres / Kafka / REST API / OTEL | None |
| **Buffering** | NVMe-resident Arrow buffer + row-position index | None — operates on already-committed snapshots |
| **Catalog story** | Their own / planned managed catalog integrations (Glue, Unity Catalog) | **No catalog service.** The Iceberg metadata files in object storage ARE the catalog. Discovery via blob LIST + max-version scan; commit via per-key conditional write |
| **Required infra** | Managed control plane + NVMe-equipped workers | Just the warehouse object store. Stateless serverless workload |
| **Multi-cloud** | Their cloud (managed) | gocloud.dev/blob — same binary on AWS, GCP, Azure, MinIO, local FS |
| **Open source / closed?** | Open source (BSL for moonlink, Apache for pg_mooncake) | Open source (Apache) |

### Iceberg specifics

| Concern | pg_mooncake / moonlink | iceberg-janitor (Go) |
|---|---|---|
| **Iceberg format version** | V3 with deletion vectors | V2 today (via iceberg-go), V3 parity in progress |
| **How are deletes represented?** | **Puffin deletion vectors** built from row-position indexes — moonlink's signature design choice | When the janitor writes deletes (planned, alongside V3 lands), Puffin deletion vectors via the same iceberg-go primitives |
| **Avoids equality-delete manifest explosion?** | Yes by design | Yes by avoiding equality deletes entirely; equality-delete tables are detected and warned about by the analyzer |
| **Handles late-arriving data?** | Yes via the buffer + index | Tables it operates on may have late data; the analyzer detects this via per-partition "last write" timestamps and the workload classifier marks the table as streaming-class |
| **Snapshot expiration?** | Implicit (the writer is the only writer; snapshots stay small) | Explicit `expire-snapshots` op (planned) with hard rules: never expire snapshots referenced by tags or branches; never delete data files referenced by any live snapshot; reference counting + 7-day trust horizon |
| **Orphan removal?** | Implicit (no orphans because no failed compactions) | Explicit `remove-orphan-files` op (planned) with two-phase delete: first run is dry-run + writes candidate list; second run requires `--i-have-reviewed=<run_id>`. No `--force` bypass |
| **Manifest rewrite?** | Implicit (manifests stay small because commits are batched) | Explicit `rewrite-manifests` op (planned) when manifest count exceeds the configured threshold |
| **V3 Puffin statistics** | Probably emitted by moonlink at commit time | Mergeable theta sketches as the cache; the janitor recomputes only the partitions touched by a compaction run, then merges into the existing Puffin blob |

### Operating model

| Concern | pg_mooncake / moonlink | iceberg-janitor (Go) |
|---|---|---|
| **Compaction trigger** | n/a — there is no compaction | Adaptive feedback loop, per-table workload classifier (streaming / batch / slow-changing / dormant), per-class trigger thresholds |
| **Self-tuning?** | Buffer thresholds tunable; otherwise managed | Yes — workload classification, adaptive tier dispatch, feedback loop, all without operator intervention in normal operation |
| **Operator workflow** | Configure ingest pipeline; monitor cluster | `analyze`, `discover`, `compact` (CLI or API); the janitor runs on its own otherwise |
| **Failure handling** | Managed by moonlink's runtime | 11 circuit breakers (cooldown, loop detection, metadata budget, effectiveness floor, expire-first ordering, manifest-rewrite-first ordering, daily byte budget, consecutive failure pause, lifetime rewrite ratio, recursion guard, ROI estimate); 3-tier kill switch (per-table self-pause, warehouse self-pause, operator panic button) |
| **Master check on writes** | Trusted by construction (the writer is moonlink itself) | Mandatory non-bypassable: row count, schema, per-column value/null counts, bounds presence, manifest reference existence — committed to the Iceberg snapshot summary so it's auditable forever |
| **Audit trail** | Whatever moonlink emits | Four stacking layers: snapshot summary properties (table-internal, immutable, queryable forever via `<table>.snapshots`), `_janitor/results/<run_id>.json` per invocation, cloud-native audit logs (CloudTrail / GCS / Azure), operator control objects with `requested_by` |

### Cost model

| Concern | pg_mooncake / moonlink | iceberg-janitor (Go) |
|---|---|---|
| **Compute** | Continuously running ingestion workers (the buffer + index need to be hot) | Zero idle cost — serverless cold-start <200ms; pays only for the actual maintenance compute |
| **Storage** | Object store for Iceberg + NVMe for the buffer/index | Object store only (warehouse + `_janitor/` prefix; few KB per table per day) |
| **Operational** | Managed control plane ↔ self-hosted | Self-hosted only (the binary is the whole thing) |
| **At idle** | Workers stay running | Nothing runs |
| **At burst** | Workers process the burst | Knative / Lambda / Fargate scale up, run, exit |

---

## Cost analysis: NVMe is not cheap, neither is continuous compute

Architectural elegance is one thing. Operational dollars are another. Here's the cost comparison for both approaches against a representative workload, with public AWS pricing (as of early 2026) so the numbers are reproducible.

### The reference workload

A real-time analytics use case typical for both projects:

| Parameter | Value |
|---|---|
| Tables monitored | 50 |
| Writers per table | 1 (Postgres CDC, Kafka, etc.) |
| Commits per minute per table | 10 (every 6 seconds, typical Flink/CDC checkpoint cadence) |
| Bytes per commit | 1 MB (small batch) |
| Total ingest rate | 50 tables × 10 commits/min × 1 MB = 500 MB/min = ~30 GB/hr = ~720 GB/day |
| Active retention | 30 days = ~21.6 TB of data, plus metadata |
| Read pattern | Continuous (queries every minute) |

### moonlink cost (rough estimate)

moonlink's value prop is "buffer + index on NVMe, then flush size-tuned Parquet to Iceberg." That requires **at least one always-on worker per active table**, sized to hold the buffer + index in memory + NVMe. The published moonlink architecture diagram shows the buffer, cache, and index all NVMe-resident.

To run moonlink yourself (self-hosted), the line items are roughly:

| Line item | AWS SKU | Quantity | Unit cost | Monthly |
|---|---|---|---|---|
| Compute (workers, always-on) | EC2 `r6gd.xlarge` (4 vCPU, 32 GB RAM, 237 GB NVMe instance store) | 2 instances (HA) | $0.226/hr × 730 hr | **$330.00** |
| OR alternatively NVMe via EBS | `io2 Block Express` provisioned IOPS SSD | 500 GB × 2 instances | $0.125/GB-month + $0.065/IOPS-month × 16k IOPS | **~$2,165.00** |
| NVMe local instance store | included in `r6gd` SKU | included | $0 | $0 |
| Iceberg storage in S3 | S3 Standard | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT operations | (one per moonlink flush, ~1/min/table = 50/min) | 50/min × 60 × 730 = ~2.2M | $0.005/1k | **$11.00** |
| S3 GET operations (queries) | reasonable estimate | ~10M | $0.0004/1k | **$4.00** |
| Cross-AZ data transfer (HA) | between the two workers | ~10 GB/day | $0.01/GB | **$3.00** |
| **Total (instance store NVMe path)** | | | | **~$845/month** |
| **Total (provisioned-IOPS EBS NVMe path)** | | | | **~$2,680/month** |

The wide range comes from one design decision: do you use **instance store NVMe** (the local SSD that comes with `i3`/`r6gd`/`m5d`-family instances) or **provisioned-IOPS EBS** (network-attached but durable across instance restarts)?

- **Instance store** is cheap (`r6gd.xlarge` = $0.226/hr ≈ $165/month per instance, NVMe included). The catch: it is **ephemeral** — if the instance terminates, the buffer + index are gone. moonlink would need to either (a) replay from the source on restart (Kafka offsets, Postgres LSN) or (b) checkpoint state to durable storage periodically. Both add complexity but are tractable.
- **Provisioned-IOPS EBS** is durable but ~10× more expensive at the IOPS levels needed for a hot ingestion buffer. ~$2,165/month for 1 TB across 2 instances at 16k IOPS.

A managed moonlink offering on Mooncake's own cloud presumably builds in some markup over the instance-store path, so the realistic managed price is probably $1,200-$2,500/month for this workload depending on how much HA + SLA is included.

**Either way, the cost is *roughly fixed* with respect to load.** If the workload is bursty — say the 50 tables are hot for 8 hours per day and idle for 16 — moonlink still pays the always-on bill. The buffer + index can't scale to zero because they hold ingestion-critical state.

### iceberg-janitor cost (background remediation, polling mode — today)

Same workload, same 50 tables, same 720 GB/day ingest. The janitor doesn't run continuously; it runs on a schedule (or on commit events when on-commit compaction lands). For polling-mode compaction once per 5 minutes:

| Line item | AWS SKU | Quantity | Unit cost | Monthly |
|---|---|---|---|---|
| Lambda invocations | `arm64`, 2048 MB, ~30 sec average (50 tables × 12 runs/hour × 730 hr = 438k invocations) | 438,000 | $0.20/1M requests + $0.0000133/GB-sec × 2 GB × 30 sec × 438k | **~$354.00** |
| S3 storage | same 21.6 TB (the data the writer produced; janitor doesn't add to it net of compaction) | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT operations | janitor's compactions: ~5 PUTs per compaction × 50 tables × 12/hr × 730 = ~2.2M | 2.2M | $0.005/1k | **$11.00** |
| S3 GET operations | janitor reads: ~10 GETs per analyze × 50 × 12 × 730 = ~4.4M | 4.4M | $0.0004/1k | **$2.00** |
| S3 LIST operations | janitor discovers: ~1 LIST per table per run | ~440k | $0.005/1k | **$2.20** |
| EventBridge (scheduled trigger) | one rule × 50 tables × 12/hr × 730 hr = 438k events | 438k | $1.00/M events | **$0.44** |
| Cross-AZ data transfer | none (Lambda + S3 same region) | 0 | — | $0 |
| **Total** | | | | **~$867/month** |

**Almost identical to the moonlink instance-store path** for the steady-state polling case. Where the cost picture diverges:

### iceberg-janitor cost (with on-commit compaction, planned)

Once on-commit compaction lands (task #16), the janitor scales with **commits**, not with time. Each commit triggers one Lambda invocation; if the trigger threshold isn't met, the invocation exits in <100 ms after a single state read. Real numbers:

| Line item | Quantity | Unit cost | Monthly |
|---|---|---|---|
| Lambda invocations (commit-driven) | 50 tables × 10 commits/min × 730 hr × 60 = 21.9M | $0.20/1M | **$4.38** |
| Lambda compute (most invocations exit in <100ms after a state-file read) | average ~150ms × 21.9M × 2 GB | $0.0000133/GB-sec | **~$87.00** |
| Lambda compute for the ~10% of invocations that actually compact | average 5 sec × 2.2M × 2 GB | $0.0000133/GB-sec | **~$293.00** |
| S3 storage | 21.6 TB | $0.023/GB-month | **$497.00** |
| S3 PUT/GET/LIST (similar to polling) | | | **~$15.00** |
| S3 event notifications → SQS → Lambda | 21.9M events | included in S3 PUT cost | $0 |
| **Total** | | | **~$896/month** |

The on-commit version is **roughly the same total cost** as the polling version because the per-event compute cost ($87 + $293) is offset by the elimination of the 5-minute polling-interval invocations that did nothing. The win is **freshness, not cost** — the time window during which small files exist drops from ~5 minutes to ~30 seconds.

### iceberg-janitor cost (the bursty workload that's actually common)

The above assumes a steady 720 GB/day. Real workloads aren't steady — they're bursty. Consider a workload that's hot 8 hours per day (business hours) and idle the other 16:

| Cost component | moonlink (always-on) | janitor (on-commit) |
|---|---|---|
| Compute during 8 active hours | $330 × 8/24 = $110/month | ~$130/month (computed pro-rata from above) |
| Compute during 16 idle hours | $330 × 16/24 = $220/month (still paying for always-on instances) | **$0/month** (no commits → no invocations) |
| Storage | $497/month | $497/month |
| **Total** | **$1,047/month** | **~$642/month** |

**On bursty workloads the janitor saves ~40% over moonlink** — entirely because the compute scales to zero between events. For workloads that are active <50% of the time (which is most analytics workloads outside of FAANG), this gap widens further.

### iceberg-janitor cost (small warehouse, infrequent maintenance)

The other interesting end of the spectrum: a warehouse with 50 tables but only sporadic writes (a few per day, most tables dormant). moonlink still needs to be always-on per active stream, but the janitor can compact dormant tables once a week and skip the rest:

| Cost component | moonlink (always-on) | janitor (sparse polling) |
|---|---|---|
| Compute (50 tables, mostly idle) | ~$330/month (workers don't know to scale down per table) | **<$5/month** (a few invocations per day per active table) |
| Storage | $50/month (much less data) | $50/month |
| **Total** | **~$380/month** | **~$55/month** |

**On small/sparse workloads the janitor is 7× cheaper.** This is where the serverless architecture's compounded scale-to-zero shows up.

### When moonlink is unambiguously cost-cheaper

There IS a regime where moonlink wins on cost: **sustained high write volume on a small number of tables**. If every table is hot 24/7 and the write volume is high enough that the janitor's compaction compute would be running constantly anyway, moonlink's "buffer once at write time" is genuinely cheaper because it does the work once instead of twice (write small + read small + write big).

The crossover is approximately:

- Tables continuously hot at >50% utilization, AND
- Average commit size <1 MB, AND
- Write rate sustained >10 commits/min/table for >12 hours/day

In that regime, moonlink saves roughly 30-50% on compute by avoiding the read-everything-and-rewrite-it pass. Storage cost is identical (the table data lives in the same S3 either way). The janitor would be running its compaction loop near-continuously and paying for it.

For typical analytics workloads — bursty traffic, mixed-volume tables, mixed-update-frequency — the janitor's serverless cost model wins. For sustained high-volume CDC pipelines on a few hot tables, moonlink wins.

### Cost summary (the table that matters)

| Workload shape | moonlink/month | janitor/month | Winner |
|---|---|---|---|
| 50 tables, steady ingest, 24/7 | ~$845 (instance store) ~$2,680 (EBS) | ~$867 | **roughly tied** with instance-store moonlink; janitor wins by 3× over EBS path |
| 50 tables, bursty (8 hr active / 16 idle) | ~$1,047 | ~$642 | **janitor by ~40%** |
| 50 tables, sparse (mostly dormant) | ~$380 | ~$55 | **janitor by ~7×** |
| 5 hot tables, sustained high-volume CDC | ~$330 | ~$450-600 | **moonlink by ~30-50%** |
| Heterogeneous warehouse (some hot, some cold, some moonlink-managed, some not) | n/a (moonlink can't operate on tables it doesn't ingest) | works | **janitor** |

### The honest takeaway

NVMe is not cheap. neither is continuous compute. The dollar comparison depends entirely on **what fraction of the time your workload is actually active**. moonlink's cost is roughly fixed; the janitor's cost scales with work. For workloads that are active <50% of the time, the janitor's serverless model wins on cost regardless of the architectural elegance argument. For workloads that are active >75% of the time on a small number of hot tables, moonlink's prevent-at-write avoids the read-rewrite double pass and wins.

The third axis the table doesn't capture: **the workload mix**. Real warehouses have all three regimes simultaneously. A few hot streaming pipelines (where moonlink would win), a long tail of bursty mid-volume tables (where the janitor wins), and a long-tail-of-the-long-tail of mostly-dormant tables (where the janitor wins by an order of magnitude). The janitor handles all three with one binary at the lower end of the cost envelope. moonlink handles one of them very well at the higher end, but only the ones it's wired into as the writer.

This is the operational reason a mature data platform might run both: moonlink for the few tables where its cost premium is justified by sustained high-volume ingest, janitor for everything else.

## When to use which

This is the practical question. The honest answer:

| Scenario | Right tool |
|---|---|
| New Postgres CDC pipeline → Iceberg, you control it end to end | **moonlink**. The buffer-then-flush strategy is genuinely simpler than running maintenance. Set up moonlink and forget about compaction for that pipeline. |
| New Kafka → Iceberg pipeline (when moonlink ships Kafka) | **moonlink**. Same reason. |
| You have a Spark batch job writing to Iceberg | **janitor**. Spark doesn't know about moonlink's buffering. The output has small files. Run the janitor against the resulting warehouse. |
| You have a Flink streaming sink writing to Iceberg | **janitor**. Same reason. |
| You have a dbt model materializing to Iceberg | **janitor**. |
| You have a Trino INSERT-as-SELECT pipeline | **janitor**. |
| You have Confluent Tableflow ingesting Kafka → Iceberg | **either**. Tableflow's compaction is its own subsystem (see `TABLEFLOW_COMPARISON.md`); the janitor or moonlink can both operate against Tableflow-produced tables, but Tableflow already does compaction internally. The janitor is a backup if Tableflow's compaction is opaque or insufficient. |
| You have a heterogeneous warehouse with multiple writers, only some of which you control | **both**. moonlink for the ingestion paths you own; janitor for everything else. They operate on disjoint table populations. |
| You want a fully managed end-to-end ingestion + storage + compaction story | **moonlink**. |
| You want to drop a maintenance tool on an existing warehouse without changing how data gets in | **janitor**. |
| You want sub-second freshness on a small number of high-value tables | **moonlink** (the buffering layer is on the latency hot path; janitor isn't) |
| You want to maintain thousands of tables across many warehouses with zero operator headcount per warehouse | **janitor** (designed for this at the workload-classifier + autopilot level) |
| You want catalog-less, multi-cloud, serverless compaction with mandatory pre-commit verification | **janitor** (those are explicit design goals; moonlink has its own opinions) |

---

## What we can learn from moonlink

A few moonlink design choices the janitor should respect or steal:

### 1. **Deletion vectors over equality deletes** — adopted

The janitor's design plan (decision #14, V3 parity) explicitly targets Puffin deletion vectors as the V3 way to handle deletes. Equality deletes are a known cause of manifest explosion and the janitor's analyzer flags them as a CRITICAL pathology when it sees them in the wild (alongside the H1 metadata-to-data ratio check).

### 2. **Prevention beats remediation when you can prevent** — acknowledged

Where the janitor controls the write path (e.g., its own compaction commits), it produces well-sized files at the target size. The streaming compaction (already shipped) bounds memory; the planned byte-level stitching binpack (next session) bounds CPU; both are about producing an output that doesn't need to be re-compacted next week. The janitor's CB4 effectiveness floor (refuse to re-compact a partition where the previous compaction had bytes_out / bytes_in > 0.85) is the explicit "stop creating churn" rule.

### 3. **The buffer-and-batch pattern is a good idea for ingestion** — out of scope but acknowledged

The janitor is not an ingestion engine and will never be. But the design explicitly **recommends moonlink (or equivalent) for new ingestion paths** rather than trying to fix bad ingestion via compaction alone. The right architecture is: prevent on the writer side where you can; remediate on the maintenance side where you can't.

### 4. **NVMe-resident state is a write-side optimization, not a maintenance-side one**

moonlink uses NVMe to keep the buffer + index hot. The janitor explicitly does NOT keep anything hot — its design goal is sub-200ms cold start from a serverless invocation, with state in object storage (the `_janitor/` prefix). These are different architectural niches: moonlink optimizes for sustained low-latency ingestion; janitor optimizes for zero idle cost between maintenance events.

### 5. **"Without complex maintenance and compaction" is a goal worth holding writers to**

The janitor's existence is essentially a confession that not every Iceberg writer follows moonlink's discipline. If they did, the janitor would be much simpler — it would only need to handle expire and orphan removal, not compaction. The janitor's `analyze` command output is partially aimed at making the writer's badness *visible* to operators: the H1 ratio, the small-file ratio, the manifest count, the snapshot count are all observable metrics that an operator can use to push back on the upstream writer. *"Your Spark job is creating 10 GB of metadata for 100 GB of data. Either fix the writer or run the janitor — but the writer is the root cause."*

---

## One thing the janitor does that moonlink doesn't (and shouldn't)

**Operate on tables it didn't write.** moonlink's whole architectural premise is that it owns the write path. The janitor's premise is the opposite: it operates on whatever it finds. These are not competing premises; they're targeting different deployment realities.

If your data lives in moonlink-managed Iceberg tables, run moonlink. If your data lives anywhere else, run the janitor. If your data lives in both (heterogeneous warehouse), run both — they will not collide because they operate on disjoint tables.

---

## The honest summary

**moonlink is the better answer** for the slice of the problem where you control the write path and you're starting fresh. The buffer-and-batch pattern is genuinely simpler than "let small files happen and then fix them," and the elimination of equality deletes via deletion vectors is a strong design choice the janitor is following.

**iceberg-janitor is the better answer** for everywhere else: brownfield warehouses, multi-writer scenarios, cases where the upstream writer is Spark / Flink / dbt / Trino / Tableflow / etc., cases where you want a stateless serverless tool that costs nothing at idle, and cases where you want mandatory pre-commit verification with the audit record committed to the table itself.

They are not competing for the same operator's attention. They are answering different operational questions:

- moonlink: *"How do I get data into Iceberg without creating a maintenance problem?"*
- janitor: *"How do I fix the maintenance problem on an Iceberg warehouse I already have?"*

A mature data platform might run both.
