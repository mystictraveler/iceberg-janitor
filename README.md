# iceberg-janitor

**Catalog-less, multi-cloud, serverless maintenance for Apache Iceberg tables, with mandatory pre-commit verification and zero operator touch in normal operation.**

The janitor solves the small-file, snapshot-explosion, and metadata-bloat problems that occur whenever streaming engines (Flink, Spark, Kafka Connect, Tableflow) continuously write to Iceberg tables. It runs as a stateless serverless workload over an existing Iceberg warehouse — drop it onto any S3, MinIO, GCS, or Azure Blob bucket holding Iceberg tables and it'll start maintaining them, no catalog service required, no managed control plane, no per-GB pricing.

This repository contains **two sibling implementations**:

| | Language | Status | Purpose |
|---|---|---|---|
| **`src/iceberg_janitor/`** | Python | Production-tested at TPC-DS scale | The original implementation. Knative-deployed, polymorphic across catalog backends (REST, Glue, Hive, SQL), with Flink as the heavy-compute escape hatch. Stays in place as the parity oracle for the Go rewrite. |
| **`go/`** | Go | MVP shipped, active development | The forward direction. Catalog-less directory catalog over `gocloud.dev/blob`, atomic conditional-write commits, mandatory pre-commit master check, designed for sub-200ms cold starts on Knative scale-to-zero and AWS Lambda. Built around a 27-decision design plan that retires Flink in favor of Lambda+Fargate (or equivalents) tiers. |

**The architectural identity is the same across both implementations.** The Python project has been audited and found to be already polymorphic across catalog backends (zero hard REST dependency); it just defaults to REST in dev. The Go project takes the architectural commitment further by removing the catalog service entirely.

---

## Table of Contents

- [The Architecture in One Picture](#the-architecture-in-one-picture)
- [What's New, What's Stable](#whats-new-whats-stable)
- [Quick Start](#quick-start)
  - [Go MVP loop (local fileblob, no Docker)](#go-mvp-loop-local-fileblob-no-docker)
  - [Go MVP loop (MinIO via Docker)](#go-mvp-loop-minio-via-docker)
  - [Python TPC-DS benchmark](#python-tpc-ds-benchmark)
- [Load-Bearing Design Decisions](#load-bearing-design-decisions)
- [The Master Check](#the-master-check)
- [Self-Recognition: How the Janitor Knows When It's Doing Too Much](#self-recognition-how-the-janitor-knows-when-its-doing-too-much)
- [Operating Principle: Zero Operator Touch in Normal Operation](#operating-principle-zero-operator-touch-in-normal-operation)
- [Comparison Against Confluent Tableflow](#comparison-against-confluent-tableflow)
- [Benchmark Results](#benchmark-results)
- [Repository Layout](#repository-layout)
- [Documentation Index](#documentation-index)
- [Evolution](#evolution)
- [License](#license)

---

## The Architecture in One Picture

```
                                ┌────────────────────────────────────────┐
                                │   Iceberg warehouse on object storage  │
                                │                                        │
                                │   <warehouse>/db.events/                │
                                │   ├── data/      (Parquet, by anyone)   │
                                │   ├── metadata/  (.json + manifests)    │
                                │   └── _janitor/                         │
                                │       ├── state/<table>.json            │
                                │       ├── leases/<table>.lease          │
                                │       ├── control/{paused,force,...}    │
                                │       ├── recycle/<run_id>/             │
                                │       └── results/<run_id>.json         │
                                └──────────────────┬─────────────────────┘
                                                   │
                                                   │ multi-cloud blob
                                                   │ (s3, gs, azblob, minio, file)
                                                   │
            ┌──────────────────────────────────────┼──────────────────────────────────────┐
            │                                      │                                      │
            ▼                                      ▼                                      ▼
   ┌──────────────────┐                 ┌──────────────────┐                 ┌──────────────────┐
   │  Knative pod /   │                 │   AWS Lambda     │                 │  Fargate /       │
   │  Cloud Run       │                 │  (Graviton)      │                 │  Cloud Run Job / │
   │                  │                 │                  │                 │  Container Apps  │
   │  warm path       │                 │  warm path       │                 │  long-running    │
   │  ~200ms cold     │                 │  ~200ms cold     │                 │  no timeout      │
   └────────┬─────────┘                 └────────┬─────────┘                 └────────┬─────────┘
            │                                    │                                    │
            └────────────────────┬───────────────┴────────────────────────────────────┘
                                 │
                                 │  same Go binary, three thin wrappers
                                 │
                                 ▼
                       ┌──────────────────────┐
                       │   pkg/janitor.Core   │
                       │                      │
                       │  ProcessTable(ctx,   │
                       │    table,            │
                       │    priorState)       │
                       │                      │
                       │  → pure function     │
                       └──────┬───────────────┘
                              │
       ┌──────────────────────┼──────────────────────┐
       │                      │                      │
       ▼                      ▼                      ▼
┌────────────┐        ┌────────────┐         ┌────────────┐
│  classify  │        │  policy +  │         │ master     │
│ (workload  │        │  feedback  │         │ check      │
│  class)    │        │ (per-class │         │ (I1..I9)   │
│            │        │  defaults) │         │ MANDATORY  │
└────────────┘        └────────────┘         └────────────┘
       │                      │                      │
       └──────────────────────┴──────────────────────┘
                              │
                              ▼
                       ┌──────────────┐
                       │  catalog.    │
                       │  AtomicCommit│
                       │              │
                       │  conditional │
                       │  write CAS   │
                       │              │
                       │  no service  │
                       └──────────────┘
```

The load-bearing claim is in the bottom-right box: **the Iceberg metadata files in the warehouse object store ARE the catalog**. There is no catalog service. There is no metastore. There is no DynamoDB / Glue / Hive Metastore / REST Catalog dependency. Discovery happens by listing object-store prefixes; the current metadata pointer is the highest-numbered `metadata/v*.metadata.json`; commit is a per-key conditional write that the cloud provider linearizes for free. Multi-writer correctness comes from the same `If-None-Match` primitive every distributed lock library on object storage uses.

---

## What's New, What's Stable

### Stable (Python implementation, `src/iceberg_janitor/`)

- **TPC-DS-validated compaction** with 15-22% query latency improvement on real multi-table joins (see [Benchmark Results](#benchmark-results))
- **Polymorphic across catalog backends** — REST Catalog, AWS Glue, Hive Metastore, SQL — verified by code inspection: every catalog construction goes through PyIceberg's generic `load_catalog()`, no hard REST coupling anywhere
- **Knative deployment** with PingSource and KafkaSource event triggers
- **Adaptive scheduling** with hot/warm/cold access classification and feedback-loop priority adjustment
- **Smart execution router** (in-process for small tables, Flink for large) with Karpenter autoscaling
- **9-endpoint REST API** mapped 1:1 to Iceberg's canonical maintenance actions
- **Multi-region strategy** documented with cost-analysis simulation (see [Multi-Region Considerations](#multi-region-considerations))

### New (Go implementation, `go/`)

The Go implementation has shipped an end-to-end MVP loop. Reproducible commands and exact numbers live at [`go/BENCHMARKS.md`](go/BENCHMARKS.md). What's working today:

- **`pkg/catalog.DirectoryCatalog`** — implements `iceberg-go`'s `Catalog` interface against any `gocloud.dev/blob` warehouse, with **no catalog service**. Discovery via blob LIST + max-version scan, commit via per-key conditional write.
- **Atomic CAS commits** — for cloud backends (s3/gcs/azblob) via `gocloud.dev/blob.WriterOptions{IfNotExist: true}`, which maps to the provider's native conditional write. For local fileblob (where gocloud.dev's `IfNotExist` has a TOCTOU race), the catalog bypasses fileblob and uses `os.Link(2)` directly, which is atomic on POSIX. Verified correct by `cas_test.go`: 32 goroutines race on the same key, exactly 1 winner, 5/5 runs.
- **CAS-conflict retry loop** — `compact` reloads the table from the new current and retries from scratch on a CAS conflict, up to 5 attempts with exponential backoff. Optimistic concurrency, no merging of in-flight work.
- **Streaming compaction** — `Scan().ToArrowRecords()` wrapped via `iter.Pull2` into an `array.RecordReader` and passed to `Transaction.Overwrite`. Peak memory is **bounded to one record batch** instead of total table size — the prerequisite for Lambda-tier compaction on tables larger than RAM.
- **Mandatory pre-commit master check** (`pkg/safety.VerifyCompactionConsistency`) with three of nine planned invariants currently shipped:
  - **I1** row count: `Σ(input rows) − Σ(deletes) == Σ(staged rows)`
  - **I2** schema by id: schema cannot silently change across a maintenance op
  - **I7** manifest references: HEAD-check every data file in the staged manifest list before commit
  - I3–I6, I8, I9 land alongside the maintenance ops that exercise them
- **`pkg/analyzer.Assess`** computing `HealthReport` with the **H1 metadata-to-data ratio axiom (CB3)**. The janitor refuses to add metadata to a table whose metadata already exceeds the configured ratio of data — and aggressively shrinks metadata via expire + manifest rewrite when the ratio is critical. This is the "metadata must never exceed data" invariant from the design plan, enforced as code.
- **`cmd/janitor-cli`** with `discover`, `analyze`, `compact` subcommands. JSON output for agent/automation use.
- **`cmd/janitor-seed`** — pure-Go fixture generator using `iceberg-go`'s `catalog/sql` with a pure-Go sqlite driver. Creates a real Iceberg table and `Append`-loops synthetic Arrow batches to simulate streaming churn. Replaces the rejected pyiceberg seed.
- **End-to-end loop verified** against both local fileblob and MinIO over docker. **DuckDB independently reads the same Iceberg table the Go janitor wrote** and returns identical results — the round-trip proof that the directory catalog is correct.
- **Three-implementation convergence**: Go writer (iceberg-go via SqlCatalog), Go reader (DirectoryCatalog), and DuckDB reader all converge on the same Iceberg table layout without any catalog service. That's the validation of the architectural bet.

### Known security caveat for the Go MVP

**Today, `janitor-cli` is a thick client.** It opens its own connection to the warehouse object store using the credentials in its environment (`AWS_ACCESS_KEY_ID`, `S3_ENDPOINT`, etc.). It does **not** call any API on a running janitor service — it IS the janitor for the duration of the command. This is fine for local development and the MVP test loop, but it has real implications for production:

- The operator running `janitor-cli compact` needs **warehouse-level read+write IAM** to reach the data and metadata files. That's a sensitive credential to give an operator.
- A panic-button operator with warehouse-write credentials on their laptop is a security surface.
- There's no rate-limiting, no quota, and no centralized audit beyond what the cloud provider's native audit logs capture.

**The intended production split** is read-only commands (`analyze`, `discover`, `status`, `history`, `inspect`, `plan`) running standalone with read-only IAM, and **mutating commands** (`compact`, `expire`, `cleanup`, `force`, `pause`, `cancel`) hitting a running `cmd/janitor-server` via an authenticated HTTP API. The server has the warehouse-write IAM; the operator has a session-scoped API token. The Python implementation already has this surface (FastAPI, 9 endpoints). The Go implementation hasn't built `cmd/janitor-server` yet — it's in the planned list below.

The architectural plan is for the CLI to become a **dual-mode** binary: if `JANITOR_API_URL` is set, the CLI dispatches mutating commands to that endpoint; otherwise it falls back to opening the warehouse directly (the current behavior, useful for laptop dev and emergency access). Read-only commands work either way. This is decision #20 in the plan ("Operator CLI as the only control plane") refined with the explicit two-mode dispatch.

### Planned (next iterations)

Tracked in [`/Users/jp/.claude/plans/async-plotting-cake.md`](/Users/jp/.claude/plans/async-plotting-cake.md), the 27-decision design plan:

- **True byte-level stitching binpack** via `parquet-go.CopyRowGroups` — column-chunk byte copy with no decode/encode. Expected ~3-10× compaction speedup. Streaming (above) is the prerequisite that's already in.
- **Master check I3–I6, I8, I9** — value counts, null counts, bounds, V3 row lineage, manifest set equality, content hash
- **`pkg/safety/circuitbreaker.go`** — eleven CB rules (cooldown, loop detection, metadata budget, effectiveness floor, daily byte budget, consecutive failure, lifetime rewrite ratio, recursion guard, ROI estimate) and the three-tier kill switch (per-table self-pause, warehouse self-pause, operator panic button)
- **`pkg/strategy/classify`** — workload classifier (`streaming` / `batch` / `slow_changing` / `dormant`) with per-class default thresholds, and the active-partition detection via time-partition + max sequence number for streaming tables
- **Adaptive tier dispatch via the feedback loop** — per-table convergence on warm vs task tier without operator tuning
- **`pkg/config`** — 12-factor env-var loader with `JANITOR_*` schema and secret-manager refs
- **Three runtime adapters** (`cmd/janitor-server`, `cmd/janitor-lambda`, `cmd/janitor-task`) over the same `pkg/janitor.Core`
- **Iceberg V3 features** — deletion vectors, row lineage, Puffin statistics files, partition stats files (the V3 statistics file system is the cache for table-level stats per the plan, not a private cache in `_janitor/state/`)
- **Java Flink jar retired** — Fargate (and Cloud Run Jobs / Container Apps Jobs / `batch/v1.Job`) covers everything that doesn't fit Lambda; distributed compute is out of scope

---

## Quick Start

### Go MVP loop (local fileblob, no Docker)

This is the smallest possible end-to-end loop. No infrastructure required beyond Go and DuckDB CLI.

```bash
# 1. Build the Go binaries.
make go-build go-test

# 2. Seed an Iceberg table at /tmp/janitor-mvp with 20 small batches × 5,000 rows.
make mvp-seed-local MVP_NUM_BATCHES=20 MVP_ROWS_PER_BATCH=5000

# 3. Discover what the janitor sees.
make mvp-discover-local

# 4. Analyze: HealthReport with the H1 metadata/data ratio check.
make mvp-analyze-local

# 5. Compact: streaming overwrite + master check + atomic CAS commit.
cd go && JANITOR_WAREHOUSE_URL=file:///tmp/janitor-mvp \
    go run ./cmd/janitor-cli compact mvp.db/events

# 6. Re-analyze: file count is now 1, ratio dropped, master check verified.
make mvp-analyze-local

# 7. DuckDB round-trip verification: independent engine reads the same table.
make mvp-query-local
```

Expected compact output:
```
compacting mvp.db/events
  before: 20 data files, 240.7 KiB, 100000 rows
  master check: PASS (I1 in=100000 out=100000  I2 schema=0  I7 refs=1/1)
  after:  1 data files, 140.5 KiB, 100000 rows
  new snapshot: 7719394904328546388
compaction complete: 20 → 1 files (20.0x reduction)
```

DuckDB round-trip:
```
┌───────────┬────────────────┬─────────────┐
│ row_count │ distinct_users │ event_types │
│   int64   │     int64      │    int64    │
├───────────┼────────────────┼─────────────┤
│    100000 │          10000 │           6 │
└───────────┴────────────────┴─────────────┘
```

### Go MVP loop (MinIO via Docker)

Same loop, against a MinIO bucket. Identical Go binaries — only the warehouse URL changes.

```bash
make mvp-up                # docker compose brings up MinIO + creates bucket
make mvp-seed              # Go seed against s3://warehouse via MinIO endpoint
make mvp-discover          # Go discover via gocloud.dev s3blob
make mvp-analyze           # CRITICAL (metadata/data ratio elevated)
cd go && JANITOR_WAREHOUSE_URL='s3://warehouse?endpoint=http://localhost:9000&s3ForcePathStyle=true&region=us-east-1' \
    AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    S3_ENDPOINT=http://localhost:9000 S3_REGION=us-east-1 \
    go run ./cmd/janitor-cli compact mvp.db/events
make mvp-analyze           # ratio dropped, file count = 1
make mvp-down              # tear everything down
```

Full runbook: [`go/test/mvp/MVP.md`](go/test/mvp/MVP.md).

### Python TPC-DS benchmark

The Python implementation's TPC-DS benchmark is the parity oracle and the historical evidence base. To reproduce:

```bash
make kind-setup-knative              # kind cluster + Knative
make docker-build && make docker-load
make dev-up                          # MinIO, REST catalog, Kafka, Flink, Karpenter
kubectl -n iceberg-janitor port-forward svc/minio 9000:9000 &
kubectl -n iceberg-janitor port-forward svc/rest-catalog 8181:8181 &
pip install -e ".[query,dev]"
NUM_BATCHES=200 pytest tests/test_tpcds_benchmark.py -v -s
```

24 TPC-DS tables with 7 streaming fact tables, 200 micro-batches each (~10M rows). Runs the 10 benchmark queries before and after compaction and prints the latency comparison. See [Benchmark Results](#benchmark-results) for what to expect.

---

## Load-Bearing Design Decisions

The full 27-entry decision log lives in the [plan file](/Users/jp/.claude/plans/async-plotting-cake.md). The seven that matter most architecturally:

| # | Decision | Rationale |
|---|---|---|
| **3** | **No catalog service** — the Iceberg metadata files in object storage ARE the catalog | Eliminates the only piece of infrastructure that isn't already needed. Multi-cloud by construction. Aligns with serverless cold-start goals. The Iceberg spec already permits this (Hadoop/directory catalog pattern) |
| **5** | **Conditional-write CAS as the metadata commit primitive** | Genuinely atomic on all four target stores (S3 since Nov 2024 GA, GCS, Azure, MinIO). No external coordinator. Same primitive every distributed lock library uses |
| **7** | **State in object storage under `_janitor/`, no KV store** | Same backend as the table data, 11-nines durability, multi-cloud, atomicity for free via CAS, zero new infrastructure. The performance gap (~30-100ms vs ~5-10ms for KV) does not matter at the per-invocation scales the janitor operates at |
| **10** | **Three runtime tiers: Knative + Lambda + Fargate (per-cloud equivalents)** | Knative for warm self-hosted/Cloud Run, Lambda for warm AWS, Fargate / Cloud Run Job / Container Apps Job for the long tail. One Go binary, three thin entrypoint wrappers |
| **11** | **Java Flink jar retired** | Fargate covers everything that doesn't fit Lambda. ZOrder and very large rewrites still work on a single Fargate task. Removes a whole language and a cluster from the operational story |
| **24** | **Mandatory non-bypassable master check before every commit** | Strongest single safety primitive available without re-reading data. Catches lost rows, duplicated rows, schema drift, dangling manifest entries. `--force` cannot disable safety checks |
| **25** | **Cloud-provider durability is the trust floor** | Sub-billionth-percent storage loss events are explicitly out of scope. The janitor trusts the same 11-nines durability that the table data already trusts |

---

## The Master Check

Every maintenance op runs `pkg/safety.VerifyCompactionConsistency` **before** the catalog commit. The function returns a structured `Verification` record on success and an error on failure. **Failure aborts the commit, releases the lease, leaves the new files as orphans (cleanable on the next orphan run), and writes a fatal-level structured log.** There is no `--force` bypass.

The nine planned invariants:

| ID | Invariant | Cost | Catches | Status |
|----|-----------|------|---------|--------|
| **I1** | `Σ(in.row_counts) − Σ(applied_DV.cardinality) == Σ(out.row_counts)` | free | Lost rows, duplicated rows, DV bugs | ✅ Shipped |
| **I2** | `in.schema_by_id == out.schema_by_id` | free | Silent schema drift | ✅ Shipped |
| **I3** | per-column value count match | free | Per-column row loss | Planned |
| **I4** | per-column null count match | free | Null/non-null drift | Planned |
| **I5** | per-column bounds (output ⊆ input) | free | Out-of-range encoding bugs | Planned |
| **I6** | V3 row lineage `_row_id` min/max preserved | free | Lost or duplicated row identity | Planned |
| **I7** | `∀f ∈ new_manifest_list: blob.Stat(f) succeeds` | N HEADs | Manifests pointing at non-existent files | ✅ Shipped |
| **I8** | manifest set equality (manifest-rewrite only) | free | Regrouping silently dropping/adding files | Planned |
| **I9** | content hash (stitching only) | free in stitching | Byte-level corruption during column-chunk copy | Planned |

The verification record is committed to TWO places on success:
1. The Iceberg snapshot summary (table-internal, immutable, queryable forever via `<table>.snapshots`)
2. `<warehouse>/_janitor/results/<run_id>.json` (cross-table query path via `janitor history`)

This means **the audit trail is the table itself**: `SELECT * FROM mytable.snapshots` shows the entire janitor history with full provenance — which run, which action, which actor, which check passed, how many files in/out.

---

## Self-Recognition: How the Janitor Knows When It's Doing Too Much

The user's instinct is correct: the **metadata-to-data ratio is the single most diagnostic Iceberg health metric**, and the janitor must enforce it on itself because every action it takes adds metadata. This is **CB3 / H1** in the design plan:

- Compute `metadata_bytes / data_bytes` on every analyze pass
- **At >5%** (yellow): warn, log, emit Prometheus metric
- **At >10%** (red): refuse compaction; switch to **metadata-shrinking mode** (expire → manifest rewrite → stats consolidation in that order) until the ratio drops back below 4% (1% hysteresis)
- **At >50%** (emergency): write `_janitor/control/EMERGENCY_STOP` and page the operator. The table is structurally broken and the janitor cannot safely repair it without human review

This is one of eleven circuit breakers (CB1–CB11) tracked in the plan that detect runaway and pause work autonomously. The full set:

| Rule | Catches |
|---|---|
| CB1 cooldown | Writer fight (janitor recompacts faster than the writer commits) |
| CB2 loop detection | Compacting back to a previously-seen state (no progress) |
| CB3 metadata budget | **The metadata-to-data axiom** |
| CB4 effectiveness floor | Diminishing-returns churn |
| CB5 expire-first ordering | Snapshot bloat |
| CB6 manifest-rewrite-first ordering | Manifest explosion |
| CB7 daily byte budget | Effort/benefit inversion |
| CB8 consecutive failure pause | Zombie work — repeated retries on a broken table |
| CB9 lifetime rewrite ratio | "We've rewritten this table 20× for no reason" canary |
| CB10 recursion guard | Recursive maintenance triggering itself |
| CB11 ROI estimate | Compaction cost exceeds query benefit |

Plus the **three-tier kill switch**:

| Tier | Trigger | Action |
|---|---|---|
| **T1 self-pause one table** | per-table circuit breaker trips | Write `_janitor/control/paused/<table_uuid>`; future runs skip this table |
| **T2 self-pause warehouse** | warehouse-wide circuit breaker trips | Write `_janitor/control/paused.json` with `paused_by: janitor-circuit-breaker`; all future runs exit on preflight |
| **T3 operator panic** | `janitor panic` from the CLI | Atomically write the global pause AND delete every active lease AND stop in-flight tasks (`ecs.StopTask` etc.) |

The circuit breakers and kill switch are designed; CB3 (the H1 ratio check) is partially shipped via the analyzer; the rest land alongside the orchestrator in subsequent phases.

---

## Operating Principle: Zero Operator Touch in Normal Operation

A pervasive design principle that justifies many specific decisions: **operators should not have to tune the janitor in normal operation. The janitor self-tunes, self-heals, and self-protects. Operator involvement is reserved for runbook-documented exceptional cases (failures, broken tables, force overrides) and for initial deployment.**

Concretely:

- **No per-table policy YAML required** for the common case. The auto-classifier (workload class), the adaptive feedback loop (tier dispatch and effectiveness), and per-class default thresholds together cover the vast majority of tables out of the box.
- **No threshold tuning** for the common case. Cooldown intervals, file-count triggers, time triggers, and daily byte budgets are all set per workload class with reasonable defaults; the feedback loop refines them per table over time.
- **Operator overrides exist** as files in `_janitor/control/` and CLI commands (`pause`, `force`, `panic`), but they are documented as **exceptional remediation paths**, not routine knobs.
- **Failure is autopilot first.** The circuit breakers detect runaway and pause tables/warehouses without operator intervention. The operator's job is to investigate and clear, not to detect.
- **The runbook** explicitly documents the cases that justify manual intervention: H1 ratio in critical/emergency, persistent CB8 consecutive failures, T2 self-pause warehouse, manual format upgrades, recycle restoration after a verification false-negative.

This is what allows the janitor to scale to thousands of tables across many warehouses without proportional operator headcount.

---

## Comparison Against Confluent Tableflow

A side-by-side architectural comparison against Confluent Tableflow's compaction subsystem lives at [`go/TABLEFLOW_COMPARISON.md`](go/TABLEFLOW_COMPARISON.md). The TL;DR:

| | Tableflow | iceberg-janitor (Go) |
|---|---|---|
| **Scope** | Vertical: Kafka topic → Iceberg table → managed maintenance | Horizontal: any existing Iceberg warehouse, regardless of who writes to it |
| **Catalog** | Confluent's catalog (proprietary control plane) | **No catalog service.** The Iceberg metadata files ARE the catalog |
| **Cloud support** | Single-cloud per Tableflow instance | Multi-cloud by abstraction (`gocloud.dev/blob` behind one interface) |
| **Pricing model** | Per managed GB | Compute time only. Zero idle cost in serverless |
| **Audit trail** | Confluent's logs (proprietary) | Four stacking layers, all on by construction; the Iceberg snapshot summary IS the audit log |
| **Source availability** | Closed source, proprietary | Open source, vendor-neutral, reproducible from `go build` |
| **Pre-commit verification** | Opaque to user | Mandatory non-bypassable master check (9 invariants) committed to the snapshot summary forever |
| **Runaway prevention** | Implicit in Confluent's algorithms | 11 explicit circuit breakers + 3-tier kill switch |

Honest about scope: Tableflow is a much bigger product that also handles Kafka ingestion, Schema Registry integration, exactly-once semantics, and managed SRE — none of which the janitor tries to do. The janitor compares against the **compaction subsystem** of Tableflow, not the whole product.

---

## Benchmark Results

### Go (live, reproducible)

Every measured number from the Go MVP — with the exact `make` command that produces it — lives at [`go/BENCHMARKS.md`](go/BENCHMARKS.md). Each row of that file is a real run of the binaries, not a projection. Sample current numbers (Run 1c — streaming compaction with 3-invariant master check):

```
Seed:    20 batches × 5,000 rows = 100,000 rows  in 53 ms
Compact: 20 → 1 files, 240.7 KiB → 140.5 KiB
         master check: PASS (I1 in=100000 out=100000  I2 schema=0  I7 refs=1/1)
DuckDB:  100000 rows / 10000 distinct users / 6 event types — identical to pre-compact
CAS:     32-goroutine race on the same key → exactly 1 winner, 5/5 runs (cas_test.go)
```

H1 metadata-to-data ratio dropped from **31.36% (CRITICAL)** to **6.38%** in a single compaction. With realistic file sizes the ratio settles well under 1%.

### Python (TPC-DS, historical)

The Python implementation has been benchmarked against TPC-DS at multiple scales. All numbers reproducible via the steps in [Quick Start → Python TPC-DS](#python-tpc-ds-benchmark).

**87K row benchmark (50 micro-batches per fact table):**

```
Query                                        Before      After     Change
---------------------------------------- ---------- ---------- ----------
q7_promo_impact (5-table join)              134.2ms     72.3ms   -46.1% faster
q25_cross_channel_returns (7-table join)    142.4ms    103.5ms   -27.4% faster
q1_top_return_customers (4-table join)       76.5ms     61.9ms   -19.1% faster
q3_brand_revenue (3-table join)              53.8ms     44.4ms   -17.5% faster
q13_demo_store_sales (6-table join)          86.4ms     72.9ms   -15.6% faster
q43_weekly_store_sales (3-table join)        54.3ms     46.1ms   -15.0% faster
TOTAL                                       858.4ms    685.6ms   -20.1%
```

**10M row benchmark (200 micro-batches per fact table):**

| Query | Before | After | Change |
|---|---|---|---|
| q1_top_return_customers (4-table join) | 245.7ms | 132.0ms | **-46.3%** |
| q19_brand_manager_zip (6-table join) | 248.2ms | 146.0ms | **-41.2%** |
| q3_brand_revenue (3-table join) | 151.1ms | 101.6ms | **-32.7%** |
| q25_cross_channel_returns (7-table join) | 400.5ms | 310.5ms | **-22.5%** |
| **TOTAL** | **1842.9ms** | **1439.5ms** | **-21.9%** |

**Compaction impact increases with data volume.** Average file size grew from ~100 KB to ~10 MB, reducing S3 API calls by 99%.

Full benchmark run logs (Python):
- [50-batch benchmark (87K rows)](https://gist.github.com/mystictraveler/190206e73d6c8cca29aac40211673b38) — 20.1% improvement, 51 → 1 files per table
- [200-batch benchmark (10M rows)](https://gist.github.com/mystictraveler/8cfbfae0bc10a26513a8b3a30ff70cf4) — 21.9% improvement, 201 → 1 files per table

The Go implementation will be benchmarked against the same TPC-DS workload as it reaches functional parity. Tracked in `go/BENCHMARKS.md`.

---

## Multi-Region Considerations

When Iceberg tables are replicated across regions, the right approach is **always compact locally, replicate metadata only**. Cross-region data transfer costs ~20× more than local compute.

| | Cost per GB |
|---|---|
| Local compaction compute | $0.001 |
| Cross-region data transfer | $0.02 |

**Recommended architecture:** Each region runs its own janitor against its own warehouse. S3 CRR (or equivalent) is scoped to `metadata/` prefixes only — schema, snapshots, table properties replicate; data files stay local. This works because Iceberg metadata is tiny (~MB) compared to data (~TB), and the janitor's directory catalog needs only the metadata files to operate.

```
Region A (us-east)                          Region B (eu-west)
┌────────────────────────────┐             ┌────────────────────────────┐
│ Kafka → Flink → S3 (data/) │             │ Kafka → Flink → S3 (data/) │
│ (own streaming writes)      │             │ (own streaming writes)      │
│                             │             │                             │
│ Janitor (compacts locally)  │             │ Janitor (compacts locally)  │
└──────────┬──────────────────┘             └──────────┬──────────────────┘
           │                                           │
    s3://warehouse-east/                        s3://warehouse-west/
    ├── metadata/ ──── CRR (metadata only) ───→ metadata/
    └── data/     ✗ NOT replicated              data/ (own files)
```

Full analysis with cost simulation: [`docs/multiregion-strategy.md`](docs/multiregion-strategy.md).

---

## Repository Layout

```
iceberg-janitor/
├── README.md                       ← you are here
│
├── src/iceberg_janitor/            ← Python implementation (production-tested)
│   ├── analyzer/                   Health assessment via PyIceberg catalog API
│   ├── policy/                     TablePolicy + evaluation engine
│   ├── strategy/                   Triggers, scheduler, access tracker, feedback loop
│   ├── execution/                  Local + Flink executors, smart router, orchestrator
│   ├── maintenance/                Compaction, expire snapshots, orphan removal
│   ├── api/                        FastAPI REST API + CloudEvents handler
│   ├── runner/                     Long-running controller, cron entrypoint
│   ├── cli/                        Click CLI
│   └── catalog.py                  Generic catalog factory (REST/Glue/Hive/SQL)
│
├── flink-jobs/                     Java FlinkCompactionJob (production today,
│                                   retired in the Go design)
│
├── go/                             ← Go implementation (MVP shipped)
│   ├── README.md                   Go-specific README
│   ├── BENCHMARKS.md               ★ Live measured numbers from each phase
│   ├── TABLEFLOW_COMPARISON.md     Architectural comparison vs Confluent Tableflow
│   ├── go.mod                      Module: github.com/mystictraveler/iceberg-janitor/go
│   ├── pkg/
│   │   ├── catalog/                Directory catalog (LoadTable + atomic CommitTable)
│   │   ├── analyzer/               HealthReport with H1 metadata-to-data axiom
│   │   ├── safety/                 Master check (I1+I2+I7 today; +I3-I9 planned)
│   │   ├── iceberg/                Thin loader over apache/iceberg-go
│   │   ├── blob/                   Multi-cloud blob abstraction (placeholder; iceberg-go's IO covers reads)
│   │   ├── policy/                 (planned) Per-table thresholds
│   │   ├── strategy/               (planned) Triggers, scheduler, access tracker, feedback, classifier
│   │   ├── maintenance/            (planned) compact, expire, orphans, manifests
│   │   ├── orchestrator/           (planned) Stateless ProcessTable function
│   │   ├── janitor/                (planned) Top-level Core entry point
│   │   ├── state/                  (planned) BlobBackend for _janitor/state/
│   │   └── config/                 (planned) 12-factor env var loader
│   ├── cmd/
│   │   ├── janitor-cli/            ★ discover, analyze, compact (today)
│   │   ├── janitor-seed/           ★ Pure-Go iceberg fixture generator
│   │   ├── janitor-server/         (planned) Knative HTTP adapter
│   │   └── janitor-lambda/         (planned) AWS Lambda adapter
│   └── test/
│       └── mvp/                    ★ docker-compose + MVP runbook
│
├── helm/                           Helm chart (Python-side, K8s + Knative)
├── manifests/dev/                  K8s manifests (MinIO, Kafka, REST catalog, Flink, Karpenter)
├── openapi/                        OpenAPI 3.1 spec for the Python REST API
├── jmeter/                         DuckDB query benchmark suite
├── scripts/                        TPC-DS schema, data generators, setup scripts
├── tests/                          Python: TPC-DS benchmark + feedback loop + policy + analyzer
└── docs/                           Research: Tableflow comparison, Flink sizing, multi-region
```

★ = ships in the current MVP and reproducible via `make`.

---

## Documentation Index

| Document | Purpose |
|---|---|
| [`README.md`](README.md) | This file: project identity, what's stable, what's new, quick start |
| [`go/README.md`](go/README.md) | Go-specific README with package layout and quick reference |
| [`go/BENCHMARKS.md`](go/BENCHMARKS.md) | **Live measured numbers from each Go build phase** with reproducible commands |
| [`go/COST_ANALYSIS.md`](go/COST_ANALYSIS.md) | **Quantitative cost analysis** vs moonlink across four representative workload shapes; AWS pricing line items so operators can re-run the math |
| [`go/MIDDLE_PATH.md`](go/MIDDLE_PATH.md) | **The architectural design that takes the best of both worlds**: event-driven on-commit compaction triggered by writer commits in real time. moonlink-class freshness without owning the writer or running NVMe |
| [`go/MOONCAKE_COMPARISON.md`](go/MOONCAKE_COMPARISON.md) | Architectural + cost comparison vs pg_mooncake / moonlink (the prevent-at-write approach) |
| [`go/TABLEFLOW_COMPARISON.md`](go/TABLEFLOW_COMPARISON.md) | Side-by-side architectural comparison vs Confluent Tableflow's compaction |
| [`go/openapi/janitor-server-v1.yaml`](go/openapi/janitor-server-v1.yaml) | OpenAPI 3.1 spec for the Go janitor-server (5 endpoints) |
| [`go/openapi/iceberg-overlap.md`](go/openapi/iceberg-overlap.md) | Endpoint-by-endpoint mapping vs the Iceberg REST Catalog spec |
| [`go/test/mvp/MVP.md`](go/test/mvp/MVP.md) | Runbook for the Go MVP test loop (local fileblob and MinIO) |
| [`/Users/jp/.claude/plans/async-plotting-cake.md`](/Users/jp/.claude/plans/async-plotting-cake.md) | The 27-decision design plan with full rationale, alternatives, tradeoffs |
| [`docs/multiregion-strategy.md`](docs/multiregion-strategy.md) | Cost analysis: local compaction vs cross-region shipping |
| [`docs/research-tableflow-comparison.md`](docs/research-tableflow-comparison.md) | Earlier research notes on Tableflow (Python era) |
| [`docs/research-iceberg-maintenance-api.md`](docs/research-iceberg-maintenance-api.md) | Gap analysis of REST Catalog spec, V3 changes |
| [`docs/flink-sizing.md`](docs/flink-sizing.md) | Flink TaskManager sizing strategy (Python-era; superseded by tier dispatch) |
| [`openapi/iceberg-janitor-api-v2.yaml`](openapi/iceberg-janitor-api-v2.yaml) | OpenAPI 3.1 spec for the Python REST API |

---

## Evolution

The architectural arc from the original Python prototype to the current state:

1. **K8s CronJob** — Simple scheduled maintenance every 15 minutes
2. **Long-running controller** — Poll-based catalog watching with trigger evaluation
3. **Knative migration** — Scale-to-zero Serving + event-driven PingSource/KafkaSource
4. **Catalog-only refactor** — Removed DuckDB/S3 dependencies from core; everything through PyIceberg's generic catalog interface
5. **Adaptive scheduling** — Access frequency tracking, heat classification, feedback loop
6. **Flink execution layer** — Smart routing: small tables in-process, large tables on Flink with Karpenter autoscaling
7. **TPC-DS validation** — 24-table benchmark proving 15-22% query improvement after compaction
8. **Multi-region strategy** — Simulated 2-region compaction; proved local compaction is 10× cheaper than cross-region shipping
9. **Go rewrite design** — 27-decision plan: catalog-less, multi-cloud, no KV store, three serverless tiers, mandatory master check, autopilot circuit breakers, Java Flink retired
10. **Go MVP** ← currently here. Directory catalog (read+write), atomic CAS, master check (I1+I2+I7), streaming compaction, end-to-end loop verified against fileblob + MinIO with DuckDB round-trip

---

## License

Apache-2.0
