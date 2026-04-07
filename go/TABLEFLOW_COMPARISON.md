# iceberg-janitor (Go) vs. Confluent Tableflow compaction

A side-by-side comparison of the Go janitor's compaction subsystem
against Confluent Tableflow's compaction subsystem. This is **NOT** a
fair comparison of the whole products — Tableflow is a vertical stack
that also handles Kafka ingestion, schema registry, exactly-once
semantics, and a managed control plane. We're only comparing the
*compaction* part, because that's the part the janitor exists to do.

If you want a tl;dr: a major vendor sells a managed compaction service
behind a per-GB price tag and a closed control plane. We do the same
job as a few thousand lines of Go that runs in Lambda for free at idle.
The janitor isn't trying to replace Tableflow as a product — it's
trying to make Tableflow's *compaction subsystem* something you can
own, audit, and run anywhere.

## Architecture

| Concern | Confluent Tableflow | iceberg-janitor (Go) |
|---|---|---|
| **Scope** | Vertical: Kafka topic → Iceberg table → managed maintenance | Horizontal: any existing Iceberg warehouse, regardless of who writes to it |
| **Source coupling** | Kafka topic + Schema Registry | None — the warehouse is the only input |
| **Catalog** | Confluent's catalog (or REST catalog), proprietary control plane | **No catalog service.** The Iceberg metadata files in object storage ARE the catalog. Discovery via blob LIST + max-version scan; commit via per-key conditional write |
| **Coordination** | Managed control plane (Confluent Cloud) | Object-store conditional writes (`If-None-Match` on S3, generation-match on GCS, ETag on Azure, `os.Link(2)` on local POSIX). No coordinator |
| **State store** | Confluent's internal state | `<warehouse>/_janitor/` JSON files (per-table, leases, control, results) — same blob backend as the table data |
| **Cloud support** | AWS first, others rolling out, single-cloud per Tableflow instance | Multi-cloud by abstraction — `gocloud.dev/blob` behind one interface, `s3blob` / `gcsblob` / `azureblob` / `fileblob` |
| **Operator surface** | Confluent Cloud UI / API + per-topic config knobs | Single Go binary; `_janitor/control/` files for paused/forced/policy overrides; CLI for ad-hoc operations. Designed for zero operator touch in normal operation |
| **Audit trail** | Confluent's logs (proprietary) | Four stacking layers, all on by construction: Iceberg snapshot summary properties (`janitor.run_id`, `janitor.action`, etc.), `_janitor/results/<run_id>.json`, cloud-native audit logs (CloudTrail / GCS / Azure), operator control objects with `requested_by` |
| **Pricing model** | Per managed GB | Compute time only. Zero idle cost in serverless; pays for actual compaction work and nothing else |
| **Source availability** | Closed source, proprietary | Open source, vendor-neutral, reproducible from `go build` |

## Compaction correctness

| Concern | Confluent Tableflow | iceberg-janitor (Go) |
|---|---|---|
| **Pre-commit verification** | Opaque to user | **Mandatory non-bypassable master check** with 9 invariants planned, 3 currently shipped (I1 row count, I2 schema, I7 manifest refs). Result is committed to the Iceberg snapshot summary so it's queryable forever via `<table>.snapshots` |
| **Atomicity primitive** | Confluent's catalog handles it server-side | Per-key conditional write on the object store. Linearized by the cloud provider (or by `link(2)` on POSIX). No external lock service. Verified by `cas_test.go` racing 32 goroutines on the same key — exactly 1 winner, 5/5 runs |
| **Concurrent writer safety** | Confluent's catalog enforces it | The conditional-write CAS works against ANY writer — janitor, Spark, Flink, Trino, an external dbt job. The janitor doesn't need to be the only writer, and an external writer doesn't need to know the janitor exists |
| **Recoverability** | Whatever Confluent's SLA covers | Recycle bin (`_janitor/recycle/<run_id>/`) + lifecycle-policy retention. Iceberg time travel still works for any committed snapshot |

## Operational shape

| Concern | Confluent Tableflow | iceberg-janitor (Go) |
|---|---|---|
| **Cold start** | Continuously running managed service (no cold start to speak of, but you pay for that) | **Sub-200ms** in Knative or Lambda (distroless Go binary, ~20MB image). Idle tables incur zero cost |
| **Trigger model** | Implicit, on data arrival | EventBridge schedule, S3/GCS/ABS event notifications, Knative PingSource, KafkaSource, or `janitor force <table>` CLI |
| **Workload differentiation** | Tunable per topic via Confluent config | Auto-classifier (streaming / batch / slow_changing / dormant) with per-class default thresholds. No operator tuning required in normal operation. Streaming class gets 5-min cadence and 60s write-buffer to avoid the writer fight |
| **Scaling model** | Confluent manages it | Three runtime tiers (Knative pod / Lambda / Fargate or Cloud Run Job or Container Apps Job) with adaptive dispatch. Per-table feedback loop converges on the right tier without operator action |
| **Failure containment** | Confluent's SRE owns it | Eleven circuit breakers (CB1–CB11) — cooldown, loop detection, metadata budget, effectiveness floor, expire-first ordering, manifest-rewrite-first ordering, daily byte budget, consecutive failure pause, lifetime rewrite ratio, recursion guard, ROI estimate. Three-tier kill switch (per-table self-pause, warehouse self-pause, operator panic button) |
| **Self-recognition of doing-too-much** | Implicit in Confluent's algorithms | Explicit: H1 metadata-to-data ratio (CB3, the "metadata must never exceed data" axiom). At >5% the janitor warns; at >10% it switches to metadata-shrinking mode and refuses further compaction; at >50% it triggers the warehouse self-pause |

## What Tableflow does that we don't (the honest part)

The janitor is NOT a product replacement for Tableflow. Things Tableflow
does that the janitor doesn't try to do:

- **Kafka → Iceberg ingestion.** Tableflow handles topic-to-table
  materialization including exactly-once semantics. The janitor
  assumes the table already exists and was written by *something*.
- **Schema Registry integration.** Tableflow connects schema evolution
  in the Kafka topic to schema evolution in the Iceberg table. The
  janitor reads whatever schema is in metadata.json and refuses to
  silently mutate it (master check I2).
- **Managed SRE.** Confluent's team owns the operational surface. With
  the janitor, your team owns it — that's the trade-off you make for
  open source and zero per-GB pricing.
- **Vendor support.** When something goes sideways at 3 AM, Confluent
  has a phone number. With the janitor, you have the runbook in
  `test/mvp/MVP.md`, the audit trail in `_janitor/results/`, and the
  source code under `pkg/`.

## What we do that Tableflow doesn't (the fun part)

- **Multi-cloud by construction.** The same Go binary runs against AWS
  S3, GCP GCS, Azure Blob, MinIO, and a local filesystem. No
  per-cloud build. No cloud lock-in. Tableflow ships per cloud.
- **No catalog service.** This is the load-bearing architectural bet.
  Tableflow has a managed catalog. Glue has a managed catalog. REST
  Catalog implementations have a managed service. Hadoop catalogs
  have `version-hint.text`. Our directory catalog uses object-store
  conditional writes as the linearization point and doesn't require a
  service or a hint file. Verified correct under 32-way concurrent
  contention by `cas_test.go`.
- **Audit trail in the table itself.** Every janitor commit writes a
  `Verification` record into the Iceberg snapshot summary. That means
  `SELECT * FROM <table>.snapshots WHERE summary['janitor.run_id'] = '...'`
  is a valid SQL query that returns the full provenance of every
  compaction the janitor ever did, queryable from any Iceberg engine.
  No external observability stack required.
- **Operates on existing warehouses.** You can drop the janitor onto a
  warehouse populated by Spark, Flink, Trino, dbt, or — yes —
  Tableflow itself, and it'll start maintaining the tables. There's
  no "you must use our writer" lock-in.
- **Open source.** You can read the entire compaction algorithm
  source-code in an afternoon. You can fork it. You can audit the
  master check yourself. None of that is true of Tableflow.
- **Zero idle cost.** A janitor that's not actively maintaining
  anything costs nothing. Tableflow charges for managed capacity even
  when nothing's happening.
- **The "metadata must never exceed data" axiom is enforced as code.**
  CB3 / H1 in `pkg/analyzer` is a literal Iceberg health check that
  refuses to do compaction on a table whose metadata has grown past
  the threshold, and self-heals via expire+manifest-rewrite. We
  haven't seen this stated as an enforced invariant in any other
  Iceberg compactor.

## The serious takeaway

Tableflow's compaction is a feature of a much bigger product. The
janitor is the open-source equivalent of *just that feature*, designed
to be droppable onto any existing Iceberg warehouse regardless of how
the data got there. If you're already a Confluent customer and you
want a managed end-to-end pipeline, Tableflow is the right choice. If
you have an existing warehouse populated by something else, want
multi-cloud, want auditable compaction, want zero idle cost, or want to
own the algorithm — that's where this project lives.

If we end up in a benchmark shootout against Tableflow on a fair
compaction-only workload, the design bet is: **same wall-clock per
compaction, dramatically lower idle cost, and a stronger correctness
story** (mandatory master check, snapshot-internal audit). Numbers TBD
when we have a Tableflow account to point at.
