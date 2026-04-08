# iceberg-janitor (Go) vs. AWS Glue Iceberg Compaction

A side-by-side comparison of the Go janitor's compaction subsystem
against AWS Glue's managed Iceberg compaction (a.k.a. "Glue Data
Catalog automatic optimization", which today bundles three operations:
**compaction**, **snapshot expiration**, and **orphan file removal**).

This is **NOT** a fair comparison of the whole products. Glue is a
multi-product catalog + ETL + crawler service; we are only comparing
the *compaction subsystem* and the two adjacent maintenance ops Glue
also automates, because that's the part the janitor exists to do.

If you want a tl;dr: Glue is the right answer if you are AWS-only,
already pay for Glue Data Catalog, want zero infrastructure, and don't
care about the closed control plane or the per-DPU bill. The janitor
is the right answer if you need multi-cloud, are catalog-less by
choice, want a non-bypassable mandatory pre-commit verification, want
to own and audit the maintenance code, or want to run on bare buckets
without registering anything.

> **Note on currency.** AWS Glue's Iceberg automation has been evolving
> rapidly. The features and behaviors below reflect public AWS
> documentation as of mid-2025; specific behaviors (target file size,
> Z-order availability, expiration / vacuum coupling) may have changed
> since. Where I'm uncertain about a Glue behavior I mark it with
> _(uncertain)_. Corrections welcome.

## Architecture

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Catalog requirement** | **Mandatory.** Table must be registered in the AWS Glue Data Catalog. Bare-bucket Iceberg tables are not eligible. | **None.** Works against any Iceberg table that exists as files in any object store. The Iceberg `metadata/` directory IS the catalog. |
| **Cloud support** | AWS-only. S3 only for storage. | Multi-cloud by abstraction — `gocloud.dev/blob` behind one interface, with `s3blob` / `gcsblob` / `azureblob` / `fileblob` / MinIO backends. |
| **Coordination** | Glue catalog's `LockTable` / `CommitTable` / `UpdateTable` APIs. Server-side coordination. | Object-store conditional writes (`If-None-Match` on S3, generation-match on GCS, ETag on Azure, `os.Link(2)` on local POSIX). No coordinator. |
| **State store** | Glue catalog (proprietary). | `<warehouse>/_janitor/` JSON files (per-table state, leases, control objects, results) — same blob backend as the table data, no separate store. |
| **Multi-tenancy** | Per-AWS-account, per-region. | Per-warehouse-bucket. One janitor binary can operate against many warehouses; tables in different warehouses are fully independent. |
| **Operator surface** | AWS Glue console + AWS CLI / SDK / CloudFormation. Per-table `compaction_enabled` boolean and a few tunables. | Single Go binary with the operator CLI (`janitor-cli compact / pause / resume / status`) plus the `_janitor/control/` blob protocol for paused/forced/policy overrides. |
| **Source availability** | Closed source, proprietary. | Open source (this repo), vendor-neutral, reproducible from `go build`. |
| **Audit trail** | CloudTrail (Glue API events) + CloudWatch metrics. | Four stacking layers, all on by construction: Iceberg snapshot summary properties (`janitor.run_id`, `janitor.action`), `_janitor/results/<run_id>.json`, cloud-native audit logs (CloudTrail / GCS / Azure), operator control objects with `requested_by`. |
| **Pricing model** | DPU-hour: ~$0.44/DPU-hour (us-east-1, 2025), billed per actual compaction work, plus Glue Data Catalog request fees. | Compute time only (Knative / Lambda / Fargate). Zero idle cost in serverless; pays for actual compaction work and nothing else. No per-table or per-GB fee. |
| **Vendor lock-in** | High: requires Glue catalog AND S3 AND AWS account. Migrating off means re-registering every table. | None: you can rip out the janitor and use the same warehouse with any other tool that reads Iceberg. |

## Compaction correctness

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Pre-commit verification** | _Not publicly documented._ Glue does not publish a "master check" equivalent. The implementation relies on the underlying Spark / iceberg-java code being correct, with no published row-count / schema / manifest cross-check that gates the commit. | **Mandatory non-bypassable master check** with 9 invariants planned, 6 currently shipped: I1 row count, I2 schema by id, I3 per-column value count, I4 per-column null count, I5 column bounds presence, I7 manifest reference existence (narrowed to "files this op wrote" — see #5). Result is committed to the Iceberg snapshot summary so it's queryable forever via `<table>.snapshots`. |
| **Atomicity primitive** | Glue catalog handles it server-side via `UpdateTable` with version preconditions. | Per-key conditional write on the object store. Linearized by the cloud provider (or by `link(2)` on POSIX). No external lock service. Verified by `cas_test.go` racing 32 goroutines on the same key — exactly 1 winner, 5/5 runs. |
| **Concurrent writer safety** | Safe for writers that go through Glue catalog. Writers that bypass Glue (raw `aws s3 cp`, custom Spark with a different catalog) can corrupt the table. | The conditional-write CAS works against ANY writer — janitor, Spark, Flink, Trino, an external dbt job, raw boto3. The janitor doesn't need to be the only writer, and an external writer doesn't need to know the janitor exists. |
| **Recoverability** | Whatever AWS's SLA covers. Snapshot expiration is destructive — no janitor-side recycle. | Recycle bin (`_janitor/recycle/<run_id>/`) + lifecycle-policy retention. Iceberg time travel still works for any committed snapshot. Two-phase orphan removal (dry run, then operator approval). |
| **Failure mode visibility** | Glue console + CloudWatch metric `numberOfBytesCompacted`. Failures surface as Glue job failures with stack traces in CloudWatch logs. | Per-table state file in `_janitor/state/<uuid>.json` with `consecutive_failed_runs`, `last_errors[]`, `last_outcome`, `last_run_at`. CB8 auto-pauses tables that fail repeatedly; CB1 cools down freshly-acted-on tables. Operator inspects via `janitor-cli status <table>`. |

## Compaction strategies and target shape

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Strategies offered** | Binpack, sort, Z-order _(uncertain on whether Z-order is GA or preview)_. | **Binpack only today.** Sort and Z-order are designed but not implemented. The current implementation is the parquet-go-direct path that walks manifests, copies parquet files via `pqarrow`, and writes a single new file per partition scope. |
| **Target file size** | Per-table table property `write_target_file_size_bytes` (Iceberg-spec property; Glue honors it). Default 128 MB. | Currently no per-table policy — the target is "rewrite the whole partition into one file." Per-table targets and the stitching binpack threshold are designed (decision #13 in the plan) but not yet wired. |
| **Stitching binpack (column-chunk byte copy)** | Glue's compaction is a Spark job that decodes → re-encodes Parquet, paying CPU for re-encoding. | Designed (decision #13 — "stitching binpack as default; decode/encode as fallback") but not yet shipped. The current parquet-go-direct path decodes/re-encodes; the next iteration switches to byte-level column-chunk copy via `parquet-go.CopyRowGroups`. |
| **Skip already-large files** | _(uncertain)_ Glue's binpack is believed to skip files already at the target size, but the threshold isn't operator-tunable beyond the underlying property. | Not yet — see [#6][issue-6] for the planned implementation. The current bench shows this is the biggest single I/O reduction available. |
| **Partition scope** | Glue compacts partition-by-partition automatically as part of its scheduled run. Operator can't pick a partition. | Operator can pass `--partition col=value` to scope a single compact to one partition. Auto-selection is the planned default once the workload classifier is wired in. |
| **V3 features** (deletion vectors, row lineage, Puffin stats, partition stats, variant) | Glue supports V3 reads; V3-aware compaction is partial (deletion vectors in particular have known limitations). _(uncertain)_ | Designed mandatory (decision #14) but not shipped. iceberg-go's V3 support is itself partial; some primitives may need to be contributed upstream. |

## Trigger model

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Trigger** | Scheduled by Glue. The user does not control the schedule directly; Glue decides when to run based on internal heuristics around file count and table activity. _(uncertain about exact heuristic)_ | Two trigger families. **Polling tier**: periodic Knative PingSource / EventBridge schedule / Cloud Run scheduled job. **Event tier (planned)**: react to S3 EventBridge / GCS Pub/Sub / ABS notifications on `metadata/v*.metadata.json` writes. Plus operator-forced runs via `janitor-cli compact` or `_janitor/control/force/<uuid>.json`. |
| **On-commit compaction** | Not supported. Glue's automation is purely scheduled; you can't react to a Spark/Flink commit immediately. | Designed in `MIDDLE_PATH.md` — the on-commit dispatcher reacts to writer commit events with one small compact per commit, so the writer-fight pathology never starts. Tracked as [#3][issue-3], the dispatcher dedupe issue. |
| **Schedule control** | Limited to "enabled / disabled" per table. No cron, no per-table cadence. | Per-class default cadence (streaming = 5 min, batch = 1 hr, slow_changing = daily, dormant = weekly) with operator override per table via `_janitor/control/policy/<uuid>.yaml`. The classifier auto-detects which class a table belongs to from foreign-commit-rate signals. |
| **Trigger latency** | Hours to a day, observed. _(uncertain — Glue does not publish an SLA)_ | Polling: bounded by `MAINTENANCE_INTERVAL_SECONDS` (default 60s on the bench, 5 min in production for streaming class). Event-driven (planned): low single-digit seconds. |

## Snapshot expiration and orphan removal

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Snapshot expiration** | **Shipped.** "Optimize" job; `MaxAge` and `MaxSnapshots` properties. Tightly coupled to Glue catalog. | **Not yet shipped.** `pkg/maintenance/expire.go` is on the implementation list and was attempted in this session by a sub-agent that hit a transient API error. The directory-catalog primitives needed (snapshot drop + atomic CAS) are all in place; the work is the snapshot-retention policy + the expire-specific master check (which has different invariants from compaction). |
| **Orphan file removal** | **Shipped.** "Vacuum" job. Hard-coded 24-hour trust horizon (files newer than 24h are protected). | **Not yet shipped.** Designed (decision #18 — "trust horizon for orphan removal, default 7 days") with **two-phase mandatory dry-run**: first run writes a candidate list to `_janitor/results/<run_id>.json`, second run requires `--i-have-reviewed=<run_id>`. No `--force` bypass. Glue's "always-on, no-dry-run" is intentionally rejected as too dangerous for an autopilot. |
| **Manifest rewrite** | _(uncertain)_ Glue may rewrite manifests as part of compaction. | **Not yet shipped.** Designed; needed for the "metadata-to-data ratio" axiom (CB3 — H1) to be enforceable. |
| **Recycle bin** | None — destructive ops are immediate. | `_janitor/recycle/<run_id>/` for all destructive ops by default (decision #17). 7-day default lifecycle. `--no-recycle` opt-out for storage-cost-sensitive deployments. |

## Failure containment and self-recognition

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Circuit breakers** | None published. Glue's job framework will retry failed jobs but does not auto-pause tables that consistently fail. Operator monitors CloudWatch and disables compaction manually. | Eleven circuit breakers (CB1–CB11) designed; **CB1 (cooldown) and CB8 (consecutive failure pause) shipped this session**. CB2 loop detection, CB3 metadata-budget axiom, CB4 effectiveness floor, CB5/CB6 ordering, CB7 daily byte budget, CB9 lifetime rewrite ratio, CB10 recursion guard, CB11 ROI estimate are designed and land alongside the maintenance ops they protect. |
| **Self-pause on repeated failure** | No. | CB8 — after 3 consecutive failed runs the table is auto-paused via `_janitor/control/paused/<uuid>.json`. Operator clears with `janitor-cli resume <table>`. Verified end-to-end against MinIO this session. |
| **Cooldown between attempts** | No published mechanism. | CB1 — per-table 5-minute (default) minimum interval between maintenance attempts. Protects freshly-resumed tables from immediately re-tripping CB8. Auto-clears when the cooldown elapses; no operator action needed. |
| **Metadata-to-data ratio enforcement** | _(uncertain)_ Glue likely tracks this internally but doesn't expose it. | **Explicit invariant.** H1 axiom: `metadata_bytes / data_bytes ≤ 5%`. At >5% the janitor warns; at >10% it switches to metadata-shrinking mode and refuses further compaction; at >50% it triggers the warehouse self-pause. The `analyzer.HealthReport` reports the current ratio. |
| **Three-tier kill switch** | Manual disable via Glue console. | Per-table self-pause (CB8) → warehouse self-pause (`_janitor/control/paused-warehouse`) → operator panic button (`_janitor/control/panic`) — each level wider in scope, all encoded as control files in the warehouse. |
| **Workload classification** | None. Same policy applies to every table. | Auto-classifier (streaming / batch / slow_changing / dormant) with per-class default thresholds. Streaming class gets 5-min cadence and 60s write-buffer to avoid the writer fight. _(`pkg/strategy/classify` exists but is not yet wired into the compact path.)_ |

## Operational shape

| Concern | AWS Glue auto-compaction | iceberg-janitor (Go) |
|---|---|---|
| **Cold start** | N/A — Glue is a managed service with provisioned DPUs. Spin-up time per job is minutes. | **Sub-200 ms** in Knative or Lambda (distroless Go binary, ~20 MB image). Idle tables incur zero cost. |
| **Scaling model** | Glue auto-allocates DPUs per job within account-level limits. Operator doesn't control. | Three runtime tiers (Knative pod / Lambda / Fargate or Cloud Run Job or Container Apps Job) with adaptive dispatch. Per-table feedback loop converges on the right tier without operator action. |
| **Distributed compaction** | Yes. Glue jobs run on Spark, can scale horizontally. | No. Single-instance per compact run. The design plan (decision #11) deliberately retired Flink in favor of "use Spark/Trino directly outside the janitor" for the rare multi-TB single-snapshot rewrites. Out of scope. |
| **Per-table policy** | Table properties on the Iceberg table. | `_janitor/control/policy/<uuid>.yaml` (planned). Today: per-class defaults. |
| **Observability** | CloudWatch metrics (`numberOfBytesCompacted`, etc.), CloudWatch logs, Glue job history. | Per-run `_janitor/results/<run_id>.json` + Iceberg snapshot summary properties + structured `slog` JSON output to stdout. Prometheus exporter planned in `internal/metrics`. |

## Cost comparison (rough)

These numbers are illustrative; actual costs depend heavily on table
size, write rate, and AWS pricing changes.

**Workload:** 100 Iceberg tables, average 10 GB each, ~5 commits/min
across all tables, retention 30 days.

| | AWS Glue auto-compaction | iceberg-janitor on Lambda |
|---|---|---|
| **Compute** | ~0.5 DPU-hours per table per day × 100 tables × 30 days = 1500 DPU-hours/month × $0.44 = **~$660/month** | ~0.001 Lambda-hours per compact × 100 tables × 24 compacts/day × 30 days = ~72 Lambda-hours × ~$0.20/hour = **~$14/month** |
| **Catalog cost** | Glue Data Catalog: 100 tables × $1/100k objects × ~10 metadata.json updates/day × 30 days = ~$3/month | $0 — no catalog service |
| **Storage** | $0 (compaction is in-place) | $0 (compaction is in-place) + ~$0.10/month for `_janitor/` blob state (negligible) |
| **Operator cost** | Configure once, monitor CloudWatch | Deploy once (Knative/Lambda template), monitor `_janitor/state/` and CB pause files |
| **Total** | **~$663/month** | **~$14/month** |

The 50× compute cost ratio is the headline. Whether it's worth saving
depends on whether the operator team can afford the deployment +
monitoring overhead of running the janitor themselves vs. the click-to-enable
convenience of Glue.

For a single small warehouse on AWS, Glue is the right answer. For a
large multi-cloud or self-hosted deployment with hundreds or thousands
of tables, the cost ratio becomes the dominant factor and the janitor's
self-host model wins.

## What Glue does that we don't (the honest part)

The janitor is **NOT** a product replacement for Glue. Things Glue does
that the janitor doesn't try to do:

- **Click-to-enable in the AWS console.** The janitor requires
  deploying a runtime tier (Knative, Lambda, Fargate). Glue is one
  toggle in the catalog UI.
- **AWS-native IAM integration.** Glue uses Glue service roles and
  table-level IAM. The janitor inherits whatever IAM the runtime tier
  has — operationally fine for most setups but you have to wire it
  yourself.
- **Distributed Spark compaction for multi-TB single-snapshot
  rewrites.** Glue can throw 100+ DPUs at one table. The janitor's
  Fargate tier maxes out at 16 vCPU / 120 GB. The design plan
  explicitly rejects distributed compute as out of scope.
- **Sort and Z-order strategies.** Designed in the janitor but not
  yet shipped.
- **Snapshot expiration shipped today.** Glue has it; the janitor's
  `pkg/maintenance/expire.go` is queued but not yet implemented.
- **Orphan file removal shipped today.** Same.
- **CloudWatch native dashboards.** The janitor exports Prometheus
  metrics via the `internal/metrics` package (skeleton today), but
  CloudWatch wiring is operator work.
- **Glue Data Catalog.** Glue is also the catalog. The janitor
  intentionally has no catalog at all and is incompatible with
  workflows that rely on Glue for table discovery (Athena, EMR, etc.
  if you're using Glue catalog mode).
- **Years of production track record on petabyte-scale workloads.**
  The janitor is at MVP. Glue's compaction has been in GA since 2023
  and is in the critical path of many production data lakes.

## What we do that Glue doesn't

- **Multi-cloud (S3, GCS, Azure Blob, MinIO, on-prem POSIX) by
  construction.** Same binary, different env vars.
- **Catalog-less.** Run against bare buckets. Don't register your
  tables anywhere.
- **Mandatory non-bypassable master check.** I1–I9 invariants
  computed against the staged transaction *before* commit. Result
  embedded in the Iceberg snapshot summary forever. Glue's verification
  approach is opaque; there is no published equivalent.
- **Eleven circuit breakers (CB1–CB11)** with explicit, code-visible,
  operator-readable failure containment. Glue has none beyond
  job-level retry. CB1 cooldown and CB8 consecutive failure pause
  shipped; the rest are designed.
- **Two-phase orphan removal with mandatory dry-run.** Glue's
  vacuum is single-phase and immediate. The janitor refuses to
  delete files without an operator-acknowledged candidate list.
- **Recycle bin (`_janitor/recycle/`) for all destructive ops.**
  7-day default retention, lifecycle-policy auto-cleanup.
  Glue has nothing equivalent.
- **Operator-readable state in object store.** Every piece of janitor
  state — pause files, run results, recycle bin contents, control
  objects — lives as JSON files at well-known keys in the warehouse.
  Operator inspects with `aws s3 ls` / `gcloud storage ls` / `az
  storage blob list` regardless of whether the janitor is running.
- **Open source, vendor-neutral.** You can fork it, audit it, run it
  anywhere, replace it without rewriting your tables.
- **Zero idle cost.** Lambda + Knative scale to zero. Glue's billing
  model is per-DPU-hour of actual work, but the catalog itself has
  per-request fees and the auto-optimization scheduler runs
  regardless of whether anything needs compacting.
- **Workload classifier.** Streaming tables get a 5-min cadence and
  a 60s write-buffer; batch tables get hourly; dormant tables get
  weekly. Glue treats every table the same.
- **Adaptive feedback loop (planned).** Per-table convergence on
  warm vs. task tier without operator tuning. Glue allocates DPUs
  by job, not by table history.
- **Three-tier kill switch.** Per-table self-pause → warehouse
  self-pause → operator panic button — each encoded as a control
  file in the warehouse, survives any runtime crashing.

## Side-by-side maturity matrix

| Capability | Glue | iceberg-janitor (Go) |
|---|---|---|
| Binpack compaction | ✅ Shipped, GA | ✅ Shipped (parquet-go-direct path) |
| Stitching binpack (column-chunk byte copy) | ❌ (decode/encode) | ⚠️ Designed, not shipped |
| Sort compaction | ✅ Shipped | ❌ Designed, not shipped |
| Z-order compaction | ⚠️ _(uncertain GA status)_ | ❌ Not on the immediate roadmap |
| Snapshot expiration | ✅ Shipped (Optimize) | ❌ Queued, sub-agent attempt this session |
| Orphan removal | ✅ Shipped (Vacuum) | ❌ Designed (two-phase dry-run), not shipped |
| Manifest rewrite | ⚠️ _(uncertain)_ | ❌ Designed, not shipped |
| Pre-commit master check | ❌ None published | ✅ I1, I2, I3, I4, I5, I7 shipped (6/9) |
| Circuit breakers | ❌ None | ⚠️ CB1, CB8 shipped (2/11) |
| Recycle bin | ❌ | ✅ Designed, partial |
| Workload classifier | ❌ | ⚠️ Designed, partial |
| Adaptive feedback loop | ❌ | ❌ Designed, not shipped |
| Multi-cloud | ❌ AWS-only | ✅ S3, GCS, Azure, MinIO, POSIX |
| Catalog-less | ❌ Glue catalog required | ✅ Bare buckets |
| Open source | ❌ | ✅ |
| Distributed compute | ✅ Spark on Glue | ❌ Single-instance, by design |
| Production track record | ✅ Years | ❌ MVP |

[issue-3]: https://github.com/mystictraveler/iceberg-janitor/issues/3
[issue-6]: https://github.com/mystictraveler/iceberg-janitor/issues/6
