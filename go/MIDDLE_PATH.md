# The middle path: event-driven on-commit compaction

The architectural answer that gives you most of moonlink's freshness, all of the janitor's deployability and cost model, and zero of the operational baggage either approach has on its own.

## The two extremes

**Prevent at write time** (moonlink): the writer buffers events on NVMe, builds indexes locally, flushes only well-sized files to Iceberg. Small files literally never exist. Compaction is unnecessary because there's nothing to compact.

- ✅ Zero seconds of small-file pathology
- ✅ Lowest total I/O (write once at the right size)
- ❌ Requires the writer to be moonlink (or moonlink-equivalent)
- ❌ Always-on compute and NVMe storage — cannot scale to zero
- ❌ Doesn't help with brownfield warehouses or multi-writer workloads
- ❌ Source coupling (Postgres CDC, Kafka, REST, OTEL — and that's it)

**Remediate after the fact via polling** (janitor today): periodic invocations walk the warehouse, find tables that need compaction, rewrite them. The writer is anyone — Spark, Flink, dbt, Trino, Tableflow, anything.

- ✅ Drops onto any warehouse populated by any writer
- ✅ Serverless — compute scales to zero between maintenance events
- ✅ No catalog service, no managed control plane
- ❌ Small files exist for the polling interval (~5-15 minutes)
- ❌ For latency-sensitive query workloads, that window is visible

## The middle path

**React to writer commit events in real time**, but stay catalog-less, multi-cloud, and serverless. The janitor's trigger changes from "polling timer" to "object-store event notification on a new `metadata/v*.metadata.json`." Everything else about the architecture stays the same.

```
                                writer X commits
                                v(N+1).metadata.json
                                       │
                                       ▼
   ┌────────────────────────────────────────────────────────────┐
   │ cloud-native event bus                                      │
   │   AWS:    S3 → EventBridge / SNS → SQS → Lambda             │
   │   GCP:    GCS → Pub/Sub → Cloud Run                          │
   │   Azure:  Blob Storage → Event Grid → Container Apps Job     │
   │   K8s:    Knative KafkaSource (if writer also publishes)    │
   └─────────────────────────────┬──────────────────────────────┘
                                 │
                                 │ "PutObject .../metadata/v(N+1).metadata.json"
                                 ▼
   ┌────────────────────────────────────────────────────────────┐
   │ janitor warm tier wakes up                                  │
   │ (Lambda / Knative pod / Cloud Run / Container Apps)         │
   │ cold start: <200ms                                          │
   └─────────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
   1. Read _janitor/state/<table_uuid>.json (cached one-shot)
   2. Workload class is "streaming"?
        no  → exit immediately, ~50ms total
        yes → continue
   3. Per-class trigger evaluation:
        small_file_count >= STREAMING_TRIGGER (e.g. 50)?
        AND oldest_unprocessed_file >= WRITE_BUFFER_SECONDS (e.g. 60s)?
   4. Acquire lease on the table
        (already-claimed → exit, another invocation is on it)
   5. Run scoped compaction:
        - active partition only (max sequence number)
        - files older than WRITE_BUFFER_SECONDS only
        - streaming-class binpack (5-30 sec total)
   6. Mandatory pre-commit master check (I1..I9)
   7. Atomic CAS commit via the directory catalog
   8. Release lease, exit
```

**Time window during which small files are query-visible:**

| Mode | Window |
|---|---|
| moonlink (prevent at write) | **0 seconds** |
| **middle path (on-commit, this doc)** | **30-60 seconds** |
| janitor polling (today) | 5-15 minutes |
| janitor scheduled batch | hours |
| Untouched warehouse | indefinite |

For 99% of query workloads, the difference between "0 seconds" and "30-60 seconds" is irrelevant — query engines aren't reading the freshest commits anyway, they're reading data that's at least seconds old by the time it reaches them. The difference between "60 seconds" and "5 minutes" matters; the difference between "0" and "60" almost never does.

This is the load-bearing claim: **the middle path is operationally equivalent to moonlink for query freshness, while preserving every other architectural advantage of the janitor.**

## What you keep from each approach

| Property | moonlink | janitor polling | **middle path** |
|---|---|---|---|
| Small files visible to queries | 0 sec | 5-15 min | **30-60 sec** |
| Owns the write path | yes | no | **no** |
| Works on existing warehouses | no | yes | **yes** |
| Works with Spark / Flink / dbt / Trino / Tableflow / anything | no | yes | **yes** |
| Catalog service required | proprietary | none | **none** |
| Multi-cloud | per-cloud | yes | **yes** |
| NVMe / specialized storage | yes (~$330-2,680/mo for ref workload) | none | **none** |
| Always-on compute | yes (~24/7 workers) | no (serverless) | **no (serverless, event-driven)** |
| Cost at idle (workload sleeps overnight) | full bill | $0 | **$0** |
| Cost on bursty workloads | high (always-on) | low (scales with work) | **low (scales with commits)** |
| Cost on sustained high-volume CDC | best (single I/O pass) | medium | medium-low (smaller per-event compaction) |
| Open source compaction algorithm | partial | yes | **yes** |
| Mandatory pre-commit master check | implicit (writer is trusted) | yes (I1..I9) | **yes (I1..I9)** |
| Audit trail in the table itself | depends on writer | yes (snapshot summary) | **yes (snapshot summary)** |
| Operator-visible runaway prevention | managed | 11 circuit breakers | **11 circuit breakers** |

The middle path checks every column the janitor checks today, AND closes the freshness gap to within seconds of moonlink. The only column where moonlink genuinely wins is "0 seconds of small files" — and that's a difference query engines can't observe.

## Why this doesn't make us moonlink

The crucial distinction: **moonlink owns the write path; the middle path subscribes to events on a write path it doesn't own.**

- moonlink intercepts ingestion data, holds it on NVMe, decides when to flush
- the middle path watches for commit notifications the cloud's event bus already produces, then runs a small remediation in the warm tier

We do NOT add:
- An NVMe buffer (the cloud event bus is the buffer)
- An ingestion endpoint (the writer is whoever it was; we don't care)
- A managed control plane (Lambda / Cloud Run / Knative is the runtime)
- A specialized worker fleet (the warm tier scales itself)
- Source coupling (any writer that produces metadata.json files works — Spark, Flink, Trino, dbt, Tableflow, PyIceberg ad-hoc scripts, anything)

What we DO add (compared to today's polling janitor):

1. **`pkg/janitor/oncommit/`** — a small package per cloud event source (`s3events.go`, `gcsevents.go`, `azureevents.go`, `kafka.go`) that parses the cloud-specific event payload into a normalized `CommitEvent{warehouse, table, snapshot_id, timestamp}` shape
2. **Event-triggered dispatch runs the same container image.** The janitor is one HTTP binary packaged as one container image; running it under AWS Lambda Web Adapter (or an API Gateway proxy) lets S3 EventBridge events invoke the existing `/v1/tables/.../maintain` endpoint. No separate Lambda binary, no separate handler code — `oncommit.HandleS3Event` parses the cloud event, produces a `CommitEvent`, and dispatches into `pkg/janitor.OnCommit` via the same HTTP route the polling path uses.
3. **`pkg/janitor.OnCommit(ctx, evt) → Decision`** — the dispatcher. Reads `_janitor/state/<table>.json`, checks the workload class, evaluates the per-class trigger, and either runs `Compact` with a scoped `CompactionRequest` or returns "skip, not yet" cheaply
4. **Per-cloud event-bus wiring** in the deployment artifacts (one-time setup; documented in the runbook)

Total new code is small — a few hundred lines per cloud event source, plus the dispatcher. The hard parts (workload classifier, master check, atomic commit, scoped compaction) are all things the existing plan already requires for other reasons.

## What you give up

The middle path has two honest costs compared to moonlink:

1. **The writer still produces small files for ~30-60 seconds.** Query engines that scan the active partition during that window will read more files than they would with moonlink. For 99% of use cases this is invisible (queries are reading data older than 60 seconds anyway), but for sub-second-freshness CDC analytics it might matter. If you're in that regime, moonlink is the right answer for those specific tables.

2. **Cloud event-bus setup is a one-time deployment cost.** Operators have to enable S3 event notifications (or GCS Pub/Sub, or Azure Event Grid) on the warehouse bucket and wire the event source to the janitor's Lambda / Cloud Run / Container Apps Job. This is a few clicks or a few lines of Terraform per warehouse. Once done, it's invisible.

If you can't enable event notifications (corp IAM lock-down, weird storage backend that doesn't have an event bus), the janitor falls back to polling. The middle path is **opt-in per warehouse**, not mandatory.

## When you'd still prefer moonlink

- You're greenfielding a Postgres CDC pipeline AND query freshness < 1 second is a hard requirement
- Your write path is already moonlink-supported (Postgres, Kafka, REST API, OTEL) AND you want vendor-managed operations
- You're willing to accept always-on cost in exchange for zero remediation latency

## When you'd still prefer the polling janitor

- Your workload is mostly batch (the "run once a night" cadence)
- Your cloud doesn't support storage event notifications
- You want maximum simplicity in deployment (one cron, no event-bus wiring)

## When the middle path is the answer (most of the time)

Everywhere else. Specifically:

- Streaming or near-real-time workloads on existing warehouses
- Heterogeneous warehouses with multiple writers (Spark batch + Flink streaming + dbt + Trino INSERTs all in one bucket)
- Bursty workloads where you want to pay nothing during idle hours
- Multi-cloud deployments where you don't want a per-cloud managed service
- Production deployments where freshness within 60 seconds is fine and zero idle cost is required
- Anyone who wants moonlink-class freshness without committing to moonlink as the writer

This covers the vast majority of real-world Iceberg deployments.

## How it composes with the rest of the design

The middle path is **not a separate runtime or a separate deployment.** It's a different trigger plumbed into the existing `cmd/janitor-server` HTTP surface. The same container image runs on Fargate, Cloud Run, Knative, or Lambda (via AWS Lambda Web Adapter) — the cloud event bus invokes the same `/v1/tables/.../maintain` route the polling loop uses. Specifically:

- **Same `pkg/janitor.Compact`** — the compaction code path is unchanged. The CLI, the polling cron, and the on-commit handler all call into the same function.
- **Same `pkg/safety.VerifyCompactionConsistency`** — the master check runs identically on every commit, regardless of whether the trigger was a polling tick or a commit event.
- **Same `pkg/catalog.DirectoryCatalog`** — atomic CAS via conditional write, no service.
- **Same circuit breakers** — CB1 cooldown, CB3 metadata-data ratio, CB10 recursion guard all enforce the same invariants. The only difference is the rate at which `OnCommit` is called.
- **Same audit trail** — every commit writes a Verification record into the snapshot summary, regardless of trigger source.

The on-commit dispatcher is one new package (`pkg/janitor/oncommit/`) and per-cloud event subscribers. Everything else is reuse.

## Dependency chain

The middle path requires four prior pieces to be in place:

1. **Workload classifier** (`pkg/strategy/classify`) — already in the design plan as part of Phase 2. Identifies which tables are streaming so the on-commit dispatcher knows which events to act on.
2. **Per-class trigger thresholds + WRITE_BUFFER_SECONDS** — already in the design plan. Per the streaming class defaults, `MIN_RECOMPACT_INTERVAL=5min`, `SMALL_FILE_TRIGGER=50`, `WRITE_BUFFER_SECONDS=60`.
3. **Active-partition detection** — already in the design plan. Time-partition + max sequence number (with whole-table-incremental fallback for non-time-partitioned tables).
4. **Lease primitive** — already shipped. Prevents two janitor invocations from racing on the same table.

With those four pieces in place, the on-commit dispatcher is small — a few hundred lines per cloud event source plus a normalized `CommitEvent` parser. The work itself is mostly plumbing.

## Cost numbers from the Mooncake comparison

The per-event Lambda invocation cost is dominated by the ~10% of invocations that actually run a compaction (the other 90% exit in <100ms after a state-file read). For the 50-table reference workload at 10 commits/min/table:

| | Cost/month |
|---|---|
| moonlink instance-store NVMe path | ~$845 |
| moonlink EBS provisioned-IOPS path | ~$2,680 |
| **middle path (janitor on-commit)** | **~$896** |
| janitor polling | ~$867 |

The middle path is ~$30/month more expensive than polling (because it runs more frequently) but ~$0 less expensive than moonlink at the cheap end and **$1,800/month less** at the EBS end. For bursty or sparse workloads (which is most workloads) the gap widens dramatically in the janitor's favor:

| Workload | moonlink | middle path |
|---|---|---|
| Steady 24/7 | ~$845-$2,680 | **~$896** |
| Bursty (8 hr active / 16 idle) | ~$1,047 | **~$642** |
| Sparse / mostly dormant | ~$380 | **~$55** |
| Sustained high-volume CDC, few hot tables | ~$330 | $450-600 |

For three out of four workload shapes, the middle path is cheaper than moonlink. For the fourth (sustained high-volume on a few hot tables), moonlink is cheaper because it does the work once instead of twice — and that's the regime where moonlink genuinely is the right answer. A mature deployment runs both: moonlink on the few tables where its cost premium pays for itself, the middle-path janitor on everything else.

## The summary, in one sentence

**The middle path gets you 99% of moonlink's freshness benefit at 30-50% lower cost on the workloads moonlink doesn't already win, with zero source coupling and zero managed-control-plane requirement.**

It's not a compromise — it's the answer that respects what each end of the spectrum is actually optimizing for. moonlink is right when you control the writer and want absolute freshness. The middle path is right everywhere else. The janitor exists to make the "everywhere else" case as good as it can possibly be, without becoming moonlink.

## Status

This document is a **design proposal**, not yet shipped. The work-ahead list:

1. Land the workload classifier (`pkg/strategy/classify`) in Phase 2
2. Land the per-class trigger thresholds and `WRITE_BUFFER_SECONDS` knob
3. Implement `pkg/janitor/oncommit/` event subscribers (one file per cloud)
4. Implement `pkg/janitor.OnCommit(ctx, evt)` dispatcher
5. Wire the event bus to `cmd/janitor-server`'s `/v1/tables/.../maintain` endpoint. For AWS, that's EventBridge → Lambda-Web-Adapter-wrapped container → HTTP route; for GCS, Pub/Sub → Cloud Run; for Azure, Event Grid → Container Apps. One container image, cloud-specific event plumbing.
6. Add per-cloud event-bus deployment artifacts (Terraform / SAM / Bicep)
7. Document the operator runbook in `go/test/mvp/MVP.md` for the local fileblob equivalent (file system watcher) and the MinIO equivalent (MinIO event notifications via webhook → curl)

This is task #16 in the project work queue. Estimated scope: medium (one focused session per cloud event source, plus the dispatcher).

The classifier (item 1) is the only true blocker. Items 2-7 are mostly mechanical once the classifier exists.
