# iceberg-janitor — Features and Current State

The canonical state ledger for every iceberg-janitor capability. One
row per feature with **STATE**, mechanism, and the test/bench evidence
that proves it works (or notes the gap). Companion to:

- `EXECUTIVE_SUMMARY.md` — what it is and why
- `BENCHMARKS.md` — full evidence trail of every measured bench run
- `README.md` — operator-facing usage

When a feature ships, partial-ships, gets planned, or gets refused,
update its row here. This doc is reviewed when scoping releases or
answering "is X supported?".

---

## State legend

- **Shipped** — on `main`, fully tested, supported in production
- **Shipped (branch)** — fully implemented + tested on a feature branch, awaiting merge
- **Partial** — runtime works, named gap remains (link the GitHub issue)
- **Planned** — designed but not started
- **Refused** — deliberate not-supported (silently-incorrect or out-of-scope)

---

## Summary

Commits below are the load-bearing change(s) where the feature took its
current shape — earlier scaffolding may exist, and follow-up fixes are
elided unless they materially changed correctness. `git log -- <path>`
on the feature's main file gives the full history.

| # | Feature | State | Shipped in | Tracker |
|---|---|---|---|---|
| 1 | [Compaction (byte-copy stitch + row group merge)](#compaction) | Shipped | `6e573f6` parquet-go-direct path, `7ff9ee8` byte-copy stitch + Pattern B, `da598f7` auto-merge row groups when >4 RGs | — |
| 2 | [V2 merge-on-read position deletes](#v2-merge-on-read-deletes) | Shipped (branch `feature/v2-deletes`) | `d54e4bf` | PR pending |
| 3 | [V2 merge-on-read equality deletes — runtime](#v2-merge-on-read-deletes) | Shipped (branch `feature/v2-deletes`) | `d54e4bf` | PR pending |
| 4 | [V2 equality-delete end-to-end fixture](#v2-merge-on-read-deletes) | Partial — runtime shipped, real-table integration test missing | — | [#8](https://github.com/mystictraveler/iceberg-janitor/issues/8) |
| 5 | [Snapshot-side cleanup of consumed delete files](#v2-merge-on-read-deletes) | Partial — orphaned delete files survive compact, cleaned up by Expire + OrphanFiles eventually | — | — |
| 6 | [Snapshot expiration](#snapshot-expiration) | Shipped | `ee9f450` | — |
| 7 | [Manifest rewrite](#manifest-rewrite) | Shipped | `ee9f450` | — |
| 8 | [Maintain pipeline (async)](#maintain-pipeline) | Shipped | `8971543` async job API, `11d916e` hot/cold maintain pipeline | — |
| 9 | [Master check I1–I8 (mandatory pre-commit)](#master-check) | Shipped | `3e2308c` I3/I4/I5 added; `d54e4bf` I1 deletedRows hint + I3 offset + I4 skip | — |
| 10 | [Master check I9](#master-check) | Planned (reserved slot) | — | — |
| 11 | [Circuit breakers CB2–CB11](#circuit-breakers) | Shipped (CB1 removed by design in `7ff9ee8`) | `e8f94d1` (all CB2–CB11) | — |
| 12 | [Workload classification (4-class)](#workload-classification) | Shipped | `1ff841c` | — |
| 13 | [Async job API + persistent records](#async-job-api) | Shipped | `8971543` async job API; `70df322` persistent records (Phase 2); `e2ce521` wired into jobStore (Phase 3) | — |
| 14 | [Per-table in-flight dedup (lease)](#per-table-in-flight-dedup) | Shipped | `09de93d` lease primitive (Phase 1); `e2ce521` wired (Phase 3); `642fcf1` in-flight guard fix | — |
| 15 | [Two runtime tiers, one container image](#two-runtime-tiers-one-container-image) | Shipped | `f5f16e3` server + CLI; `8971543` AWS Fargate deployment; lambda scaffold removed on observability-track | — |
| 16 | [Catalog-less directory catalog](#catalog-less-directory-catalog) | Shipped | `23f8440` Spark-compatible v\<N\>.metadata.json + version-hint.text; `d54e4bf` WithProperties round-trip fix | — |
| 17 | [Sort-on-merge from Iceberg metadata](#sort-on-merge) | Shipped | `f3918f3` | — |
| 18 | [Dry-run mode (all 4 endpoints)](#dry-run-mode) | Shipped | `a7b6110` cut points; `a029370` wired through handlers + CompactCold + OpenAPI + tests | — |
| 19 | [Glue registration](#glue-registration) | Shipped | `896048b` janitor-cli fast path; `ca96c63` server: metadata_location in job result + direct Glue UpdateTable | — |
| 19a | [Schema-evolution guard (skip mixed-schema rounds)](#schema-evolution-guard) | Shipped (branch `feature/schema-evolution-guard`) | TBD on merge | PR pending |
| 20 | [V3 deletion vectors](#refused) | Refused (safety gate) — backlog [#10](https://github.com/mystictraveler/iceberg-janitor/issues/10) | refusal in `feature/v2-deletes` (`d54e4bf`) | [#10](https://github.com/mystictraveler/iceberg-janitor/issues/10) |
| 21 | [V3 row lineage](#refused) | Refused (safety gate) — backlog [#11](https://github.com/mystictraveler/iceberg-janitor/issues/11) | — | [#11](https://github.com/mystictraveler/iceberg-janitor/issues/11) |
| 22 | [V3 Puffin stats](#refused) | Refused (safety gate) — backlog [#12](https://github.com/mystictraveler/iceberg-janitor/issues/12) | — | [#12](https://github.com/mystictraveler/iceberg-janitor/issues/12) |
| 23 | [Mixed partition-spec compaction](#refused) | Refused (safety gate) | refusal in `feature/v2-deletes` (`d54e4bf`) | — |
| 24 | [Equality deletes on complex column types](#refused) | Refused (safety gate) | refusal in `feature/v2-deletes` (`d54e4bf`) | — |
| 25 | [Pattern C (on-commit dispatcher)](#planned) | Planned | — | [#3](https://github.com/mystictraveler/iceberg-janitor/issues/3) |
| 26 | [Adaptive feedback loop](#planned) | Planned (design plan #12) | — | — |
| 27 | [pprof endpoint on janitor-server](#observability) | Shipped (branch `feature/observability-track`) | `b23accc` | PR pending |
| 28 | [q1 query latency outlier diagnosis](#planned) | Planned (open since Run 8) | — | — |
| 29 | [OpenTelemetry tracing](#observability) | Shipped (branch `feature/observability-track`) | `7d4beae` phase 1; `478f22d` phase 2; `7cf2adb` phase 3; `7b6cf01` phase 4 | PR pending |
| 30 | [OpenTelemetry metrics](#observability) | Shipped (branch `feature/observability-track`) | `dfa0179` | PR pending |

---

## Table of contents

- [Compaction](#compaction)
- [V2 merge-on-read deletes](#v2-merge-on-read-deletes)
- [Snapshot expiration](#snapshot-expiration)
- [Manifest rewrite](#manifest-rewrite)
- [Maintain pipeline](#maintain-pipeline)
- [Master check (I1–I9 invariants)](#master-check)
- [Circuit breakers](#circuit-breakers)
- [Workload classification](#workload-classification)
- [Async job API + persistent records](#async-job-api)
- [Per-table in-flight dedup (lease)](#per-table-in-flight-dedup)
- [Two runtime tiers, one container image](#two-runtime-tiers-one-container-image)
- [Catalog-less directory catalog](#catalog-less-directory-catalog)
- [Sort-on-merge](#sort-on-merge)
- [Dry-run mode](#dry-run-mode)
- [Glue registration](#glue-registration)
- [Schema-evolution guard](#schema-evolution-guard)
- [Observability](#observability)
- [Refused (safety gates)](#refused)
- [Planned](#planned)

---

## Compaction

**STATE: Shipped** in `6e573f6` (parquet-go-direct path that closed the row-loss bug under streaming load), `7ff9ee8` (byte-copy stitch + Pattern B + remove CB1), `da598f7` (auto-merge row groups when stitch produces > 4 RGs).

**Mechanism.** Two-phase: byte-copy stitch (parquet-go `WriteRowGroup`
fast path, no decode) for the common case, then a post-stitch row group
merge via pqarrow when the stitched output has > 4 row groups or a sort
order is defined. Replaces `oldPaths` → `newPath` via
`tx.ReplaceDataFiles`.

**Three entry points all go through the same execute path:**

- `Compact` — single-table, full or partition-scoped
- `CompactHot` — per-partition delta-stitch with single-snapshot batched commit; the streaming hot path
- `CompactCold` — per-partition trigger-based (cold data)
- `CompactTable` — wrapper that runs hot then cold

**Correctness evidence:**

- Unit tests: `pkg/janitor/compact_*_test.go` exercise the byte-copy + pqarrow paths
- AWS bench Run 20: 192× file reduction on 3 TPC-DS tables; Athena query latency 23–27% faster vs uncompacted baseline; identical output to Spark EMR Serverless `rewriteDataFiles` at 6.3× less compute (`BENCHMARKS.md`)
- MinIO bench Run 18.6: phased A/B with `bench.sh minio` confirms file reduction without row drift on every iteration

---

## V2 merge-on-read deletes

**STATE:**
- Position deletes (read-through during compaction): **Shipped (branch `feature/v2-deletes`, pending merge to `main`)** in `d54e4bf`.
- Equality deletes (runtime in `BuildRowMask`): **Shipped (same branch)** in `d54e4bf`.
- Bench harness `WORKLOAD=deletes` mode: shipped in `7e174ea`.
- Equality deletes (end-to-end real-table fixture): **Partial — tracked at [#8](https://github.com/mystictraveler/iceberg-janitor/issues/8).**
- Snapshot-side cleanup of consumed delete files at commit: **Partial — orphans cleaned up by Expire + OrphanFiles eventually.**
- V3 deletion vectors / equality deletes on complex column types: **Refused via safety gate** (refusal added in `d54e4bf`; see [Refused](#refused)).

**Mechanism.** When the manifest walk encounters position-delete or
equality-delete entries on the source partitions, `executeStitchAndCommit`
forces the pqarrow decode/encode path (byte-copy can't filter rows), loads
the delete payloads, builds a per-source-file row mask, and applies it
during the Arrow batch write. The dropped-row count is passed to the
master check via `WithDeletedRows` so I1 validates `before − dropped ==
staged`. Equality deletes on complex column types (timestamp/decimal/
uuid/binary/struct/list/map) are refused via `*UnsupportedFeatureError`
— correctness over coverage.

**Refusal gates (silently-incorrect cases):**

- V3 deletion vectors (PUFFIN-format pos deletes) — refused
- Mixed partition spec ids across source files — refused
- Equality-delete schemas with non-scalar columns — refused

**Correctness evidence:**

- 12 unit tests in `pkg/janitor/deletes_test.go`: position-set membership; multi-file pos-delete merge dedup; row mask with non-zero offset; eq-delete int64/composite-key/null-match/cross-width; complex-type refusal returns `*UnsupportedFeatureError`; scalar-type round-trip through parquet
- 3 end-to-end tests in `pkg/janitor/compact_v2_deletes_test.go`:
  - `TestCompact_V2_PositionDeletes_EndToEnd`: seed 48 rows / 6 files, fire `tx.Delete(id=5)`, run `Compact`, verify 47 rows post-compact, id=5 absent, master check PASS
  - `TestCompact_V2_MultiRowPositionDeletes`: seed 40 rows / 4 files, fire `tx.Delete(value >= 500)` (drops 5 of 10 rows per file), verify 20 rows post-compact, all surviving rows have value < 500
  - `TestUnsupportedFeatureError_V3Shape`: pins the V3 DV refusal error contract
- MinIO bench (`WORKLOAD=deletes bench.sh minio`):

  | Seed shape | Deletes | Files before → after | Rows before → after | Compact wall | Master |
  |---|---|---|---|---|---|
  | 200 batches × 50 rows = 10000 rows | 50 | 200 → 1 (200×) | 10000 → 9950 | **1.4 s** | PASS (I1 in=10000 DVs=50 out=9950) |
  | 500 batches × 100 rows = 50000 rows | 200 | 500 → 1 (500×) | 50000 → 49800 | **2.3 s** | PASS (I1 in=50000 DVs=200 out=49800) |

**Known gaps (logged as future work):**

- End-to-end equality-delete fixture: iceberg-go has no public emit path for eq deletes (only pos deletes via `tx.Delete` + merge-on-read). Eq-delete runtime code in `BuildRowMask` is exercised today by unit tests with synthetic Arrow batches but lacks a real-table integration test. Tracked at GitHub issue #8. Three options documented: DuckDB iceberg writer, hand-craft via `ManifestWriter` + `DataFileBuilder`, or upstream `AddEqualityDeleteFile` PR.
- Removing consumed delete files from the snapshot at commit: iceberg-go's public `ReplaceDataFiles` takes only data file paths; there's no exposed primitive to drop a delete file. After compaction, the orphaned delete files are semantically inert (referenced data files are gone OR have lower seq_num) and will be cleaned up by Expire + OrphanFiles. A follow-up tapping iceberg-go's internal `snapshotProducer` API would close the gap.

---

## Snapshot expiration

**STATE: Shipped** in `ee9f450`.

**Mechanism.** `pkg/maintenance/expire.go` walks the snapshot chain,
identifies snapshots older than the retention threshold, and drops them
from `metadata.snapshots` + `metadata.refs.main.history`. Master check
runs against the staged metadata to confirm the surviving chain is
internally consistent before commit.

**Correctness evidence:**

- Unit + integration tests in `pkg/maintenance/expire_test.go`
- Master check I7 (manifest references) verifies every data file referenced by surviving snapshots actually exists on the FS

---

## Manifest rewrite

**STATE: Shipped** in `ee9f450` (same commit as expire).

**Mechanism.** `pkg/maintenance/manifest_rewrite.go` consolidates
per-commit micro-manifests into a partition-organized layout. Reduces
the manifest list walk on subsequent compactions from O(commits) to
O(partitions).

**Correctness evidence:**

- Unit tests in `pkg/maintenance/manifest_rewrite_test.go`
- Master check I8 (file-set invariant): the union of data files across the rewritten manifests must equal the union before the rewrite
- AWS bench Run 19+: hot-loop CAS race wins improve after manifest rewrite reduces per-attempt walk time

---

## Maintain pipeline

**STATE: Shipped** in `8971543` (async job API), `11d916e` (hot/cold maintain pipeline), `f9f6d40` (wired into bench).

**Mechanism.** Single endpoint `POST /v1/tables/{ns}/{name}/maintain`
that runs `expire → rewrite-manifests → compact → rewrite-manifests`
in load-bearing order. Each phase is async and tracked by job_id.

**Correctness evidence:**

- Integration tests in `pkg/maintenance/maintain_test.go`
- AWS bench Run 18-20: `maintain` is the only entry point used by the bench harness; every Run row in BENCHMARKS.md is a maintain run

---

## Master check

**STATE: Shipped (I1–I8). I9 reserved/Planned.**

I1 dates back to the initial Phase 3 compactor; I3/I4/I5 added in `3e2308c`; I7 narrowing fix is `b6e7ac2`-era; I1 `WithDeletedRows` hint + I3 bounded offset + I4 skip on deletes added in `d54e4bf`.

Mandatory, non-bypassable pre-commit verification. Cannot be disabled
with `--force`. Runs against the staged transaction's `StagedTable`
before `tx.Commit()`.

| Invariant | Check | Caught real bug? |
|---|---|---|
| **I1 row count** | `before_rows − deleted_rows == staged_rows` (the `deleted_rows` term is the new V2-delete hint) | Yes — iceberg-go scan path silently lost rows under concurrent writers (apache/iceberg-go#860) |
| **I2 schema** | `before_schema_id == staged_schema_id` | — |
| **I3 per-column value count** | `before_col_values − Δ == staged_col_values` (Δ bounded by `0 ≤ Δ ≤ deleted_rows`) | — |
| **I4 per-column null count** | strict equality (skipped when `deleted_rows > 0` because null deltas can't be derived from manifest stats alone) | — |
| **I5 column bounds presence** | every column with bounds in input must have bounds in staged | — |
| **I7 manifest references** | every data file the staged snapshot newly references must exist on the FS (HEAD check, narrowed to new files only — fix for #5) | — |
| **I8 file-set invariant** | manifest rewrite preserves the data file set | — |
| **I9 (planned)** | reserved | — |

**Correctness evidence:**

- Unit tests in `pkg/safety/verify_test.go` cover each invariant + the new `WithDeletedRows` option
- The check has refused commits in real bench runs that would have committed silently-wrong data — see Run 11 in BENCHMARKS.md

---

## Circuit breakers

**STATE: Shipped (CB2–CB11)** in `e8f94d1`. CB1 was added in `5adece5` and deliberately removed in `7ff9ee8` (see project memory `cb1_deleted.md`).

10 fatal/soft severity gates implemented in `pkg/safety/circuitbreaker.go`:

| ID | Trigger | Action |
|---|---|---|
| CB2 | Loop detection: same files compacted N consecutive times | Auto-pause |
| CB3 | Metadata-to-data ratio breaches warn / pause / critical thresholds | Warn / pause |
| CB4 | No-effectiveness: compaction produced ≥ same number of files | Mark ineffective |
| CB7 | Daily byte budget exceeded | Pause until reset |
| CB8 | Consecutive failed runs ≥ threshold | Auto-pause |
| CB9 | Lifetime rewrite ratio (rewritten / original) exceeds limit | Auto-pause |
| CB10 | Recursion guard: refuses to operate on janitor's own internal path | Refuse |
| CB11 | Low-ROI: estimated benefit/cost ratio below threshold | Skip |

**Correctness evidence:**

- Unit tests in `pkg/safety/circuitbreaker_test.go` (test coverage 48.6%)
- CB-trip auto-pause writes to `_janitor/control/paused/<table_uuid>.json` which the next compaction attempt reads and respects (state-machine test in `pkg/state/state_test.go`)

---

## Workload classification

**STATE: Shipped** in `1ff841c` (4-class classifier), wired into the maintain endpoint shortly after; doc section in `0f59007`.

**Mechanism.** `pkg/strategy/classify` reads commit-rate windows
(15-min fast path, 24h, 7d) and classifies each table as one of:
streaming, batch, slow_changing, dormant. Feeds per-class compaction
plans (hot vs cold cadence, target file size, parallelism).

**Correctness evidence:**

- Unit tests in `pkg/strategy/classify/classify_test.go`
- Behavior reported through the `/v1/tables/{ns}/{name}/health` endpoint — visible in the bench `with-janitor` server logs

---

## Async job API

**STATE: Shipped** in `8971543` (initial async + AWS deployment), `70df322` (Phase 2 persistent records), `e2ce521` (Phase 3 wired into jobStore).

**Mechanism.** Every maintenance op (`compact`, `expire`,
`rewrite-manifests`, `maintain`) returns 202 + `job_id`. Job state
persists at `_janitor/state/jobs/<job_id>.json` (`pkg/jobrecord`).
`GET /v1/jobs/{id}` polls. Survives server restarts.

**Correctness evidence:**

- Integration tests in `pkg/jobrecord/record_test.go`
- Bench harness (`bench.sh`) uses the async API for every call; persistence verified by killing + restarting `janitor-server` mid-run

---

## Per-table in-flight dedup

**STATE: Shipped** in `09de93d` (Phase 1 lease primitive), `e2ce521` (Phase 3 wired into jobStore), `642fcf1` (in-flight guard race fix).

**Mechanism.** `pkg/lease` writes a TTL'd lease at
`_janitor/state/leases/<ns>.<table>/<op>.lease` via S3 conditional create
(`If-None-Match: *`). Concurrent requests for the same (table, op)
return the existing job_id instead of starting a duplicate. Stale-lease
takeover via TTL.

**Correctness evidence:**

- Unit tests in `pkg/lease/lease_test.go` exercise CAS contention, TTL takeover, and concurrent acquire
- Bench Run 19 (3-replica): cross-replica dedup observed in CloudWatch logs ("returning in-flight job" entries match across server hostnames)

---

## Two runtime tiers, one container image

**STATE: Shipped** in `f5f16e3` (HTTP server + CLI + OpenAPI) and `8971543` (AWS Fargate deployment). Lambda scaffold (`cmd/janitor-lambda`) removed as dead code — see below.

Same `pkg/janitor` core runs as:

- Long-lived HTTP server (`cmd/janitor-server`) — container image runs on Fargate / EKS / Cloud Run / Knative; the same image runs on AWS Lambda when wrapped with [AWS Lambda Web Adapter](https://github.com/awslabs/aws-lambda-web-adapter) (LWA terminates the Lambda Runtime API and proxies to the HTTP port). Orchestrator choice is a Terraform-layer decision, not a binary decision.
- One-shot CLI (`cmd/janitor-cli`) — local operator tool for analyze / compact / expire / rewrite-manifests / maintain / glue-register.

**Why no dedicated Lambda binary.** An earlier design had `cmd/janitor-lambda` as a separate Lambda handler binary. Two facts killed it:

1. The real binary (had it been written) would have imported the same `pkg/janitor` + `pkg/maintenance` + `pkg/catalog` + `pkg/safety` dep graph as the server, so the linux/amd64 stripped binary would be ~77 MB — essentially the same as the server. Cold-start advantage of a zip Lambda over a container-image Lambda shrinks to ~100–200 ms at this size, not the 3–5× the original design assumed.
2. A separate binary means a second CI path, a second artifact, and a second test surface for identical pkg/janitor code. Container-image Lambda via AWS Lambda Web Adapter gets us to one binary, one image, one CI, one test surface, with a bounded cold-start cost.

The scaffold was a 21-line placeholder that printed `"scaffold only; Lambda adapter lands in Phase 4"` and exited 2. It was never deployed, and the Terraform stack has no Lambda function resource. Removed in the observability-track branch.

**Correctness evidence:**

- Each `cmd/*` builds and tests independently
- AWS bench runs the server in Fargate; CLI is used by `bench.sh local`
- For Lambda deployment: untested today — will be proven when the Pattern C on-commit dispatcher (GitHub issue #3) lands and wires the first EventBridge → LWA → server path

---

## Catalog-less directory catalog

**STATE: Shipped.** Initial `pkg/catalog/directory.go` predates the Phase 1 cut; Spark-compatible `v<N>.metadata.json` + `version-hint.text` naming added in `23f8440`. `WithProperties` round-trip fix (latent bug) added in `d54e4bf`.

**Mechanism.** `pkg/catalog/directory.go` reads + writes Iceberg
metadata directly to/from object storage, with no external catalog
service. Atomic commits via S3 `If-None-Match: *` (or GCS
`x-goog-if-generation-match: 0`). Spark-compatible `v<N>.metadata.json`
naming with `version-hint.text` pointer.

**Correctness evidence:**

- Unit tests in `pkg/catalog/directory_*_test.go`
- CAS race tests in `pkg/catalog/cas_test.go`
- AWS bench reads the same S3 warehouse from Athena (Glue-registered) without the janitor needing a Glue or REST catalog
- MinIO round-trip: DuckDB reads the same warehouse the janitor wrote (BENCHMARKS.md Run 2b)

**Recently fixed (`feature/v2-deletes` branch):** `WithProperties` on
`CreateTable` was silently dropped. Now round-trips. Required for
`write.delete.mode=merge-on-read` and any future property-driven feature.

---

## Sort-on-merge

**STATE: Shipped** in `f3918f3` (sort-on-merge for all compact variants + CreateTable sort order fix). Bench A/B in `c488ee5` (Run 20).

**Mechanism.** When `maybeMergeRowGroups` fires (row groups > 4 OR
sort order defined OR V2 deletes apply), if the table has a
non-trivial default sort order set in Iceberg metadata, rows are sorted
by those columns before writing. Produces tighter min/max column stats
→ better predicate pushdown.

**Correctness evidence:**

- `pkg/janitor/merge_sort_test.go` — out-of-order ids 40,0,24,8,32,16 across 6 files compact to a single sorted output [0..47]
- Bench Run 20: sort-on-merge A/B (`WITH_SORT` knob in streamer) confirms tighter min/max bounds; query latency neutral on the bench's range predicates

---

## Dry-run mode

**STATE: Shipped** in `a7b6110` (cut points started) and `a029370` (wired through handlers + CompactCold + OpenAPI + tests).

**Mechanism.** `?dry_run=true` on all four maintenance endpoints. Cut
points run real manifest walks (so contention is detected) but stop
before any side effects. Reload-and-compare-snapshot-id at the end
probes for CAS contention an actual run would have hit.

**Correctness evidence:**

- Integration tests in `pkg/maintenance/dryrun_test.go` confirm no parquet writes, no transaction stage, no commit
- Contention probe verified in MinIO bench by triggering a foreign writer mid-walk

---

## Glue registration

**STATE: Shipped** in `896048b` (`janitor-cli glue-register --metadata-location` fast path) and `ca96c63` (server: `metadata_location` in job result + direct Glue UpdateTable).

**Mechanism.** `janitor-cli glue-register` registers the warehouse's
Iceberg tables with AWS Glue using `metadata_location` so Athena can
query them. Returns `metadata_location` in job result for external
callers.

**Correctness evidence:**

- AWS bench Run 18-20: Athena queries the Glue-registered tables with no manual SQL
- Sandbox NACL blocks Glue from Fargate; production deployments call externally — see memory note `glue_bottleneck.md`

---

## Schema-evolution guard

**STATE: Shipped (branch `feature/schema-evolution-guard`, PR pending).**

### What it does

When a compaction round's source file set straddles a schema change
(some files at schema N, others at schema N+1 after a DDL
evolution), the janitor **skips** the round rather than producing
silently-corrupt output. The result carries `Skipped=true`,
`SkippedReason="mixed_schemas"`, and a `SkippedDetail` string of the
form `"schema=1 files=45 | schema=2 files=7"` (or `fid-sig=...` when
the schema-id metadata key is absent — see below). Before/after
snapshot ids are equal; no transaction is staged; no rows move.

### Rationale — why skip rather than rewrite

**Schema evolutions are rare; compaction is continuous.** A real
Iceberg table evolves its schema at a cadence of days to weeks —
deliberate DDL operations like "add nullable column", "drop column",
"widen int32 to int64". Streaming workloads, which are the janitor's
hot path, run against a stable schema for the lifetime of the
streamer. Compaction in contrast runs every few seconds on hot
tables. The rate ratio is ~10⁵ or larger: compaction cycles per
schema change.

Given that asymmetry, the architecturally simple thing is for the
compactor to respect the boundary and let time heal:

- **Skip** any round whose source set spans schemas. Zero work, zero
  risk of silent corruption, zero coupling between maintenance and DDL.
- As the writer produces more files at the new schema, the tail at
  schema N+1 grows. The next round's source selection window will
  eventually contain only N+1 files and fire normally.
- Old-schema files age out through Expire (old snapshots drop from
  the retain set) and OrphanFiles (the recycle bin sweeps
  unreferenced paths). Within one or two expire cycles the old
  schema is gone from the active table entirely.

**Rewriting across a schema boundary** — decoding schema-N files and
re-encoding them at schema N+1 — is a fundamentally different
operation than compaction. It changes data: added-column values
become NULL in the output, dropped-column values are lost, widened
types may overflow. That belongs in a separate `rewrite-schema` op
if a user ever needs it, not silently bundled into compaction.

### Mechanism

`pkg/janitor/schema_group.go` owns the detection:

- **Primary path**: parse `iceberg.schema.id` from the parquet
  footer's key-value metadata (stamped by some Iceberg writers,
  notably Spark).
- **Fallback path** (used when the KV key is absent, which is the
  case for iceberg-go-written files in v0.5.0): SHA-256 signature
  over the sorted `(field-id, physical-type-code)` pairs for every
  leaf column. Catches add/drop/widen evolutions without flagging
  benign column renames (which preserve field IDs).
- **Sign-bit collision avoidance**: signature-derived keys are
  negated so they can never collide with a legitimate non-negative
  schema-id from the direct-parse path.

`pkg/janitor/compact_replace.go::executeStitchAndCommit` calls
`groupPathsBySchemaID` before any write-side work. If the result has
more than one group, `CompactResult` gets `Skipped` + reason +
detail populated and the function returns `nil` — a no-op round, not
an error.

### Correctness evidence

- `pkg/janitor/schema_evolution_test.go`:
  - `TestCompact_SchemaEvolution_MixedSchemasSkipped` — seeds 3
    files at the original schema, evolves with `AddColumn`, seeds 2
    more files at the evolved schema, runs `Compact`, verifies
    `Skipped=true` + `SkippedReason="mixed_schemas"` + snapshot id
    unchanged + file/row counts unchanged.
  - `TestCompact_SchemaEvolution_SingleSchemaRunsNormally` — evolves
    FIRST, then seeds 5 files all at the evolved schema, confirms
    `Skipped=false` and normal file reduction.
- Full `go test ./...` green on `feature/schema-evolution-guard`
  (all 29 packages).

### Known gaps

- `SkippedDetail` currently shows `fid-sig=<hex>` because iceberg-go
  doesn't stamp `iceberg.schema.id` in parquet KV metadata today.
  Functionally correct — grouping still fires — but less friendly
  than `schema=1 files=45 | schema=2 files=7`. If iceberg-go adds
  the stamp (or Spark is the writer), the detail string uses the
  friendlier form automatically.
- The guard refuses mixed rounds but does not OFFER the per-group
  compaction alternative (produce N output files, one per schema
  version, in one transaction). Per the rationale above, that's a
  deliberate non-feature — if a user needs it, the right shape is a
  dedicated `rewrite-schema` op.

---

## Observability

OpenTelemetry tracing + metrics + pprof. Lives on branch
`feature/observability-track` until the MinIO TPC-DS bench gate
(OBSERVABILITY_SPEC.md §Hot-path overhead, ±1% rule) is run. Default
production posture: every signal is **off** — NoOp tracer, NoOp
meter, no pprof listener. Operators opt in per signal at deploy
time via env vars.

| Signal | Default | How to turn on | Export destination |
|---|---|---|---|
| OTel tracing | off (NoOp tracer) | `JANITOR_TRACE=stdout` (pretty) or `stdout-compact` (smaller) | stderr; operators who need OTLP register their own `TracerProvider` before janitor startup |
| OTel metrics | off (NoOp meter) | `JANITOR_METRICS=stdout` | stderr; `JANITOR_METRICS=otlp` is reserved — operators register their own `MeterProvider` to keep the OTLP/gRPC dep out of the default binary |
| pprof | off (no listener) | `--debug-addr=127.0.0.1:6060` or `JANITOR_DEBUG_ADDR=...` | separate `http.Server`, no overlap with the public `/v1` surface |

Instrument inventory (stable field names; dashboard contract):

- Traces: `Compact`, `Expire`, `RewriteManifests`, `VerifyCompactionConsistency`, `compactOnce`, `manifest_walk`, `stitchParquetFiles`, `stitch_source`, `stitch_write_fallback`, `stitch_source_worker`, `master_check`, `cas_commit`, `load_table`, plus per-request `HTTP <method> <route>` spans
- Counters: `janitor.compact.attempts` (tags: table/outcome/skip_reason), `janitor.compact.cb_trips` (tags: cb_id/table), `janitor.expire.attempts`, `janitor.rewrite_manifests.attempts`
- Histograms: `janitor.compact.wall_ms`, `janitor.master_check.wall_ms`, `janitor.compact.cas_retries`, `janitor.compact.file_reduction_ratio`

Dev stack: `go/test/mvp/docker-compose.dev.yml` overlays Jaeger,
Pyroscope, and an otel-collector onto the base MinIO compose so a
local run can see traces in Jaeger UI and profiles in Pyroscope
without touching production infra.

Hot-path protection: OBSERVABILITY_SPEC.md enforces a **±1% rule**
on the MinIO TPC-DS A/B bench. Every recording site uses
`span.IsRecording()` gating for any attribute that would cost an
allocation even on the NoOp path (the source file path in the
stitch workers, specifically). Per-row and per-row-group spans are
deliberately skipped.

---

## Refused

Capabilities the janitor deliberately does not support, refused at the
safety gate (`checkTableForUnsupportedFeatures` in `pkg/janitor/safety_guards.go`,
or `LoadEqualityDelete` in `pkg/janitor/deletes.go`). All refusals
return `*UnsupportedFeatureError` to the caller — never a silent
degradation.

| Capability | Why refused |
|---|---|
| **V3 deletion vectors** (PUFFIN-format pos deletes) | Compacting these would silently resurrect deleted rows because the byte-copy stitch can't read the Puffin payload to filter. Detected via `EntryContentPosDeletes` + `FileFormat == PUFFIN`. Backlog: [#10](https://github.com/mystictraveler/iceberg-janitor/issues/10). |
| **V3 row lineage columns** | Not propagated by the byte-copy stitch; output would lose lineage IDs silently. Backlog: [#11](https://github.com/mystictraveler/iceberg-janitor/issues/11). |
| **V3 Puffin stats** | Not yet read; statistics in output would diverge from input. Backlog: [#12](https://github.com/mystictraveler/iceberg-janitor/issues/12). |
| **Mixed partition spec ids across source files** | Per-partition grouping becomes ambiguous; output partition assignment would be wrong. |
| **Equality deletes on complex column types** (timestamp / decimal / uuid / binary / struct / list / map) | Comparator semantics are non-trivial for these types; matching them approximately would either over- or under-delete. Refused in `LoadEqualityDelete` after schema inspection. |

Lifting any of these from Refused → Shipped requires a real V3-using
or complex-eq-delete-using table to test against, plus the
correctness-equivalence proof against an existing engine (Spark or
Trino).

---

## Planned

Designed but not implemented. Each row links to its tracker.

| Capability | Status | Tracker |
|---|---|---|
| **End-to-end equality-delete fixture** | Runtime shipped (branch); fixture path is the gap. Three options documented in the issue. | [#8](https://github.com/mystictraveler/iceberg-janitor/issues/8) |
| **Snapshot-side removal of consumed delete files at commit** | iceberg-go's public `ReplaceDataFiles` takes only data file paths; no exposed primitive drops a delete file. Workaround today: orphans cleaned up by Expire + OrphanFiles. Closed by either tapping iceberg-go's internal `snapshotProducer` or upstreaming `RemoveDeleteFiles`. | — |
| **Master check I9** | Reserved invariant slot. Shape TBD. | — |
| **Pattern C (on-commit dispatcher)** | External SQS-driven dispatcher with dequeue-time dedup; reacts to writer commits via S3 EventBridge. | [#3](https://github.com/mystictraveler/iceberg-janitor/issues/3) |
| **Adaptive feedback loop** | Per-table convergence on the right compute tier based on history. Design plan #12. | — |
| **pprof endpoint on `janitor-server`** | Shipped on branch `feature/observability-track` — `net/http/pprof` behind `--debug-addr` / `JANITOR_DEBUG_ADDR`, default off. See the Observability section. | — |
| **q1 query latency outlier diagnosis** | +32% on q1 since Run 8, never diagnosed. EXPLAIN ANALYZE pass needed. | — |
| **Per-table maintain config override** | Earlier scoped, then cancelled by user ("classification logic is very advanced already"). Re-listed if a real workload demands it. | — |

---

## How to reproduce a result row

Every correctness signal in this doc is reproducible:

| Signal | Command |
|---|---|
| Unit + integration test suite | `cd go && go test ./...` |
| MinIO TPC-DS A/B bench | `bash go/test/bench/bench.sh minio` |
| MinIO V2 delete bench | `WORKLOAD=deletes bash go/test/bench/bench.sh minio` |
| Local fileblob bench | `bash go/test/bench/bench.sh local` |
| AWS bench (Fargate) | `bash go/test/bench/bench.sh aws` (inside the bench Fargate container) |
| Specific V2 delete tests | `cd go && go test ./pkg/janitor/ -run TestCompact_V2 -v` |
| Specific delete primitives | `cd go && go test ./pkg/janitor/ -run 'TestPosition\|TestBuildRowMask\|TestLoadEqualityDelete' -v` |

Numbers in this doc were captured on `feature/v2-deletes` on 2026-04-15.
Newer runs append to BENCHMARKS.md; the per-feature `Shipped in` commit
column above updates whenever a feature's shape changes (new commit
appended). When merging a branch back to `main`, drop the
`(branch ...)` qualifier on the relevant rows.
