# iceberg-janitor — Features and Correctness Evidence

A complete inventory of what the janitor does today and the test/bench
evidence that proves each feature works. Companion to:

- `EXECUTIVE_SUMMARY.md` — what it is and why
- `BENCHMARKS.md` — full evidence trail of every measured bench run
- `README.md` — operator-facing usage

This doc is the matrix view: one row per capability, one column per
correctness signal.

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
- [Three runtime tiers](#three-runtime-tiers)
- [Catalog-less directory catalog](#catalog-less-directory-catalog)
- [Sort-on-merge](#sort-on-merge)
- [Dry-run mode](#dry-run-mode)
- [Glue registration](#glue-registration)

---

## Compaction

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

- End-to-end equality-delete fixture: iceberg-go has no public emit path for eq deletes (only pos deletes via `tx.Delete` + merge-on-read). Eq-delete runtime code in `BuildRowMask` is exercised today by unit tests with synthetic Arrow batches but lacks a real-table integration test. Tracked at GitHub issue #8 (cc @praveentandra). Three options documented: DuckDB iceberg writer, hand-craft via `ManifestWriter` + `DataFileBuilder`, or upstream `AddEqualityDeleteFile` PR.
- Removing consumed delete files from the snapshot at commit: iceberg-go's public `ReplaceDataFiles` takes only data file paths; there's no exposed primitive to drop a delete file. After compaction, the orphaned delete files are semantically inert (referenced data files are gone OR have lower seq_num) and will be cleaned up by Expire + OrphanFiles. A follow-up tapping iceberg-go's internal `snapshotProducer` API would close the gap.

---

## Snapshot expiration

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

**Mechanism.** Single endpoint `POST /v1/tables/{ns}/{name}/maintain`
that runs `expire → rewrite-manifests → compact → rewrite-manifests`
in load-bearing order. Each phase is async and tracked by job_id.

**Correctness evidence:**

- Integration tests in `pkg/maintenance/maintain_test.go`
- AWS bench Run 18-20: `maintain` is the only entry point used by the bench harness; every Run row in BENCHMARKS.md is a maintain run

---

## Master check

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

11 fatal/soft severity gates implemented in `pkg/safety/circuitbreaker.go`:

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

**Mechanism.** `pkg/strategy/classify` reads commit-rate windows
(15-min fast path, 24h, 7d) and classifies each table as one of:
streaming, batch, slow_changing, dormant. Feeds per-class compaction
plans (hot vs cold cadence, target file size, parallelism).

**Correctness evidence:**

- Unit tests in `pkg/strategy/classify/classify_test.go`
- Behavior reported through the `/v1/tables/{ns}/{name}/health` endpoint — visible in the bench `with-janitor` server logs

---

## Async job API

**Mechanism.** Every maintenance op (`compact`, `expire`,
`rewrite-manifests`, `maintain`) returns 202 + `job_id`. Job state
persists at `_janitor/state/jobs/<job_id>.json` (`pkg/jobrecord`).
`GET /v1/jobs/{id}` polls. Survives server restarts.

**Correctness evidence:**

- Integration tests in `pkg/jobrecord/record_test.go`
- Bench harness (`bench.sh`) uses the async API for every call; persistence verified by killing + restarting `janitor-server` mid-run

---

## Per-table in-flight dedup

**Mechanism.** `pkg/lease` writes a TTL'd lease at
`_janitor/state/leases/<ns>.<table>/<op>.lease` via S3 conditional create
(`If-None-Match: *`). Concurrent requests for the same (table, op)
return the existing job_id instead of starting a duplicate. Stale-lease
takeover via TTL.

**Correctness evidence:**

- Unit tests in `pkg/lease/lease_test.go` exercise CAS contention, TTL takeover, and concurrent acquire
- Bench Run 19 (3-replica): cross-replica dedup observed in CloudWatch logs ("returning in-flight job" entries match across server hostnames)

---

## Three runtime tiers

Same `pkg/janitor` core runs as:

- Long-lived HTTP server (`cmd/janitor-server`) — Knative / Fargate
- AWS Lambda handler (`cmd/janitor-lambda`)
- One-shot CLI (`cmd/janitor-cli`)

**Correctness evidence:**

- Each `cmd/*` builds and tests independently
- AWS bench runs the server in Fargate; CLI is used by `bench.sh local`; Lambda has its own test target

---

## Catalog-less directory catalog

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

**Mechanism.** `?dry_run=true` on all four maintenance endpoints. Cut
points run real manifest walks (so contention is detected) but stop
before any side effects. Reload-and-compare-snapshot-id at the end
probes for CAS contention an actual run would have hit.

**Correctness evidence:**

- Integration tests in `pkg/maintenance/dryrun_test.go` confirm no parquet writes, no transaction stage, no commit
- Contention probe verified in MinIO bench by triggering a foreign writer mid-walk

---

## Glue registration

**Mechanism.** `janitor-cli glue-register` registers the warehouse's
Iceberg tables with AWS Glue using `metadata_location` so Athena can
query them. Returns `metadata_location` in job result for external
callers.

**Correctness evidence:**

- AWS bench Run 18-20: Athena queries the Glue-registered tables with no manual SQL
- Sandbox NACL blocks Glue from Fargate; production deployments call externally — see memory note `glue_bottleneck.md`

---

## Honest gaps

This list is the inverse of the table above — features that are
designed but not yet shipped, or shipped with caveats:

- **V3 features** (deletion vectors, row lineage, Puffin stats): refused by safety gate; not implemented. Awaiting real V3-using tables.
- **Equality-delete end-to-end fixture**: see V2 deletes section above; runtime code is shipped + unit-tested, real-table integration test is GitHub issue #8.
- **Snapshot-side cleanup of consumed delete files**: orphan delete files survive compaction commit and rely on Expire + OrphanFiles for eventual cleanup.
- **q1 query latency outlier**: +32% on q1 since Run 8, never diagnosed. Tracked in future-tasks memory.
- **Pattern C (on-commit dispatcher)**: external SQS-driven dispatcher with dequeue-time dedup. GitHub issue #3 — not yet started.
- **Adaptive feedback loop**: per-table convergence on the right compute tier from history. Design plan #12, not implemented.

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

Numbers in this doc were captured on commit `7e174ea`
(`feature/v2-deletes`) on 2026-04-15. Newer runs append to
BENCHMARKS.md; correctness signals here update when the feature shape
changes.
