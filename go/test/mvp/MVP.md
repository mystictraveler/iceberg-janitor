# iceberg-janitor (Go) — MVP test loop

A reproducible end-to-end loop that exercises the read path of the Go janitor
against a real Iceberg table written by `iceberg-go`, with DuckDB as the
independent query engine for round-trip verification.

## What's in the loop

- **Seed** (`cmd/janitor-seed`) — pure-Go program. Uses `iceberg-go`'s
  `catalog/sql` with a pure-Go sqlite driver to create an Iceberg table at a
  configured warehouse URL, then calls `Table.AppendTable` in a loop to write
  N small Arrow batches. Each batch becomes one snapshot, one manifest list,
  one manifest file, and one (small) data file — exactly the streaming-churn
  shape the janitor exists to fix.
- **Janitor CLI** (`cmd/janitor-cli`) — the system under test. `discover`
  walks the warehouse and finds Iceberg tables by their on-disk layout (no
  catalog service). `analyze` loads a table via the directory catalog,
  walks its manifests via `iceberg-go`, and prints a `HealthReport` with the
  metadata-to-data ratio and the small-file ratio.
- **DuckDB** — independent query engine. Reads the same Iceberg table via
  the `iceberg_scan` function and confirms the data round-trips correctly.
  Used for both the read-path verification AND (eventually) the
  before/after compaction query latency benchmark.

## Prerequisites

- Go 1.23+ (the module compiles cleanly with 1.24)
- `duckdb` CLI 1.4+ (any recent version with the `iceberg` extension)
- (Optional, for the docker path) Docker Desktop or `colima`

No Python, no pyiceberg, no REST catalog. The seed binary's sqlite catalog
is a *write-side convenience* for table creation; the janitor itself never
touches the sqlite file. The Go side is no-catalog-service on purpose.

## Quick start (local filesystem, no Docker)

From the repository root:

```bash
# 1. Seed an Iceberg table at /tmp/janitor-mvp with 10 batches × 10000 rows.
make mvp-seed-local MVP_NUM_BATCHES=10 MVP_ROWS_PER_BATCH=10000

# 2. Discover what the janitor sees.
make mvp-discover-local

# 3. Analyze the table — this is the read path under test.
make mvp-analyze-local

# 4. Round-trip verification: query the same table from DuckDB.
make mvp-query-local
```

Expected output of step 3:

```
Table:               mvp.db/events
Format version:      v2
Current snapshot:    8405430320815646982
Snapshot count:      10

Data files:          10
Data bytes:          200.8 KiB
Total rows:          100000

Small file threshold:64.0 MiB
Small files:         10 (100.0% of all data files)
Small file bytes:    200.8 KiB

Manifests:           10
Manifest bytes:      37.8 KiB

Metadata bytes:      37.8 KiB
Metadata/data ratio: 18.8121%   <-- the H1 axiom (CB3): healthy < 5%, critical > 10%

STATUS: CRITICAL — metadata-to-data ratio 18.81% exceeds critical threshold 10% — janitor maintenance is BLOCKED
```

The critical status is **expected** on a synthetic seed with tiny batches —
manifest overhead really does dwarf the data here, which is exactly the
pathology the metadata-to-data axiom (CB3) catches. With realistic batch
sizes (Spark / Flink streaming sinks producing 5-50 MB files) the ratio
will sit comfortably under 1%, and only an unhealthy table will trip the
threshold.

Step 4 (DuckDB) prints:

```
metadata: /tmp/janitor-mvp/mvp.db/events/metadata/00010-...metadata.json
┌───────────┬────────────────┬─────────────┐
│ row_count │ distinct_users │ event_types │
│   int64   │     int64      │    int64    │
├───────────┼────────────────┼─────────────┤
│    100000 │          10000 │           6 │
└───────────┴────────────────┴─────────────┘
```

The same 100,000 rows the Go seed wrote, queried by DuckDB through the
standard Iceberg table format. This is the round-trip proof.

## Quick start (MinIO via Docker)

Same loop, against a MinIO bucket instead of the local filesystem:

```bash
# 1. Bring up MinIO + an init container that creates the warehouse bucket.
make mvp-up

# 2. Seed.
make mvp-seed MVP_NUM_BATCHES=10 MVP_ROWS_PER_BATCH=10000

# 3. Discover.
make mvp-discover

# 4. Analyze.
make mvp-analyze

# 5. Tear down.
make mvp-down
```

`mvp-query` (DuckDB against the MinIO warehouse) is not yet wired —
DuckDB's `iceberg_scan` needs httpfs configured for s3 endpoints. Easy to
add when needed.

## Configuration knobs

| Make variable        | Default                                  | Effect |
|----------------------|------------------------------------------|--------|
| `MVP_NUM_BATCHES`    | 100                                      | Number of micro-batches the seed writes |
| `MVP_ROWS_PER_BATCH` | 200                                      | Rows per batch — smaller = more pathological metadata ratio |
| `MVP_NAMESPACE`      | `mvp`                                    | Iceberg namespace for the seed table |
| `MVP_TABLE`          | `events`                                 | Iceberg table name |
| `MVP_WAREHOUSE_LOCAL`| `file:///tmp/janitor-mvp`                | Local warehouse URL |
| `MVP_WAREHOUSE_S3`   | `s3://warehouse?endpoint=...`            | MinIO warehouse URL |

The seed binary also reads `JANITOR_WAREHOUSE_URL`, `NAMESPACE`, `TABLE`,
`NUM_BATCHES`, `ROWS_PER_BATCH`, `CATALOG_DB`, and `S3_ENDPOINT` /
`S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_REGION` directly from the
environment, so you can run it standalone without `make`.

## What's not yet in the MVP

- **Compaction** — `cmd/janitor-cli compact` does not exist yet. The
  analyzer correctly identifies the table as needing maintenance; the
  next iteration will add the maintenance write path so the loop becomes
  seed → analyze → compact → analyze (improvement) → DuckDB query.
- **Master check (pkg/safety)** — the I1–I9 invariants will be enforced
  on every commit. Currently we only have the read path so there's
  nothing to check yet.
- **Circuit breakers (pkg/safety/circuitbreaker.go)** — referenced in the
  plan; not yet wired into the orchestrator (which itself doesn't exist
  yet).
- **MinIO + DuckDB query** — `mvp-query` only works against the local
  fileblob path right now. Adding httpfs config to query against S3 is
  the obvious next step.
- **Workload classifier, feedback loop, tier dispatch, all of Phase 2 and
  beyond** — see `/Users/jp/.claude/plans/async-plotting-cake.md` for
  the full phased plan.

## Architecture sanity check (what this loop proves)

The MVP demonstrates that **the no-catalog-service design actually works
end-to-end across two independent implementations**:

1. The Go seed writes an Iceberg table to a warehouse using
   `iceberg-go`'s `catalog/sql` for write-time bookkeeping.
2. The Go janitor reads the same warehouse using its **directory catalog**
   (no service, max-version scan over `metadata/v*.metadata.json`) and
   produces a HealthReport via `iceberg-go`'s manifest reader. **The
   janitor never touches the sqlite catalog file.**
3. DuckDB reads the same warehouse using *its* iceberg extension and the
   data round-trips correctly.

Three independent code paths converge on the same Iceberg table layout.
That's the validation that the design assumption — "the metadata files in
object storage ARE the catalog" — is correct in practice, not just in
theory.
