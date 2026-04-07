# iceberg-janitor (Go)

Catalog-less, multi-cloud, serverless Iceberg table maintenance.

This is a sibling Go implementation of the Python `iceberg-janitor` in this repo. It targets sub-200ms cold starts on Knative scale-to-zero and AWS Lambda by going directly against object storage (S3, MinIO, Azure Blob, GCS) — no Iceberg REST Catalog, no Glue, no metastore.

See `/Users/jp/.claude/plans/async-plotting-cake.md` for the design and phased delivery plan.

## Layout

```
pkg/
  blob/          multi-cloud object store abstraction (gocloud.dev/blob)
  fileio/        Iceberg FileIO bridging blob -> apache/iceberg-go
  catalog/       FileIO catalog: discovery + atomic metadata commit
  iceberg/       wrapper over apache/iceberg-go for the reads we use
  analyzer/      HealthReport (port of Python analyzer)
  policy/        TablePolicy + evaluation engine
  strategy/      triggers, scheduler, access tracker, feedback loop
  maintenance/   compact, expire snapshots, remove orphans, rewrite manifests
  orchestrator/  pure-function state machine
  janitor/       top-level core: ProcessTable(ctx, table)
  state/         persistent janitor state in object storage
cmd/
  janitor-cli/    local CLI (analyze / maintain / inspect)
  janitor-server/ Knative HTTP server adapter
  janitor-lambda/ AWS Lambda handler adapter
internal/
  metrics/  Prometheus exporter
  log/      slog setup
test/
  integration/  end-to-end tests against MinIO / Azurite / fake-gcs-server
  benchmark/    parity benchmark vs the Python implementation
```

## Status

Phase 1 shipped + naive Phase 3: blob layer, directory catalog read AND write, analyzer with H1 metadata-data ratio check, CLI `discover` / `analyze` / `compact`, Go-only seed binary, master check (I1 row count), end-to-end loop verified against both local fileblob and MinIO.

**Measured benchmark results live at [BENCHMARKS.md](BENCHMARKS.md)** and are updated every time a new build phase lands. That file is the single source of truth for "how well does it actually work right now?"

**For an architectural comparison against [Confluent Tableflow](https://www.confluent.io/product/tableflow/)'s compaction subsystem**, see [TABLEFLOW_COMPARISON.md](TABLEFLOW_COMPARISON.md). TL;DR: same correctness story with stronger guarantees (mandatory master check, snapshot-internal audit), no managed control plane, multi-cloud by construction, zero idle cost, and an open-source compaction algorithm you can audit in an afternoon.

## MVP test loop

See [test/mvp/MVP.md](test/mvp/MVP.md) for the runbook. TL;DR:

```bash
make mvp-seed-local MVP_NUM_BATCHES=20 MVP_ROWS_PER_BATCH=5000
make mvp-discover-local
make mvp-analyze-local
cd go && JANITOR_WAREHOUSE_URL=file:///tmp/janitor-mvp go run ./cmd/janitor-cli compact mvp.db/events
make mvp-analyze-local
make mvp-query-local       # DuckDB round-trip verification
```

For the same loop against MinIO over docker compose, replace `-local` with the docker variants (`mvp-up`, `mvp-seed`, `mvp-discover`, `mvp-analyze`).
