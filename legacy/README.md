# Legacy: Python + Flink implementation

This directory preserves the original Python/Flink implementation of
iceberg-janitor. It is **no longer maintained** — the Go rewrite in
[`../go/`](../go) is the current production path.

Kept here for safe keeping: the architectural decisions, TPC-DS
benchmark methodology, and multi-region strategy documented in this
tree informed the Go design. The code itself is frozen.

## Contents

| Path | What it is |
|---|---|
| `src/iceberg_janitor/` | Python implementation (Knative-deployed, polymorphic catalogs) |
| `flink-jobs/` | Java FlinkCompactionJob (retired — replaced by Go byte-copy stitch) |
| `docker/`, `helm/`, `manifests/` | K8s/Knative deployment artifacts |
| `jmeter/` | DuckDB query benchmark suite |
| `openapi/iceberg-janitor-api-v2.yaml` | Python REST API spec (the Go spec is at `../go/openapi/`) |
| `scripts/` | TPC-DS schema, data generators, setup scripts |
| `tests/` | pytest suite: analyzer + feedback loop + policy + TPC-DS benchmark |
| `pyproject.toml`, `Makefile` | Python build |
| `flink-sizing.md` | Flink TaskManager sizing guide (superseded by tier dispatch in Go) |
| `research-tableflow-comparison.md` | Earlier research on Confluent Tableflow |
| `research-iceberg-maintenance-api.md` | Gap analysis of REST Catalog spec |
| `multiregion-strategy.md` | Cost simulation: local compaction vs cross-region shipping |

## The full Python branch

If you need the full git history of the Python implementation (not
just the snapshot preserved here), it's available at
[`reference/python-v0`](https://github.com/mystictraveler/iceberg-janitor/tree/reference/python-v0).

## Why the rewrite

Per the 27-decision design plan, the Go rewrite:

- Removed the catalog service requirement (directory catalog on object storage)
- Replaced Flink compaction with byte-copy stitch + row group merge
- Added mandatory pre-commit master check (I1-I9 invariants)
- Achieved sub-200ms cold start on Knative/Lambda vs minutes for JVM
- Delivered 192× file reduction and 23-27% faster Athena queries on the bench

See [`../README.md`](../README.md) for the current architecture and
[`../go/BENCHMARKS.md`](../go/BENCHMARKS.md) for measured results.
