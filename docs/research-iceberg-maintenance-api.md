# Iceberg Table Maintenance API: Spec Alignment & Simplification

> **Date:** 2026-04-05
> **Context:** Aligning iceberg-janitor's REST API with the canonical Iceberg maintenance actions and comparing against the Iceberg REST Catalog spec

---

## 1. Key Finding: Iceberg REST Catalog Has No Maintenance Endpoints

The official [Iceberg REST Catalog OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) defines **zero maintenance endpoints**. It is strictly a metadata catalog protocol:

| What it covers | What it does NOT cover |
|---|---|
| Namespace CRUD | Compaction |
| Table CRUD + commit | Snapshot expiration |
| Scan planning (new in 1.10+) | Orphan file cleanup |
| Credentials + request signing | Manifest rewriting |
| Views CRUD | Delete file compaction |
| Multi-table transactions | Health assessment |
| Metrics reporting | Policy management |

This is intentional — the REST Catalog is a thin metadata protocol using optimistic concurrency. Maintenance requires compute orchestration, which is a fundamentally different concern.

**iceberg-janitor fills this gap.** No open standard exists for Iceberg table maintenance via REST.

---

## 2. The 5 Canonical Iceberg Maintenance Actions

Iceberg defines maintenance as **client-side actions** in `org.apache.iceberg.actions` (Java) and as Spark Procedures. These are the authoritative operations:

### 2.1 `rewriteDataFiles` (Compaction)

Rewrites small data files into larger ones to reduce file count and improve scan performance.

| Parameter | Description | Default |
|---|---|---|
| `strategy` | binpack, sort, or zorder | binpack |
| `target-file-size-bytes` | Target output file size | 512 MB (Spark) |
| `min-file-size-bytes` | Files below this are candidates | 75% of target |
| `max-file-size-bytes` | Files above this are left alone | 180% of target |
| `min-input-files` | Minimum files in a group to trigger rewrite | 5 |
| `partial-progress.enabled` | Commit partial results for large jobs | false |
| `where` | Partition filter to scope compaction | — |

### 2.2 `expireSnapshots`

Removes old snapshots and their unreferenced metadata. Data files become orphan candidates.

| Parameter | Description | Default |
|---|---|---|
| `older_than` | Expire snapshots before this timestamp | 5 days ago |
| `retain_last` | Always keep at least N snapshots | 1 |
| `max_concurrent_deletes` | Parallel file deletions | 10000 |

### 2.3 `deleteOrphanFiles`

Removes files in the table's storage that are not referenced by any snapshot.

| Parameter | Description | Default |
|---|---|---|
| `location` | Storage path to scan | Table location |
| `older_than` | Safety margin (only delete files older than this) | 3 days ago |
| `dry_run` | List orphans without deleting | false |

### 2.4 `rewriteManifests`

Consolidates many small manifest files into fewer larger ones, reducing query planning overhead.

| Parameter | Description | Default |
|---|---|---|
| `use-caching` | Cache manifest entries during rewrite | false |

### 2.5 `rewritePositionDeleteFiles` (V2/V3)

Compacts position delete files by merging them into their associated data files. In V3, this also applies deletion vectors.

| Parameter | Description | Default |
|---|---|---|
| Same options as rewriteDataFiles | Strategy, file size targets, partition filter | — |

---

## 3. Iceberg Format Versions and Maintenance Implications

### V1 (Original)
- Append-only or overwrite-only
- Maintenance: `rewriteDataFiles`, `expireSnapshots`, `deleteOrphanFiles`, `rewriteManifests`

### V2 (Row-Level Deletes)
- Adds position delete files and equality delete files
- New maintenance action: `rewritePositionDeleteFiles` to compact delete files
- Compaction (`rewriteDataFiles`) applies pending deletes during rewrite

### V3 (Latest Ratified — 2025)
- **Deletion Vectors**: Roaring bitmaps in Puffin files replacing position delete files
  - At most one deletion vector per data file per snapshot
  - More efficient merge-on-read than V2 position deletes
- **Default Column Values**: Schema evolution without data rewrite
- **Row-Level Lineage**: Track when rows were added/modified
- **New Types**: `variant`, `geometry`, `geography`, `timestamp_ns`
- **Maintenance impact**: `rewritePositionDeleteFiles` now also handles deletion vector compaction
- Existing actions work transparently on V3 tables

### V4 (Under Development — NOT Ratified)
- Under active development, no formal spec yet
- No publicly documented new maintenance actions

---

## 4. API Simplification: V1 → V2

The original iceberg-janitor API (v1) had 17 endpoints across 6 tag groups with separate concerns for tables, health, maintenance, policy, metadata inspection, and operations.

The simplified API (v2) reduces to **9 endpoints** that map directly to Iceberg's action vocabulary:

### V2 Endpoint Map

| Endpoint | Iceberg Action | Purpose |
|---|---|---|
| `GET /v1/tables/{id}/health` | — | DuckDB-based health assessment (janitor-specific, no Iceberg equivalent) |
| `POST /v1/tables/{id}/rewrite-data-files` | `rewriteDataFiles` | Compaction (binpack/sort/zorder) |
| `POST /v1/tables/{id}/expire-snapshots` | `expireSnapshots` | Snapshot expiration |
| `POST /v1/tables/{id}/delete-orphan-files` | `deleteOrphanFiles` | Orphan file removal |
| `POST /v1/tables/{id}/rewrite-manifests` | `rewriteManifests` | Manifest consolidation |
| `POST /v1/tables/{id}/rewrite-position-delete-files` | `rewritePositionDeleteFiles` | V2/V3 delete file compaction |
| `POST /v1/tables/{id}/maintain` | All of the above | Full maintenance cycle |
| `GET /v1/health` | — | Service health |
| `GET /v1/metrics` | — | Prometheus metrics |

### What Was Removed

| V1 Endpoint | Reason for Removal |
|---|---|
| `GET /api/v1/tables` | Use the Iceberg REST Catalog `GET /v1/namespaces/{ns}/tables` instead |
| `POST /api/v1/tables/{id}/analyze` | Merged into `GET .../health` (synchronous) |
| `GET /api/v1/tables/{id}/snapshots` | Use the Iceberg REST Catalog `GET /v1/.../tables/{table}` metadata |
| `GET /api/v1/tables/{id}/files` | Derivable from health report + catalog metadata |
| `GET /api/v1/tables/{id}/partitions` | Folded into health report's `hot_partitions` |
| `GET/PUT /api/v1/tables/{id}/policy` | Policy is config-driven (ConfigMap), not a runtime API concern |
| `GET /api/v1/tables/{id}/maintenance/history` | Derivable from Prometheus metrics + logs |
| `POST .../optimize-sort` | Merged into `rewrite-data-files` with `strategy: sort` |
| `POST .../run-all` | Renamed to `POST .../maintain` for clarity |

### What Was Added

| V2 Addition | Reason |
|---|---|
| `POST .../rewrite-position-delete-files` | Covers V2/V3 delete file compaction — a canonical Iceberg action missing from V1 |
| `strategy` field on `rewrite-data-files` | Exposes binpack/sort/zorder — aligns with Iceberg's `RewriteDataFiles` API |
| `where` filter on compaction endpoints | Partition-scoped operations — aligns with Iceberg's filter parameter |
| `delete_files` in health report | V2/V3 awareness: reports position delete files and deletion vectors |
| `format_version` in health report | Callers need to know if V2/V3 specific actions apply |

---

## 5. Design Principles

### 5.1 Name endpoints after Iceberg actions

V1 used generic names (`compact`, `remove-orphans`). V2 uses the exact Iceberg action names (`rewrite-data-files`, `delete-orphan-files`). This makes the API self-documenting for anyone familiar with Iceberg.

### 5.2 Don't duplicate the REST Catalog

The Iceberg REST Catalog already handles table listing, metadata reads, and schema inspection. The janitor API should not reimplement these. If a caller needs snapshot details, they query the catalog.

### 5.3 Policy is configuration, not API

Runtime policy changes via PUT are an anti-pattern for K8s-native services. Policy belongs in ConfigMaps/Helm values, versioned in git, applied via `kubectl`. The API executes actions; the config defines when and how aggressively.

### 5.4 Health assessment is the unique value

The `GET .../health` endpoint has no Iceberg equivalent. DuckDB-based metadata analysis — file size distributions, small-file ratios, partition hotspots — is what differentiates iceberg-janitor from running raw Iceberg actions.

### 5.5 Every maintenance endpoint supports dry_run

This is non-negotiable for production use. The Iceberg Java API supports dry_run on `deleteOrphanFiles` but not on other actions. The janitor API adds it uniformly.

---

## 6. Gap Analysis: What the Iceberg Ecosystem Lacks

| Capability | Iceberg Spec | REST Catalog | Spark Procedures | iceberg-janitor |
|---|---|---|---|---|
| Compaction | Defined as action | No | Yes | Yes (REST + CLI) |
| Snapshot expiry | Defined as action | No | Yes | Yes (REST + CLI) |
| Orphan cleanup | Defined as action | No | Yes | Yes (REST + CLI) |
| Manifest rewrite | Defined as action | No | Yes | Yes (REST + CLI) |
| Delete file compaction | Defined as action | No | Yes | Yes (REST + CLI) |
| Health assessment | Not defined | No | No | Yes |
| Adaptive scheduling | Not defined | No | No | Yes |
| Access-based priority | Not defined | No | No | Yes |
| Maintenance feedback loop | Not defined | No | No | Yes |
| Per-table policy | Not defined | No | No | Yes |
| K8s-native scheduling | Not defined | No | No | Yes |

The ecosystem gap is clear: Iceberg defines **what** maintenance actions exist but provides no standard for **when** or **how aggressively** to run them, no health assessment, and no REST API for triggering them. Every vendor (AWS, Databricks, Snowflake, Confluent) builds proprietary solutions. iceberg-janitor is an open-source alternative.

---

## 7. References

- [Iceberg Spec (Format V1/V2/V3)](https://iceberg.apache.org/spec/)
- [Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Iceberg Maintenance Docs](https://iceberg.apache.org/docs/latest/maintenance/)
- [Iceberg Spark Procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Iceberg V3 Deletion Vectors (AWS Blog)](https://aws.amazon.com/blogs/big-data/unlock-the-power-of-apache-iceberg-v3-deletion-vectors-on-amazon-emr/)
- [Iceberg 1.10 REST API + V3 (Google Blog)](https://opensource.googleblog.com/2025/09/apache-iceberg-110-maturing-the-v3-spec-the-rest-api-and-google-contributions.html)
- [PyIceberg Maintenance Issue #1065](https://github.com/apache/iceberg-python/issues/1065) — closed as "not planned" (Dec 2025)
