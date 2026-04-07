# janitor-server vs the Iceberg REST Catalog API

## TL;DR

**The janitor-server API is NOT a re-implementation of the Iceberg REST Catalog spec.** They overlap on table identity and metadata-pointer commit semantics, but they are answering different questions:

- **Iceberg REST Catalog** (`apache/iceberg/open-api`) is a **catalog protocol**: how a client loads, lists, creates, drops, and atomically updates tables. It's a service contract that any Iceberg engine (Spark, Trino, DuckDB, PyIceberg, iceberg-go) speaks to find and modify tables.
- **janitor-server** is a **maintenance operator**: how an authorized caller asks the janitor to compact / expire / clean up an existing Iceberg table. It does NOT provide a catalog service. The janitor's directory catalog is an internal implementation detail; it never exposes catalog operations over HTTP.

The architectural bet of the janitor (decision #3 in the design plan) is that **the Iceberg metadata files in object storage ARE the catalog** — there is no service. So we explicitly do NOT want to be on the Iceberg REST Catalog spec, because that would reintroduce the catalog service we just eliminated.

This document maps endpoint-by-endpoint where the surfaces overlap, where they're disjoint, and what the rationale is for each.

---

## Side-by-side endpoint mapping

The Iceberg REST Catalog spec is large (~30 endpoints across catalog config, namespaces, tables, views, transactions, scan planning). The janitor-server API is small (5 endpoints). Here's how they line up:

| Iceberg REST Catalog | janitor-server | Same? | Why |
|---|---|---|---|
| `GET /v1/config` | _(none)_ | ❌ | The janitor has no catalog config to negotiate. The warehouse URL is set at process startup via `JANITOR_WAREHOUSE_URL`; nothing about it is per-request. |
| `GET /v1/{prefix}/namespaces` | _(none)_ | ❌ | The janitor never models namespaces explicitly. They're derived from the object-store prefix layout. The list-tables endpoint (below) walks the warehouse and returns each table's prefix; an operator deduces namespaces from the prefixes. |
| `POST /v1/{prefix}/namespaces` (create namespace) | _(none)_ | ❌ | Catalog operation. Out of scope. |
| `DELETE /v1/{prefix}/namespaces/{ns}` | _(none)_ | ❌ | Catalog operation. Out of scope. |
| `GET /v1/{prefix}/namespaces/{ns}/tables` | `GET /v1/tables[?prefix=...]` | **partial** | Both list tables in a (possibly nested) namespace. Iceberg returns logical `(namespace, name)` identifiers from a metastore; janitor returns physical bucket-relative prefixes from a directory walk. Both work for "what tables exist?" but the wire formats are different. |
| `POST /v1/{prefix}/namespaces/{ns}/tables` (create table) | _(none)_ | ❌ | Catalog operation. The janitor does not create tables — it operates on tables that exist. (The fixture generator `cmd/janitor-seed` uses iceberg-go's SqlCatalog for write-side bookkeeping, but that is a test fixture, not part of the production server.) |
| `GET /v1/{prefix}/namespaces/{ns}/tables/{table}` (load table) | _(none, but `GET .../health` is close)_ | **partial** | Iceberg's "load table" returns the full `LoadTableResult` (metadata.json + io config). The janitor's `health` endpoint returns a *summary* (HealthReport) — not the raw metadata. An operator who actually wants the metadata reads it directly from the object store. |
| `POST /v1/{prefix}/namespaces/{ns}/tables/{table}` (commit) | _(none — exposed only to the janitor's own internal catalog)_ | ❌ | The Iceberg catalog commit endpoint is the load-bearing primitive of the REST Catalog spec — the metadata-pointer swap with requirements + updates. The janitor uses the same primitive **internally** via `pkg/catalog.DirectoryCatalog.CommitTable`, but does not expose it on the HTTP API. We do not want operators committing arbitrary updates; the janitor's HTTP API only exposes high-level *intents* (compact, expire, clean) that produce well-formed commits internally. |
| `DELETE /v1/{prefix}/namespaces/{ns}/tables/{table}` (drop) | _(none)_ | ❌ | Out of scope. The janitor never drops tables. |
| `POST /v1/{prefix}/tables/rename` | _(none)_ | ❌ | Out of scope. |
| `POST .../tables/{table}/metrics` (report) | _(none)_ | ❌ | The janitor's audit trail is the snapshot summary properties + `_janitor/results/<run_id>.json`, not metric reports. |
| `POST .../scan/plan` (scan planning) | _(none)_ | ❌ | The janitor doesn't expose query-time scan planning. iceberg-go does scan planning internally during compaction, but that's not a public surface. |
| _(none)_ | `GET /v1/healthz` | ❌ | Liveness probe. Knative / k8s convention; not part of the catalog spec. |
| _(none)_ | `GET /v1/readyz` | ❌ | Readiness probe (warehouse-reachable). Knative / k8s convention. |
| _(none)_ | `POST .../tables/{ns}/{table}/compact` | ❌ | **The janitor's reason for existing.** The Iceberg REST Catalog spec has no equivalent. The Iceberg Java Actions API has `RewriteDataFiles`, but that's a Java library API, not a REST endpoint. |

Plus the planned-but-not-yet-shipped maintenance endpoints (`expire-snapshots`, `delete-orphan-files`, `rewrite-manifests`, `rewrite-position-delete-files`, `compute-table-stats`, `compute-partition-stats`) — none of which are in the Iceberg REST Catalog spec, all of which are in the janitor's maintenance namespace.

---

## Where the overlap is real

There are exactly **two** places where the janitor-server API and the Iceberg REST Catalog spec genuinely overlap:

### 1. Table identity (URL path shape)

Both APIs use a `(namespace, table)` tuple in the URL path. The janitor-server uses `/v1/tables/{ns}/{name}/...`, the Iceberg spec uses `/v1/{prefix}/namespaces/{ns}/tables/{table}`. The differences:

- **No `{prefix}` segment** in the janitor — there's only one warehouse per server, set at startup
- **No multi-level namespace flattening** — the janitor's MVP supports two-part identifiers; the Iceberg spec supports nested namespaces with custom separators
- **`.db` suffix handling** — the janitor accepts the iceberg-go default-location-provider `.db` suffix in the URL (`mvp.db/events`) and strips it internally; the Iceberg REST spec doesn't have this convention

The shapes are similar enough that an operator who knows one can pattern-match the other, but they are not interchangeable.

### 2. The atomic commit semantics

Both APIs ultimately rely on the same correctness primitive: **a conditional metadata-pointer swap**. In the Iceberg REST Catalog spec, this is the `POST .../tables/{table}` endpoint with a `requirements` array (preconditions) and an `updates` array (deltas). The catalog service validates the requirements against the current metadata, applies the updates, computes the new metadata.json location, and persists it.

The janitor does **the exact same thing** internally — `pkg/catalog.DirectoryCatalog.CommitTable` validates requirements via `Requirement.Validate()`, applies updates via `iceberg-go`'s public `table.UpdateTableMetadata`, computes the next location via `LocationProvider.NewTableMetadataFileLocation`, and persists the new metadata.json with a conditional write. The implementation is the same shape as the SqlCatalog and RestCatalog implementations in iceberg-go.

**The difference is who calls it:**
- In the Iceberg REST Catalog spec, the COMMIT endpoint is called by ANY Iceberg client (Spark, Trino, DuckDB, etc.)
- In the janitor design, the commit is called only by the janitor's own maintenance ops, and the janitor's HTTP API exposes only high-level *intents* (compact, expire, clean) — not raw commit

This is a deliberate API-design decision: a maintenance operator should not be a general-purpose catalog. Letting external clients commit arbitrary updates through the janitor would defeat its master-check guarantees (an operator could commit a metadata change without going through `pkg/safety.VerifyCompactionConsistency`). By exposing only intents, every commit that flows through the janitor goes through the same mandatory verification path.

---

## Why the janitor doesn't (and shouldn't) implement the Iceberg REST Catalog spec

Several reasons, in roughly decreasing importance:

### 1. The architectural bet: no catalog service

Decision #3 in the design plan is "no catalog service — the Iceberg metadata files in object storage ARE the catalog." Implementing the Iceberg REST Catalog spec would mean **reintroducing the catalog service we just eliminated**. The whole point of the directory catalog is that an operator can drop the janitor onto an existing warehouse (populated by Spark, Flink, dbt, Tableflow, anything) and the janitor reads the metadata files directly. No client speaks to the janitor over HTTP to load tables — they speak to the object store, which is the source of truth.

If the janitor exposed a REST Catalog endpoint, operators would be tempted to point their Spark / Trino / DuckDB clients at it as a catalog. That would create a hard dependency on the janitor being up — which is exactly the failure mode we're avoiding. The janitor should be a stateless **side car** to the warehouse, not a service in front of it.

### 2. Different problem, different surface

The Iceberg REST Catalog spec is designed to answer "what tables exist? give me their metadata. let me commit a new snapshot." The janitor is designed to answer "is this table healthy? please compact it for me. show me the audit trail of recent maintenance runs."

These are different problems. The catalog protocol is about **table identity and metadata atomicity**; the janitor API is about **maintenance intents and verification results**. Mashing them into one API would muddle both.

### 3. The janitor does write to the catalog — but always through its own master check

When the janitor commits a compaction, it goes through `pkg/safety.VerifyCompactionConsistency` first. The verification is mandatory and non-bypassable. If we exposed `POST .../tables/{table}` (the raw Iceberg commit endpoint), an operator could commit changes without verification — losing the entire safety story. By exposing only `POST .../tables/{table}/compact` and (planned) `expire`, `cleanup`, etc., every mutation flows through the master check.

### 4. The HTTP API is small on purpose

Decision #20 in the plan: "operator CLI as the only control plane." The HTTP API is for the CLI (and for runbook automation that wants to shell out to `curl`), not for general-purpose Iceberg clients. Keeping the surface small is part of the operator-zero-touch principle: fewer endpoints means fewer security surfaces, fewer auth concerns, fewer things to version.

### 5. iceberg-go already speaks the REST Catalog spec when we need it

If a deployment ever needs to operate against an existing REST Catalog (as a *consumer* of catalog operations, not a *provider* of them), `iceberg-go` already has a `catalog/rest` implementation. The janitor's `pkg/catalog.DirectoryCatalog` is one of several catalog implementations that satisfy the same `iceberg-go` `Catalog` interface. We get the REST Catalog client for free; we just don't run a server.

---

## What about the Python implementation's REST API?

The Python `src/iceberg_janitor/api/` exposes 9 endpoints that mirror the **Iceberg Java Actions API** (not the REST Catalog spec). These are:

- `POST /v1/tables/{id}/rewrite-data-files` ← Java's `RewriteDataFiles`
- `POST /v1/tables/{id}/expire-snapshots` ← Java's `ExpireSnapshots`
- `POST /v1/tables/{id}/delete-orphan-files` ← Java's `DeleteOrphanFiles`
- `POST /v1/tables/{id}/rewrite-manifests` ← Java's `RewriteManifests`
- `POST /v1/tables/{id}/rewrite-position-delete-files` ← Java's `RewritePositionDeleteFiles`
- `POST /v1/tables/{id}/maintain` ← all of the above in recommended order
- `GET /v1/tables/{id}/health` ← no Java equivalent; janitor-specific
- `GET /v1/health` ← liveness
- `GET /v1/metrics` ← Prometheus

The Go `janitor-server` is **converging on the same pattern** but with slightly different conventions:
- `compact` instead of `rewrite-data-files` (clearer for non-Iceberg-experts; the URL is an intent, not a Java method name)
- The MVP only ships `compact`; the rest land as the maintenance ops do
- Verification records are returned in the response body and committed to snapshot summary properties (the Python implementation does not currently do the latter)
- Master check is mandatory in the Go version (the Python version trusts the underlying actions to be correct)

This is the right precedent to follow: the maintenance API mirrors the **Java Actions** API names, NOT the REST Catalog spec endpoints. They are different layers of the Iceberg ecosystem. The Java Actions API is the de facto standard for "what maintenance operations should an Iceberg tool support?" — that's the surface the janitor implements.

---

## Future: should we ever implement the REST Catalog spec?

There is one scenario where it would make sense: **read-only proxy mode**. If a deployment has a use case for letting query engines load tables through the janitor (e.g., to enforce read-time policies, to count queries for the access tracker, to add caching), the janitor could implement the read-side of the REST Catalog spec (`GET /v1/{prefix}/namespaces`, `GET .../tables`, `GET .../tables/{table}`) without implementing the write-side (`POST .../tables/{table}` commit).

This is a possible future enhancement, NOT in the design plan today. If you want it, file an issue with the use case. The current architectural position is: the janitor reads the warehouse directly (via its directory catalog) and does not relay catalog operations.

---

## Summary table

| Question | Iceberg REST Catalog | janitor-server |
|---|---|---|
| What is it? | Catalog protocol | Maintenance operator |
| Who runs it? | Confluent, Tabular, Polaris, Snowflake, anyone running an Iceberg metastore | Operators of an Iceberg warehouse who want maintenance |
| What does it manage? | Table identity + metadata atomicity | Compaction, expire, orphans, manifest rewrite, statistics |
| Source of truth | The catalog service | The Iceberg metadata files in the warehouse object store |
| Auth model | Catalog-defined (often OAuth2, SigV4, etc.) | Delegated to deployment platform |
| Does it create tables? | Yes | No |
| Does it commit metadata changes? | Yes (general-purpose, called by any client) | Yes (only via maintenance intents, mandatory master check) |
| Does it expose Iceberg `LoadTable`? | Yes (returns LoadTableResult) | No (returns HealthReport summary) |
| Multi-cloud? | Catalog-implementation-specific | Yes (gocloud.dev/blob, all four targets) |
| Hard dependency on a service? | Yes — the catalog service itself is the dependency | No — janitor is stateless side car to the warehouse |

The two APIs are **complementary**, not redundant. A deployment can run an Iceberg REST Catalog (or use Glue, Hive, etc.) for the engine clients AND run the janitor against the same warehouse for maintenance. The janitor reads the warehouse directly via its directory catalog regardless of what catalog service the engines use.
