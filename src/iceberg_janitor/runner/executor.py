"""Shared maintenance execution logic used by both CronJob and Knative entrypoints."""

from __future__ import annotations

import time

import structlog
from pyiceberg.catalog import Catalog

from iceberg_janitor.analyzer.health import assess_table
from iceberg_janitor.maintenance.compaction import compact_files
from iceberg_janitor.maintenance.orphans import remove_orphans
from iceberg_janitor.maintenance.snapshots import expire_snapshots
from iceberg_janitor.metrics.prometheus import (
    compaction_runs,
    maintenance_duration_seconds,
    orphan_removals,
    orphans_removed_count,
    snapshot_expirations,
    snapshots_expired_count,
    update_health_metrics,
)
from iceberg_janitor.policy.engine import evaluate
from iceberg_janitor.policy.models import ActionType, PolicyConfig

logger = structlog.get_logger()


def execute_table_maintenance(
    catalog: Catalog,
    table_id: str,
    policy_config: PolicyConfig,
) -> dict:
    """Run the analyze -> evaluate -> execute cycle for a single table.

    Args:
        catalog: PyIceberg catalog connection.
        table_id: Fully-qualified table identifier (e.g. ``ns.table``).
        policy_config: Policy configuration (provides per-table overrides).

    Returns:
        Dict with ``actions_planned``, ``actions_executed`` keys on success,
        or an ``error`` key on failure.
    """
    log = logger.bind(table_id=table_id)
    log.info("processing_table")

    try:
        table = catalog.load_table(table_id)
        table_path = table.metadata.location

        # Analyze
        policy = policy_config.get_policy(table_id)
        report = assess_table(
            table_path,
            small_file_threshold=policy.small_file_threshold_bytes,
        )
        update_health_metrics(table_id, report)

        # Evaluate
        actions = evaluate(report, policy)

        table_results: dict = {"actions_planned": len(actions), "actions_executed": []}

        # Execute
        for action in actions:
            start = time.time()
            try:
                if action.action_type == ActionType.COMPACT_FILES:
                    result = compact_files(catalog, table_id, **action.params)
                    compaction_runs.labels(table_id=table_id, status="success").inc()

                elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                    result = expire_snapshots(catalog, table_id, **action.params)
                    snapshot_expirations.labels(table_id=table_id, status="success").inc()
                    snapshots_expired_count.labels(table_id=table_id).inc(
                        result.get("expired", 0)
                    )

                elif action.action_type == ActionType.REMOVE_ORPHANS:
                    result = remove_orphans(catalog, table_id, **action.params)
                    orphan_removals.labels(table_id=table_id, status="success").inc()
                    orphans_removed_count.labels(table_id=table_id).inc(
                        result.get("removed", 0)
                    )
                else:
                    continue

                duration = time.time() - start
                maintenance_duration_seconds.labels(
                    table_id=table_id, action_type=action.action_type.value
                ).observe(duration)

                table_results["actions_executed"].append({
                    "action": action.action_type.value,
                    "result": result,
                    "duration_seconds": round(duration, 2),
                })

            except Exception as exc:
                log.error("action_failed", action=action.action_type.value, error=str(exc))
                if action.action_type == ActionType.COMPACT_FILES:
                    compaction_runs.labels(table_id=table_id, status="error").inc()
                elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                    snapshot_expirations.labels(table_id=table_id, status="error").inc()

        return table_results

    except Exception as exc:
        log.error("table_processing_failed", error=str(exc))
        return {"error": str(exc)}


def execute_full_cycle(
    catalog: Catalog,
    policy_config: PolicyConfig,
    table_ids: list[str] | None = None,
) -> dict:
    """Run maintenance for multiple tables (discovers tables when *table_ids* is ``None``).

    Args:
        catalog: PyIceberg catalog connection.
        policy_config: Policy configuration.
        table_ids: Explicit list of tables; ``None`` to discover from the catalog.

    Returns:
        Dict keyed by table identifier with per-table result dicts.
    """
    log = logger.bind()
    log.info("maintenance_cycle_starting")

    if table_ids is None:
        log.info("discovering_tables")
        table_ids = []
        for namespace in catalog.list_namespaces():
            ns_name = ".".join(namespace)
            for table_ident in catalog.list_tables(namespace):
                table_ids.append(f"{ns_name}.{table_ident[-1]}")
        log.info("tables_discovered", count=len(table_ids))

    results: dict = {}
    for table_id in table_ids:
        results[table_id] = execute_table_maintenance(catalog, table_id, policy_config)

    log.info("maintenance_cycle_complete", tables_processed=len(results))
    return results
