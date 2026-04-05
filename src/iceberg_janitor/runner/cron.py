"""CronJob entrypoint: single-shot maintenance cycle for K8s CronJob execution."""

from __future__ import annotations

import time
from pathlib import Path

import structlog
import yaml

from iceberg_janitor.analyzer.health import assess_table
from iceberg_janitor.catalog import get_catalog
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


def run_maintenance_cycle(config_path: str) -> dict:
    """Execute a full maintenance cycle: analyze all tables, evaluate policies, run actions.

    This is the main entrypoint for the K8s CronJob. It:
    1. Loads policy config from YAML
    2. Connects to the catalog
    3. Lists tables (or uses configured table list)
    4. For each table: analyze -> evaluate policy -> execute maintenance
    5. Emits Prometheus metrics

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Summary dict with per-table results.
    """
    log = logger.bind(config_path=config_path)
    log.info("maintenance_cycle_starting")

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    # Extract catalog config and policy config
    catalog_config = raw_config.get("catalog", {})
    policy_config = PolicyConfig.model_validate(raw_config.get("policy", {}))
    table_ids: list[str] = raw_config.get("tables", [])

    # Connect to catalog
    catalog = get_catalog(
        name=catalog_config.get("name", "default"),
        uri=catalog_config.get("uri"),
        warehouse=catalog_config.get("warehouse"),
        catalog_type=catalog_config.get("type", "rest"),
        **{k: v for k, v in catalog_config.items() if k not in ("name", "uri", "warehouse", "type")},
    )

    # If no explicit table list, discover tables from catalog
    if not table_ids:
        log.info("discovering_tables")
        for namespace in catalog.list_namespaces():
            ns_name = ".".join(namespace)
            for table_ident in catalog.list_tables(namespace):
                table_ids.append(f"{ns_name}.{table_ident[-1]}")
        log.info("tables_discovered", count=len(table_ids))

    results = {}

    for table_id in table_ids:
        table_log = log.bind(table_id=table_id)
        table_log.info("processing_table")

        try:
            table = catalog.load_table(table_id)
            table_path = table.metadata.location

            # Analyze
            report = assess_table(
                table_path,
                small_file_threshold=policy_config.get_policy(table_id).small_file_threshold_bytes,
            )
            update_health_metrics(table_id, report)

            # Evaluate
            policy = policy_config.get_policy(table_id)
            actions = evaluate(report, policy)

            table_results = {"actions_planned": len(actions), "actions_executed": []}

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

                except Exception as e:
                    table_log.error("action_failed", action=action.action_type.value, error=str(e))
                    if action.action_type == ActionType.COMPACT_FILES:
                        compaction_runs.labels(table_id=table_id, status="error").inc()
                    elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                        snapshot_expirations.labels(table_id=table_id, status="error").inc()

            results[table_id] = table_results

        except Exception as e:
            table_log.error("table_processing_failed", error=str(e))
            results[table_id] = {"error": str(e)}

    log.info("maintenance_cycle_complete", tables_processed=len(results))
    return results
