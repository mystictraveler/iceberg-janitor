"""Policy engine: evaluate a health report against thresholds and emit maintenance actions."""

from __future__ import annotations

import structlog

from iceberg_janitor.analyzer.health import HealthReport
from iceberg_janitor.policy.models import ActionType, MaintenanceAction, TablePolicy

logger = structlog.get_logger()


def evaluate(report: HealthReport, policy: TablePolicy) -> list[MaintenanceAction]:
    """Evaluate a table's health against its policy and return required maintenance actions.

    Actions are returned in priority order (highest first).
    """
    if not policy.enabled:
        return []

    actions: list[MaintenanceAction] = []
    log = logger.bind(table_id=report.table_id)

    # Check compaction needs
    if report.file_stats.small_file_ratio > policy.max_small_file_ratio:
        actions.append(
            MaintenanceAction(
                action_type=ActionType.COMPACT_FILES,
                table_id=report.table_id,
                priority=10,
                params={
                    "target_file_size_bytes": policy.target_file_size_bytes,
                    "small_file_threshold_bytes": policy.small_file_threshold_bytes,
                },
                reason=(
                    f"Small file ratio {report.file_stats.small_file_ratio:.1%} "
                    f"exceeds threshold {policy.max_small_file_ratio:.1%} "
                    f"({report.file_stats.small_file_count}/{report.file_stats.file_count} files)"
                ),
            )
        )
        log.info("compaction_needed", ratio=report.file_stats.small_file_ratio)

    elif report.file_stats.file_count > policy.max_file_count:
        actions.append(
            MaintenanceAction(
                action_type=ActionType.COMPACT_FILES,
                table_id=report.table_id,
                priority=8,
                params={"target_file_size_bytes": policy.target_file_size_bytes},
                reason=(
                    f"File count {report.file_stats.file_count} "
                    f"exceeds threshold {policy.max_file_count}"
                ),
            )
        )
        log.info("compaction_needed_file_count", count=report.file_stats.file_count)

    # Check snapshot expiration needs
    if report.snapshot_stats.snapshot_count > policy.max_snapshots:
        actions.append(
            MaintenanceAction(
                action_type=ActionType.EXPIRE_SNAPSHOTS,
                table_id=report.table_id,
                priority=7,
                params={
                    "retention_hours": policy.snapshot_retention_hours,
                    "min_snapshots_to_keep": policy.min_snapshots_to_keep,
                },
                reason=(
                    f"Snapshot count {report.snapshot_stats.snapshot_count} "
                    f"exceeds threshold {policy.max_snapshots}"
                ),
            )
        )
        log.info("snapshot_expiry_needed", count=report.snapshot_stats.snapshot_count)

    # Orphan cleanup is always recommended (low priority, safe operation)
    actions.append(
        MaintenanceAction(
            action_type=ActionType.REMOVE_ORPHANS,
            table_id=report.table_id,
            priority=3,
            params={"retention_hours": policy.orphan_retention_hours},
            reason="Routine orphan file cleanup",
        )
    )

    # Sort by priority descending
    actions.sort(key=lambda a: a.priority, reverse=True)
    return actions
