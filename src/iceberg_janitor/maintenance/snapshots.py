"""Snapshot expiration operations via PyIceberg."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog

from pyiceberg.catalog import Catalog

logger = structlog.get_logger()


def expire_snapshots(
    catalog: Catalog,
    table_id: str,
    retention_hours: int = 168,
    min_snapshots_to_keep: int = 5,
    dry_run: bool = False,
) -> dict:
    """Expire old snapshots from an Iceberg table.

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier (namespace.table).
        retention_hours: Expire snapshots older than this many hours.
        min_snapshots_to_keep: Always retain at least this many snapshots.
        dry_run: If True, report what would be expired without actually expiring.

    Returns:
        Dict with expiration results (expired_count, retained_count, etc.).
    """
    log = logger.bind(table_id=table_id, retention_hours=retention_hours)
    log.info("expiring_snapshots")

    table = catalog.load_table(table_id)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

    # Get current snapshots sorted by timestamp (newest first)
    snapshots = sorted(
        table.metadata.snapshots,
        key=lambda s: s.timestamp_ms,
        reverse=True,
    )

    total = len(snapshots)
    to_keep = max(min_snapshots_to_keep, 0)

    # Always keep the most recent `min_snapshots_to_keep` snapshots
    candidates_for_expiry = snapshots[to_keep:]
    expired_ids = []

    for snap in candidates_for_expiry:
        snap_time = datetime.fromtimestamp(snap.timestamp_ms / 1000, tz=timezone.utc)
        if snap_time < cutoff:
            expired_ids.append(snap.snapshot_id)

    if dry_run:
        log.info("dry_run_expiration", would_expire=len(expired_ids), total=total)
        return {
            "dry_run": True,
            "total_snapshots": total,
            "would_expire": len(expired_ids),
            "would_retain": total - len(expired_ids),
        }

    if expired_ids:
        tx = table.manage_snapshots()
        for sid in expired_ids:
            tx = tx.expire_snapshot_by_id(sid)
        tx.commit()

    log.info("snapshots_expired", expired=len(expired_ids), retained=total - len(expired_ids))
    return {
        "dry_run": False,
        "total_snapshots": total,
        "expired": len(expired_ids),
        "retained": total - len(expired_ids),
    }
