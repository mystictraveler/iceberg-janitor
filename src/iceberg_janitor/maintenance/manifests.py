"""Manifest rewriting for Iceberg tables."""

from __future__ import annotations

import structlog

from pyiceberg.catalog import Catalog

logger = structlog.get_logger()


def rewrite_manifests(
    catalog: Catalog,
    table_id: str,
    dry_run: bool = False,
) -> dict:
    """Rewrite manifest files to reduce metadata overhead.

    When streaming continuously writes small commits, each commit creates new
    manifest files. Over time this causes manifest bloat that slows down query
    planning. Rewriting merges many small manifests into fewer, larger ones.

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier.
        dry_run: If True, report manifest state without rewriting.

    Returns:
        Dict with rewrite results.
    """
    log = logger.bind(table_id=table_id)
    log.info("analyzing_manifests")

    table = catalog.load_table(table_id)

    # Count current manifests across all snapshots
    current_snapshot = table.current_snapshot()
    if current_snapshot is None:
        log.info("no_current_snapshot")
        return {"dry_run": dry_run, "manifest_count": 0, "rewritten": False}

    manifests = current_snapshot.manifests(table.io)
    manifest_count = len(manifests)
    total_manifest_bytes = sum(m.manifest_length for m in manifests)

    if dry_run:
        log.info(
            "dry_run_manifest_rewrite",
            manifest_count=manifest_count,
            total_mb=total_manifest_bytes / (1024 * 1024),
        )
        return {
            "dry_run": True,
            "manifest_count": manifest_count,
            "total_manifest_bytes": total_manifest_bytes,
            "rewritten": False,
        }

    # PyIceberg doesn't have a direct rewrite_manifests API yet.
    # This is a placeholder — in production, you'd either:
    # 1. Use Spark's rewriteManifests() action
    # 2. Use a custom implementation that reads all manifest entries
    #    and writes them into fewer, larger manifest files
    log.warning(
        "manifest_rewrite_not_yet_implemented",
        manifest_count=manifest_count,
        note="PyIceberg lacks native rewrite_manifests. Use Spark for this operation.",
    )

    return {
        "dry_run": False,
        "manifest_count": manifest_count,
        "total_manifest_bytes": total_manifest_bytes,
        "rewritten": False,
        "reason": "PyIceberg lacks native rewrite_manifests API. Consider using Spark.",
    }
