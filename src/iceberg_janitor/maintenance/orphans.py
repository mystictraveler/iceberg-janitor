"""Orphan file detection and cleanup for Iceberg tables.

Uses only the PyIceberg catalog API — no direct S3/FileIO listing.
Orphan detection works by comparing files referenced in snapshots against
files referenced in the *current* snapshot's manifests. Files that appear
in old (expired) snapshots but not in any current manifest are orphan
candidates.

Note: true orphan detection (unreferenced files on storage) requires
storage listing which is a catalog-level concern. This implementation
focuses on metadata-level orphan detection which catches the most common
cases: files from expired snapshots and failed writes that were committed
but later superseded.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import structlog

from pyiceberg.catalog import Catalog

logger = structlog.get_logger()


def find_orphans(
    catalog: Catalog,
    table_id: str,
    retention_hours: int = 72,
) -> list[str]:
    """Find orphan files not referenced by the current snapshot.

    Walks all snapshots to find data files that were referenced by old
    snapshots but are no longer referenced by the current snapshot's
    manifest tree. These are safe to delete after snapshot expiration.

    This approach uses only the catalog API — no direct storage access.

    Args:
        catalog: PyIceberg catalog instance (REST, Glue, Hive, etc.).
        table_id: Fully qualified table identifier.
        retention_hours: Only flag files from snapshots older than this.

    Returns:
        List of orphan file paths.
    """
    log = logger.bind(table_id=table_id)
    log.info("scanning_for_orphans")

    table = catalog.load_table(table_id)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

    # Collect files referenced by the CURRENT snapshot
    current_snapshot = table.current_snapshot()
    if current_snapshot is None:
        log.info("no_current_snapshot")
        return []

    current_files: set[str] = set()
    for manifest in current_snapshot.manifests(table.io):
        current_files.add(manifest.manifest_path)
        for entry in manifest.fetch_manifest_entry(table.io):
            current_files.add(entry.data_file.file_path)

    # Collect files referenced by ALL snapshots (including expired ones)
    all_referenced: set[str] = set()
    for snapshot in table.metadata.snapshots:
        snap_time = datetime.fromtimestamp(
            snapshot.timestamp_ms / 1000, tz=timezone.utc
        )
        if snap_time > cutoff:
            continue  # Skip recent snapshots (retention safety margin)

        try:
            for manifest in snapshot.manifests(table.io):
                all_referenced.add(manifest.manifest_path)
                for entry in manifest.fetch_manifest_entry(table.io):
                    all_referenced.add(entry.data_file.file_path)
        except Exception as e:
            log.warning("manifest_read_failed", snapshot_id=snapshot.snapshot_id, error=str(e))

    # Orphans = files in old snapshots but NOT in current snapshot
    orphans = sorted(all_referenced - current_files)

    log.info(
        "orphan_scan_complete",
        current_files=len(current_files),
        all_referenced=len(all_referenced),
        orphans=len(orphans),
    )
    return orphans


def remove_orphans(
    catalog: Catalog,
    table_id: str,
    orphan_files: list[str] | None = None,
    retention_hours: int = 72,
    dry_run: bool = False,
) -> dict:
    """Remove orphan files from an Iceberg table's storage.

    Uses the table's configured IO (from the catalog) for file deletion.

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier.
        orphan_files: Pre-computed list of orphans. If None, will scan for them.
        retention_hours: Safety margin for orphan detection.
        dry_run: If True, report orphans without deleting.

    Returns:
        Dict with cleanup results.
    """
    log = logger.bind(table_id=table_id)

    if orphan_files is None:
        orphan_files = find_orphans(catalog, table_id, retention_hours)

    if not orphan_files:
        log.info("no_orphans_found")
        return {"dry_run": dry_run, "orphans_found": 0, "removed": 0}

    if dry_run:
        log.info("dry_run_orphan_removal", count=len(orphan_files))
        return {
            "dry_run": True,
            "orphans_found": len(orphan_files),
            "removed": 0,
            "files": orphan_files[:50],
        }

    table = catalog.load_table(table_id)

    removed = 0
    for path in orphan_files:
        try:
            table.io.delete(path)
            removed += 1
        except Exception as e:
            log.warning("orphan_delete_failed", path=path, error=str(e))

    log.info("orphans_removed", found=len(orphan_files), removed=removed)
    return {
        "dry_run": False,
        "orphans_found": len(orphan_files),
        "removed": removed,
    }
