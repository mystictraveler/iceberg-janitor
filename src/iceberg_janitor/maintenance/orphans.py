"""Orphan file detection and cleanup for Iceberg tables."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

import structlog

from pyiceberg.catalog import Catalog
from pyiceberg.io import load_file_io

logger = structlog.get_logger()


def find_orphans(
    catalog: Catalog,
    table_id: str,
    retention_hours: int = 72,
) -> list[str]:
    """Find orphan files not referenced by any snapshot.

    Orphan files are data/metadata files that exist in the table's storage
    but are not referenced by any current snapshot. Common causes:
    - Failed streaming writes that created files but didn't commit
    - Expired snapshots whose files weren't cleaned up
    - Interrupted compaction jobs

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier.
        retention_hours: Only flag files older than this as orphans (safety margin).

    Returns:
        List of orphan file paths.
    """
    log = logger.bind(table_id=table_id)
    log.info("scanning_for_orphans")

    table = catalog.load_table(table_id)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)

    # Collect all files referenced by current snapshots
    referenced_files: set[str] = set()
    for snapshot in table.metadata.snapshots:
        manifests = snapshot.manifests(table.io)
        for manifest in manifests:
            referenced_files.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(table.io):
                referenced_files.add(entry.data_file.file_path)

    # List all files in the table's data directory
    table_location = table.metadata.location
    data_dir = str(PurePosixPath(table_location) / "data")
    file_io = load_file_io(properties=table.metadata.properties, location=table_location)

    all_files: set[str] = set()
    try:
        for file_info in file_io.list(data_dir):
            all_files.add(file_info)
    except Exception as e:
        log.warning("cannot_list_data_dir", error=str(e))
        return []

    orphans = [f for f in all_files if f not in referenced_files]

    log.info(
        "orphan_scan_complete",
        total_files=len(all_files),
        referenced=len(referenced_files),
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
            "files": orphan_files[:50],  # Cap for readability
        }

    table = catalog.load_table(table_id)
    file_io = load_file_io(
        properties=table.metadata.properties, location=table.metadata.location
    )

    removed = 0
    for path in orphan_files:
        try:
            file_io.delete(path)
            removed += 1
        except Exception as e:
            log.warning("orphan_delete_failed", path=path, error=str(e))

    log.info("orphans_removed", found=len(orphan_files), removed=removed)
    return {
        "dry_run": False,
        "orphans_found": len(orphan_files),
        "removed": removed,
    }
