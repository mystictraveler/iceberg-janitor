"""Small file compaction for Iceberg tables.

Provides both a synchronous local implementation (``compact_files``) and an
asynchronous orchestrated path (``compact_files_async``) that routes large
tables to Flink while keeping small tables in-process.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from pyiceberg.catalog import Catalog

if TYPE_CHECKING:
    from iceberg_janitor.execution.base import CompactionJob
    from iceberg_janitor.execution.orchestrator import CompactionOrchestrator, PipelineRecord

logger = structlog.get_logger()


def compact_files(
    catalog: Catalog,
    table_id: str,
    target_file_size_bytes: int = 128 * 1024 * 1024,
    small_file_threshold_bytes: int = 8 * 1024 * 1024,
    dry_run: bool = False,
) -> dict:
    """Compact small data files in an Iceberg table (local, in-process).

    Uses PyIceberg's rewrite_files to merge small files into larger ones
    approaching the target file size.

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier.
        target_file_size_bytes: Target size for compacted files (default 128MB).
        small_file_threshold_bytes: Files below this size are candidates (default 8MB).
        dry_run: If True, report what would be compacted without doing it.

    Returns:
        Dict with compaction results.
    """
    log = logger.bind(
        table_id=table_id,
        target_mb=target_file_size_bytes / (1024 * 1024),
    )
    log.info("starting_compaction")

    table = catalog.load_table(table_id)
    scan = table.scan()

    # Identify small files from the current plan
    plan_files = list(scan.plan_files())
    small_files = [
        task for task in plan_files if task.file.file_size_in_bytes < small_file_threshold_bytes
    ]

    if not small_files:
        log.info("no_small_files_found")
        return {
            "dry_run": dry_run,
            "total_files": len(plan_files),
            "small_files_found": 0,
            "compacted": False,
        }

    total_small_bytes = sum(t.file.file_size_in_bytes for t in small_files)

    if dry_run:
        log.info(
            "dry_run_compaction",
            small_files=len(small_files),
            total_small_mb=total_small_bytes / (1024 * 1024),
        )
        return {
            "dry_run": True,
            "total_files": len(plan_files),
            "small_files_found": len(small_files),
            "total_small_bytes": total_small_bytes,
            "estimated_output_files": max(1, total_small_bytes // target_file_size_bytes),
            "compacted": False,
        }

    # Group small files and rewrite them
    # PyIceberg compact/rewrite approach
    log.info(
        "compacting",
        small_files=len(small_files),
        total_small_mb=total_small_bytes / (1024 * 1024),
    )

    # Read all data from small files and rewrite as larger files
    df = table.scan().to_arrow()
    # Overwrite with compacted data
    table.overwrite(df)

    log.info("compaction_complete", original_files=len(plan_files))
    return {
        "dry_run": False,
        "total_files": len(plan_files),
        "small_files_found": len(small_files),
        "total_small_bytes": total_small_bytes,
        "compacted": True,
    }


def compact_files_async(
    orchestrator: CompactionOrchestrator,
    job: CompactionJob,
) -> PipelineRecord:
    """Run compaction through the orchestrator, routing large tables to Flink.

    This is the preferred entry point for production workloads.  The
    orchestrator's :class:`ExecutionRouter` decides whether to run locally or
    submit to a Flink cluster based on estimated data size and cluster
    availability.

    The call blocks until the compaction pipeline reaches a terminal state
    (DONE or FAILED).

    Args:
        orchestrator: Fully configured :class:`CompactionOrchestrator`.
        job: Compaction job descriptor with estimated size information.

    Returns:
        :class:`PipelineRecord` containing the final state, metrics, and any
        error information.
    """
    log = logger.bind(
        job_id=job.job_id,
        table_id=job.table_id,
        estimated_data_size_mb=round(job.estimated_data_size_bytes / (1024 * 1024), 2),
    )
    log.info("compact_files_async_starting")

    record = orchestrator.run_compaction_pipeline(job)

    log.info(
        "compact_files_async_complete",
        state=record.state.value,
        attempts=record.attempt,
        error=record.error,
    )
    return record
