"""Local in-process compaction executor using PyIceberg.

This executor runs compaction directly in the janitor process.  It is suitable
for small tables (typically under 1 GB) where the overhead of a distributed
engine is not justified.
"""

from __future__ import annotations

import threading
import traceback
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone

import structlog

from iceberg_janitor.execution.base import (
    CompactionExecutor,
    CompactionHandle,
    CompactionJob,
    JobStatus,
)

logger = structlog.get_logger()


class LocalCompactionExecutor(CompactionExecutor):
    """Runs compaction in-process via PyIceberg's table rewrite.

    Jobs execute on a background thread pool so the caller is not blocked.
    Status is tracked per ``job_id`` and cleaned up after the result is read.

    Parameters
    ----------
    max_workers:
        Maximum number of concurrent local compaction threads.
    """

    def __init__(self, max_workers: int = 2) -> None:
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="local-compact")
        self._futures: dict[str, Future] = {}
        self._handles: dict[str, CompactionHandle] = {}
        self._errors: dict[str, str] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # CompactionExecutor interface
    # ------------------------------------------------------------------

    def submit_compaction(self, job: CompactionJob) -> CompactionHandle:
        """Submit a local compaction job running on a background thread.

        The job calls into the existing ``compact_files`` logic from
        ``iceberg_janitor.maintenance.compaction``.
        """
        handle = CompactionHandle(
            job_id=job.job_id,
            executor_type="local",
            submitted_at=datetime.now(timezone.utc),
        )

        future = self._pool.submit(self._run_compaction, job)

        with self._lock:
            self._futures[job.job_id] = future
            self._handles[job.job_id] = handle

        logger.info(
            "local_compaction_submitted",
            job_id=job.job_id,
            table_id=job.table_id,
            strategy=job.strategy,
        )
        return handle

    def get_status(self, handle: CompactionHandle) -> JobStatus:
        """Return the current status of a local compaction job."""
        with self._lock:
            future = self._futures.get(handle.job_id)
            if future is None:
                return JobStatus.FAILED

            if future.running():
                return JobStatus.RUNNING
            if not future.done():
                return JobStatus.PENDING
            if future.cancelled():
                return JobStatus.CANCELLED

            exc = future.exception()
            if exc is not None:
                return JobStatus.FAILED

            return JobStatus.COMPLETED

    def cancel(self, handle: CompactionHandle) -> None:
        """Attempt to cancel a pending local compaction job.

        If the job is already running, the cancel request is best-effort
        (Python thread pool futures can only be cancelled before execution
        starts).
        """
        with self._lock:
            future = self._futures.get(handle.job_id)
            if future is not None:
                cancelled = future.cancel()
                logger.info(
                    "local_compaction_cancel_requested",
                    job_id=handle.job_id,
                    cancelled=cancelled,
                )

    # ------------------------------------------------------------------
    # Result access
    # ------------------------------------------------------------------

    def get_result(self, handle: CompactionHandle) -> dict | None:
        """Return the compaction result dict once the job completes, or ``None``."""
        with self._lock:
            future = self._futures.get(handle.job_id)
        if future is None or not future.done():
            return None
        exc = future.exception()
        if exc is not None:
            return {"error": str(exc)}
        return future.result()

    def get_error(self, handle: CompactionHandle) -> str | None:
        """Return the error message for a failed job, or ``None``."""
        with self._lock:
            future = self._futures.get(handle.job_id)
        if future is None or not future.done():
            return None
        exc = future.exception()
        return str(exc) if exc is not None else None

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self, handle: CompactionHandle) -> None:
        """Remove tracking state for a completed or failed job."""
        with self._lock:
            self._futures.pop(handle.job_id, None)
            self._handles.pop(handle.job_id, None)
            self._errors.pop(handle.job_id, None)

    def shutdown(self, wait: bool = True) -> None:
        """Shut down the background thread pool."""
        self._pool.shutdown(wait=wait)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _run_compaction(job: CompactionJob) -> dict:
        """Execute compaction synchronously using PyIceberg.

        This method runs on a background thread.  It lazily imports PyIceberg
        to avoid module-level coupling when the local executor is not in use.
        """
        from pyiceberg.catalog import load_catalog

        log = logger.bind(job_id=job.job_id, table_id=job.table_id)
        log.info("local_compaction_starting")

        try:
            # Build catalog configuration from the job
            catalog_config: dict[str, str] = {
                "uri": job.catalog_uri,
                "warehouse": job.warehouse,
                "type": job.catalog_type,
            }
            catalog_config.update(job.catalog_properties)

            catalog = load_catalog("janitor_local", **catalog_config)
            table = catalog.load_table(job.table_id)

            if job.dry_run:
                scan = table.scan()
                plan_files = list(scan.plan_files())
                small_files = [
                    t
                    for t in plan_files
                    if t.file.file_size_in_bytes < job.min_file_size_bytes
                ]
                total_small_bytes = sum(t.file.file_size_in_bytes for t in small_files)
                log.info(
                    "local_compaction_dry_run",
                    total_files=len(plan_files),
                    small_files=len(small_files),
                )
                return {
                    "dry_run": True,
                    "total_files": len(plan_files),
                    "small_files_found": len(small_files),
                    "total_small_bytes": total_small_bytes,
                    "compacted": False,
                }

            # Identify small files
            scan = table.scan()
            plan_files = list(scan.plan_files())
            small_files = [
                t
                for t in plan_files
                if t.file.file_size_in_bytes < job.min_file_size_bytes
            ]

            if len(small_files) < job.min_input_files:
                log.info(
                    "local_compaction_skipped",
                    reason="not enough small files",
                    small_files=len(small_files),
                    min_required=job.min_input_files,
                )
                return {
                    "dry_run": False,
                    "total_files": len(plan_files),
                    "small_files_found": len(small_files),
                    "compacted": False,
                    "reason": "below min_input_files threshold",
                }

            total_small_bytes = sum(t.file.file_size_in_bytes for t in small_files)

            # Execute the compaction via table overwrite
            log.info(
                "local_compaction_executing",
                small_files=len(small_files),
                total_small_mb=round(total_small_bytes / (1024 * 1024), 2),
            )
            df = table.scan().to_arrow()
            table.overwrite(df)

            log.info("local_compaction_complete", original_files=len(plan_files))
            return {
                "dry_run": False,
                "total_files": len(plan_files),
                "small_files_found": len(small_files),
                "total_small_bytes": total_small_bytes,
                "compacted": True,
            }

        except Exception:
            log.error("local_compaction_failed", error=traceback.format_exc())
            raise
