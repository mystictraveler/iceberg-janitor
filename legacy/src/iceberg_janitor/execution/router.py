"""Smart execution routing — selects the appropriate compaction backend.

The :class:`ExecutionRouter` inspects table size, user configuration, and
Flink cluster availability to decide whether compaction should run locally
(in-process via PyIceberg) or be offloaded to a Flink Session Cluster.
"""

from __future__ import annotations

import structlog

from iceberg_janitor.execution.base import (
    CompactionExecutor,
    CompactionHandle,
    CompactionJob,
    JobStatus,
)
from iceberg_janitor.execution.flink import FlinkCompactionExecutor
from iceberg_janitor.execution.local import LocalCompactionExecutor

logger = structlog.get_logger()


class ExecutionRouter(CompactionExecutor):
    """Routes compaction jobs to the best available executor.

    Routing logic (evaluated in order):

    1. **Forced executor** — If the caller explicitly sets ``force_executor``
       in the job's ``catalog_properties`` (key ``janitor.force_executor``),
       that backend is used unconditionally.
    2. **Size threshold** — Tables whose ``estimated_data_size_bytes`` is
       below ``local_max_bytes`` use the local executor.  Tables above
       ``flink_min_bytes`` use Flink.  In the overlap zone both are acceptable
       and Flink is preferred when available.
    3. **Flink availability** — If Flink is selected but the cluster is
       unreachable, the router falls back to the local executor and logs a
       warning.

    Parameters
    ----------
    local_executor:
        Local (PyIceberg) executor instance.
    flink_executor:
        Flink REST API executor instance.
    local_max_bytes:
        Maximum data size in bytes for which the local executor is preferred.
        Defaults to 1 GiB.
    flink_min_bytes:
        Minimum data size in bytes for which Flink is preferred.
        Defaults to 512 MiB.  The gap between ``flink_min_bytes`` and
        ``local_max_bytes`` creates an overlap zone where Flink is preferred
        but local is an acceptable fallback.
    flink_enabled:
        Master switch to enable/disable Flink routing.  When ``False`` all
        jobs are routed locally regardless of size.
    """

    def __init__(
        self,
        local_executor: LocalCompactionExecutor,
        flink_executor: FlinkCompactionExecutor,
        local_max_bytes: int = 1_073_741_824,  # 1 GiB
        flink_min_bytes: int = 536_870_912,  # 512 MiB
        flink_enabled: bool = True,
    ) -> None:
        self._local = local_executor
        self._flink = flink_executor
        self.local_max_bytes = local_max_bytes
        self.flink_min_bytes = flink_min_bytes
        self.flink_enabled = flink_enabled

        # Cache the last known Flink availability to avoid hammering the API
        self._flink_available: bool | None = None

    # ------------------------------------------------------------------
    # CompactionExecutor interface
    # ------------------------------------------------------------------

    def submit_compaction(self, job: CompactionJob) -> CompactionHandle:
        """Route the job to the best executor and submit it."""
        executor = self._select_executor(job)
        return executor.submit_compaction(job)

    def get_status(self, handle: CompactionHandle) -> JobStatus:
        """Delegate status polling to the executor that owns the handle."""
        executor = self._executor_for_handle(handle)
        return executor.get_status(handle)

    def cancel(self, handle: CompactionHandle) -> None:
        """Delegate cancellation to the executor that owns the handle."""
        executor = self._executor_for_handle(handle)
        executor.cancel(handle)

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def _select_executor(self, job: CompactionJob) -> CompactionExecutor:
        """Decide which executor should handle *job*."""
        log = logger.bind(
            job_id=job.job_id,
            table_id=job.table_id,
            estimated_data_size_bytes=job.estimated_data_size_bytes,
        )

        # 1. Check for forced executor override
        forced = job.catalog_properties.get("janitor.force_executor")
        if forced == "flink":
            if self.flink_enabled and self._is_flink_available():
                log.info("routing_forced_flink")
                return self._flink
            log.warning("routing_forced_flink_unavailable_fallback_local")
            return self._local
        if forced == "local":
            log.info("routing_forced_local")
            return self._local

        # 2. Size-based routing
        data_size = job.estimated_data_size_bytes

        if not self.flink_enabled:
            log.info("routing_local_flink_disabled")
            return self._local

        if data_size <= self.flink_min_bytes:
            log.info("routing_local_small_table")
            return self._local

        if data_size > self.local_max_bytes:
            # Large table — must use Flink (or fall back)
            if self._is_flink_available():
                log.info("routing_flink_large_table")
                return self._flink
            log.warning("routing_flink_unavailable_fallback_local_large_table")
            return self._local

        # Overlap zone: prefer Flink if available
        if self._is_flink_available():
            log.info("routing_flink_overlap_zone")
            return self._flink

        log.info("routing_local_overlap_zone_flink_unavailable")
        return self._local

    def _executor_for_handle(self, handle: CompactionHandle) -> CompactionExecutor:
        """Return the executor that created *handle*."""
        if handle.executor_type == "flink":
            return self._flink
        return self._local

    # ------------------------------------------------------------------
    # Flink availability
    # ------------------------------------------------------------------

    def _is_flink_available(self) -> bool:
        """Check Flink cluster availability, using a lightweight cache."""
        available = self._flink.is_cluster_available()
        self._flink_available = available
        return available

    def refresh_flink_availability(self) -> bool:
        """Force-refresh the Flink availability cache.

        Returns the current availability state.
        """
        self._flink_available = self._flink.is_cluster_available()
        return self._flink_available

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    @property
    def flink_available(self) -> bool | None:
        """Last known Flink availability (``None`` if never checked)."""
        return self._flink_available
