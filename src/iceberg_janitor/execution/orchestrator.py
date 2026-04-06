"""Compaction orchestrator — central state machine that drives the full lifecycle.

The orchestrator is the brain of the execution layer.  It:

* Tracks every in-flight compaction job through a state machine.
* Integrates with the :class:`MaintenanceScheduler` for priority ordering.
* Records pre/post compaction metrics into the :class:`FeedbackLoop`.
* Rate-limits concurrent jobs both per-table and globally.
* Retries failed jobs with exponential back-off.
* Provides a summary view of all active and recent jobs.

State machine
-------------
::

    IDLE -> ANALYZING -> SUBMITTING -> RUNNING -> POST_ANALYSIS -> DONE
                                              |-> FAILED -> RETRY -> SUBMITTING
"""

from __future__ import annotations

import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any

import structlog

from iceberg_janitor.execution.base import (
    CompactionHandle,
    CompactionJob,
    JobStatus,
)
from iceberg_janitor.execution.router import ExecutionRouter

if TYPE_CHECKING:
    from iceberg_janitor.strategy.feedback_loop import CompactionMetrics, FeedbackLoop
    from iceberg_janitor.strategy.scheduler import MaintenanceScheduler

logger = structlog.get_logger()


# ------------------------------------------------------------------
# Orchestrator-level state machine
# ------------------------------------------------------------------


class PipelineState(Enum):
    """Lifecycle states of a single compaction pipeline run."""

    IDLE = "idle"
    ANALYZING = "analyzing"
    SUBMITTING = "submitting"
    RUNNING = "running"
    POST_ANALYSIS = "post_analysis"
    DONE = "done"
    FAILED = "failed"
    RETRY = "retry"


@dataclass
class PipelineRecord:
    """Full tracking record for one compaction pipeline invocation."""

    job_id: str
    table_id: str
    state: PipelineState = PipelineState.IDLE
    handle: CompactionHandle | None = None
    attempt: int = 1
    max_attempts: int = 3

    # Timing
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    submitted_at: datetime | None = None
    completed_at: datetime | None = None

    # Results
    pre_metrics: Any = None
    post_metrics: Any = None
    result: dict | None = None
    error: str | None = None

    # Retry tracking
    next_retry_at: float | None = None  # epoch seconds


class CompactionOrchestrator:
    """Central brain coordinating compaction across multiple tables.

    Parameters
    ----------
    router:
        Execution router that selects the appropriate backend.
    scheduler:
        Maintenance scheduler for priority ordering and slot management.
        Optional — when ``None`` priority and slot management are disabled.
    feedback_loop:
        Feedback loop for effectiveness tracking.  Optional — when ``None``
        pre/post metrics recording is skipped.
    max_concurrent_jobs:
        Global cap on concurrent compaction jobs across all tables.
    max_concurrent_per_table:
        Maximum concurrent jobs for a single table.
    retry_max_attempts:
        Maximum number of submission attempts (including the original).
    retry_backoff_seconds:
        Base back-off between retries.  Doubles on each attempt.
    job_timeout_seconds:
        Maximum wall-clock time before a running job is considered timed out.
    poll_interval_seconds:
        Interval between status polls while monitoring a running job.
    """

    def __init__(
        self,
        router: ExecutionRouter,
        scheduler: MaintenanceScheduler | None = None,
        feedback_loop: FeedbackLoop | None = None,
        max_concurrent_jobs: int = 5,
        max_concurrent_per_table: int = 1,
        retry_max_attempts: int = 3,
        retry_backoff_seconds: int = 60,
        job_timeout_seconds: int = 3600,
        poll_interval_seconds: int = 10,
    ) -> None:
        self._router = router
        self._scheduler = scheduler
        self._feedback_loop = feedback_loop

        self.max_concurrent_jobs = max_concurrent_jobs
        self.max_concurrent_per_table = max_concurrent_per_table
        self.retry_max_attempts = retry_max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds

        self._lock = threading.Lock()
        self._pipelines: dict[str, PipelineRecord] = {}  # keyed by job_id
        self._active_per_table: dict[str, int] = {}
        self._active_total: int = 0

    # ------------------------------------------------------------------
    # Full pipeline: analyse -> submit -> monitor -> post-analyse
    # ------------------------------------------------------------------

    def run_compaction_pipeline(
        self,
        job: CompactionJob,
    ) -> PipelineRecord:
        """Execute the full compaction lifecycle for a single job.

        Steps:

        1. **Pre-compaction analysis** — record current table metrics.
        2. **Submit** — route to the appropriate executor and obtain a handle.
        3. **Monitor** — poll status until the job completes, fails, or times out.
        4. **Post-compaction analysis** — re-assess health and update the
           feedback loop.
        5. **Record results** — mark the pipeline as DONE or FAILED.

        This method blocks until the pipeline reaches a terminal state.

        Args:
            job: Fully populated compaction job descriptor.

        Returns:
            The final :class:`PipelineRecord` for the job.
        """
        record = PipelineRecord(
            job_id=job.job_id,
            table_id=job.table_id,
            max_attempts=self.retry_max_attempts,
        )

        log = logger.bind(job_id=job.job_id, table_id=job.table_id)

        # Register the pipeline
        with self._lock:
            self._pipelines[job.job_id] = record

        try:
            self._run_pipeline_loop(record, job, log)
        except Exception:
            record.state = PipelineState.FAILED
            record.error = traceback.format_exc()
            log.error("pipeline_unexpected_error", error=record.error)
        finally:
            # Release resources
            self._release_slot(record.table_id)
            record.completed_at = datetime.now(timezone.utc)

        return record

    def _run_pipeline_loop(self, record: PipelineRecord, job: CompactionJob, log: Any) -> None:
        """Inner loop that handles retries."""
        while record.attempt <= record.max_attempts:
            # --- ANALYZING ---
            record.state = PipelineState.ANALYZING
            log.info("pipeline_analyzing", attempt=record.attempt)

            pre_metrics = self._record_pre_metrics(job)
            record.pre_metrics = pre_metrics

            # --- Acquire slot ---
            if not self._acquire_slot(job.table_id):
                record.state = PipelineState.FAILED
                record.error = "Could not acquire compaction slot (concurrency limit reached)"
                log.warning("pipeline_slot_unavailable")
                return

            # --- SUBMITTING ---
            record.state = PipelineState.SUBMITTING
            log.info("pipeline_submitting")

            try:
                handle = self._router.submit_compaction(job)
                record.handle = handle
                record.submitted_at = datetime.now(timezone.utc)
            except Exception as exc:
                record.error = str(exc)
                log.error("pipeline_submit_failed", error=record.error)
                self._release_slot(job.table_id)
                if self._should_retry(record):
                    record.state = PipelineState.RETRY
                    record.attempt += 1
                    backoff = self.retry_backoff_seconds * (2 ** (record.attempt - 2))
                    record.next_retry_at = time.time() + backoff
                    log.info("pipeline_retry_scheduled", backoff_seconds=backoff, attempt=record.attempt)
                    time.sleep(backoff)
                    continue
                record.state = PipelineState.FAILED
                return

            # --- RUNNING ---
            record.state = PipelineState.RUNNING
            log.info("pipeline_running", flink_job_id=getattr(handle, "flink_job_id", None))

            final_status = self._poll_until_terminal(handle, log)

            if final_status == JobStatus.COMPLETED:
                # --- POST_ANALYSIS ---
                record.state = PipelineState.POST_ANALYSIS
                log.info("pipeline_post_analysis")

                post_metrics = self._record_post_metrics(job, pre_metrics)
                record.post_metrics = post_metrics

                record.state = PipelineState.DONE
                record.result = {
                    "status": "completed",
                    "executor_type": handle.executor_type,
                    "attempts": record.attempt,
                }

                # Notify scheduler
                if self._scheduler is not None:
                    self._scheduler.record_compaction_done(job.table_id)

                log.info("pipeline_done", attempts=record.attempt)
                return

            if final_status == JobStatus.CANCELLED:
                record.state = PipelineState.FAILED
                record.error = "Job was cancelled"
                log.info("pipeline_cancelled")
                return

            # Job failed — try retry
            record.error = f"Job ended with status: {final_status.value}"
            log.warning("pipeline_job_failed", status=final_status.value)
            self._release_slot(job.table_id)

            if self._should_retry(record):
                record.state = PipelineState.RETRY
                record.attempt += 1
                backoff = self.retry_backoff_seconds * (2 ** (record.attempt - 2))
                record.next_retry_at = time.time() + backoff
                log.info("pipeline_retry_scheduled", backoff_seconds=backoff, attempt=record.attempt)
                time.sleep(backoff)
                continue

            record.state = PipelineState.FAILED
            log.error("pipeline_exhausted_retries", attempts=record.attempt)
            return

        # All retries exhausted
        record.state = PipelineState.FAILED
        record.error = f"Exhausted all {record.max_attempts} attempts"

    # ------------------------------------------------------------------
    # Slot management
    # ------------------------------------------------------------------

    def _acquire_slot(self, table_id: str) -> bool:
        """Try to acquire a global and per-table compaction slot."""
        with self._lock:
            if self._active_total >= self.max_concurrent_jobs:
                return False
            per_table = self._active_per_table.get(table_id, 0)
            if per_table >= self.max_concurrent_per_table:
                return False
            self._active_total += 1
            self._active_per_table[table_id] = per_table + 1
        return True

    def _release_slot(self, table_id: str) -> None:
        """Release a previously acquired compaction slot."""
        with self._lock:
            self._active_total = max(0, self._active_total - 1)
            current = self._active_per_table.get(table_id, 0)
            if current <= 1:
                self._active_per_table.pop(table_id, None)
            else:
                self._active_per_table[table_id] = current - 1

    # ------------------------------------------------------------------
    # Status polling
    # ------------------------------------------------------------------

    def _poll_until_terminal(self, handle: CompactionHandle, log: Any) -> JobStatus:
        """Block until the job reaches a terminal state or times out."""
        deadline = time.time() + self.job_timeout_seconds

        while time.time() < deadline:
            status = self._router.get_status(handle)

            if status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                return status

            time.sleep(self.poll_interval_seconds)

        # Timed out — attempt to cancel
        log.warning("pipeline_job_timeout", timeout_seconds=self.job_timeout_seconds)
        try:
            self._router.cancel(handle)
        except Exception:
            pass
        return JobStatus.FAILED

    # ------------------------------------------------------------------
    # Retry logic
    # ------------------------------------------------------------------

    def _should_retry(self, record: PipelineRecord) -> bool:
        """Return True if the record is eligible for another attempt."""
        return record.attempt < record.max_attempts

    # ------------------------------------------------------------------
    # Feedback loop integration
    # ------------------------------------------------------------------

    def _record_pre_metrics(self, job: CompactionJob) -> Any:
        """Capture pre-compaction metrics if the feedback loop is attached."""
        if self._feedback_loop is None:
            return None

        from iceberg_janitor.strategy.feedback_loop import CompactionMetrics

        metrics = CompactionMetrics(
            file_count=job.estimated_file_count,
            total_bytes=job.estimated_data_size_bytes,
            avg_file_size_bytes=(
                job.estimated_data_size_bytes / max(1, job.estimated_file_count)
            ),
        )
        self._feedback_loop.record_pre_compaction(job.table_id, metrics)
        return metrics

    def _record_post_metrics(self, job: CompactionJob, pre_metrics: Any) -> Any:
        """Capture post-compaction metrics and compute effectiveness."""
        if self._feedback_loop is None or pre_metrics is None:
            return None

        from iceberg_janitor.strategy.feedback_loop import CompactionMetrics

        # Estimate post-compaction state: assumes files are merged toward target size
        estimated_post_files = max(
            1,
            job.estimated_data_size_bytes // job.target_file_size_bytes,
        )
        post_metrics = CompactionMetrics(
            file_count=estimated_post_files,
            total_bytes=job.estimated_data_size_bytes,
            avg_file_size_bytes=(
                job.estimated_data_size_bytes / max(1, estimated_post_files)
            ),
        )
        self._feedback_loop.record_post_compaction(job.table_id, post_metrics)
        return post_metrics

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_pipeline(self, job_id: str) -> PipelineRecord | None:
        """Look up a pipeline record by job ID."""
        return self._pipelines.get(job_id)

    def get_active_pipelines(self) -> list[PipelineRecord]:
        """Return all pipelines that are not in a terminal state."""
        terminal = {PipelineState.DONE, PipelineState.FAILED}
        return [p for p in self._pipelines.values() if p.state not in terminal]

    def get_recent_pipelines(self, limit: int = 50) -> list[PipelineRecord]:
        """Return the most recent pipeline records, newest first."""
        records = sorted(
            self._pipelines.values(),
            key=lambda p: p.created_at,
            reverse=True,
        )
        return records[:limit]

    def summary(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary of the orchestrator state."""
        by_state: dict[str, int] = {}
        for p in self._pipelines.values():
            by_state[p.state.value] = by_state.get(p.state.value, 0) + 1

        return {
            "total_pipelines": len(self._pipelines),
            "active_total": self._active_total,
            "active_per_table": dict(self._active_per_table),
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "by_state": by_state,
        }

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_completed(self, older_than_seconds: int = 3600) -> int:
        """Remove completed/failed pipeline records older than the threshold.

        Returns the number of records removed.
        """
        terminal = {PipelineState.DONE, PipelineState.FAILED}
        cutoff = datetime.now(timezone.utc).timestamp() - older_than_seconds
        to_remove: list[str] = []

        with self._lock:
            for job_id, record in self._pipelines.items():
                if record.state in terminal and record.created_at.timestamp() < cutoff:
                    to_remove.append(job_id)
            for job_id in to_remove:
                del self._pipelines[job_id]

        return len(to_remove)
