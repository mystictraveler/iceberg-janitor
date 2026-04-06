"""Flink-based compaction executor.

Submits compaction work as Flink batch jobs via the Flink REST API.  The janitor
remains the orchestrator; Flink handles the heavy data-shuffling compute.

The executor communicates with a Flink Session Cluster whose JobManager exposes
a REST endpoint (typically port 8081).  Compaction jobs are executed by the
``FlinkCompactionJob`` JAR which must be pre-uploaded to the cluster.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import structlog

from iceberg_janitor.execution.base import (
    CompactionExecutor,
    CompactionHandle,
    CompactionJob,
    JobStatus,
)

logger = structlog.get_logger()

# Mapping from Flink REST API job states to our internal enum.
_FLINK_STATE_MAP: dict[str, JobStatus] = {
    "CREATED": JobStatus.PENDING,
    "INITIALIZING": JobStatus.PENDING,
    "RUNNING": JobStatus.RUNNING,
    "RESTARTING": JobStatus.RUNNING,
    "RECONCILING": JobStatus.RUNNING,
    "FAILING": JobStatus.FAILED,
    "FAILED": JobStatus.FAILED,
    "CANCELLING": JobStatus.CANCELLED,
    "CANCELED": JobStatus.CANCELLED,
    "FINISHED": JobStatus.COMPLETED,
    "SUSPENDED": JobStatus.CANCELLED,
}


@dataclass
class FlinkResources:
    """Estimated Flink resource requirements for a compaction job."""

    parallelism: int
    taskmanager_memory_mb: int
    num_taskmanagers: int


class FlinkCompactionExecutor(CompactionExecutor):
    """Submits compaction jobs to a Flink Session Cluster via the REST API.

    Parameters
    ----------
    flink_rest_url:
        Base URL for the Flink JobManager REST API (e.g. ``http://flink-jobmanager:8081``).
    jar_id:
        ID of the pre-uploaded compaction JAR on the Flink cluster.
        Can be obtained via ``GET /jars`` after uploading.
    default_parallelism:
        Default job parallelism when not auto-calculated.
    max_parallelism:
        Upper bound on parallelism regardless of data size.
    timeout_seconds:
        HTTP request timeout for REST API calls.
    """

    def __init__(
        self,
        flink_rest_url: str = "http://flink-jobmanager:8081",
        jar_id: str = "",
        default_parallelism: int = 4,
        max_parallelism: int = 32,
        timeout_seconds: int = 30,
    ) -> None:
        self.flink_rest_url = flink_rest_url.rstrip("/")
        self.jar_id = jar_id
        self.default_parallelism = default_parallelism
        self.max_parallelism = max_parallelism
        self._client = httpx.Client(timeout=timeout_seconds)

    # ------------------------------------------------------------------
    # CompactionExecutor interface
    # ------------------------------------------------------------------

    def submit_compaction(self, job: CompactionJob) -> CompactionHandle:
        """Submit a compaction job to the Flink cluster.

        Serialises the :class:`CompactionJob` as JSON program args and POSTs
        to the Flink REST API ``/jars/{jar-id}/run`` endpoint.

        Returns a :class:`CompactionHandle` containing the Flink job ID for
        subsequent status polling.
        """
        resources = self._estimate_resources(job)

        program_args = self._build_program_args(job)

        url = f"{self.flink_rest_url}/jars/{self.jar_id}/run"
        payload = {
            "programArgs": program_args,
            "parallelism": resources.parallelism,
            "entryClass": "com.icebergjanitor.FlinkCompactionJob",
        }

        log = logger.bind(
            job_id=job.job_id,
            table_id=job.table_id,
            parallelism=resources.parallelism,
            taskmanager_memory_mb=resources.taskmanager_memory_mb,
        )
        log.info("flink_compaction_submitting", url=url)

        try:
            response = self._client.post(url, json=payload)
            response.raise_for_status()
            body = response.json()
            flink_job_id = body.get("jobid", "")
        except httpx.HTTPStatusError as exc:
            log.error(
                "flink_submit_failed",
                status_code=exc.response.status_code,
                response_body=exc.response.text,
            )
            raise RuntimeError(
                f"Flink job submission failed: {exc.response.status_code} "
                f"{exc.response.text}"
            ) from exc
        except httpx.RequestError as exc:
            log.error("flink_submit_connection_error", error=str(exc))
            raise RuntimeError(
                f"Failed to connect to Flink REST API at {self.flink_rest_url}: {exc}"
            ) from exc

        handle = CompactionHandle(
            job_id=job.job_id,
            executor_type="flink",
            submitted_at=datetime.now(timezone.utc),
            flink_job_id=flink_job_id,
        )
        log.info("flink_compaction_submitted", flink_job_id=flink_job_id)
        return handle

    def get_status(self, handle: CompactionHandle) -> JobStatus:
        """Query the Flink REST API for the current job status.

        Maps Flink-specific state strings to the generic :class:`JobStatus` enum.
        """
        if not handle.flink_job_id:
            return JobStatus.FAILED

        url = f"{self.flink_rest_url}/jobs/{handle.flink_job_id}"
        log = logger.bind(job_id=handle.job_id, flink_job_id=handle.flink_job_id)

        try:
            response = self._client.get(url)
            response.raise_for_status()
            body = response.json()
            flink_state = body.get("state", "UNKNOWN")
            status = _FLINK_STATE_MAP.get(flink_state, JobStatus.FAILED)
            log.debug("flink_status_polled", flink_state=flink_state, mapped_status=status.value)
            return status
        except httpx.HTTPStatusError as exc:
            log.error(
                "flink_status_check_failed",
                status_code=exc.response.status_code,
            )
            return JobStatus.FAILED
        except httpx.RequestError as exc:
            log.error("flink_status_connection_error", error=str(exc))
            return JobStatus.FAILED

    def cancel(self, handle: CompactionHandle) -> None:
        """Send a cancellation request to the Flink cluster via PATCH."""
        if not handle.flink_job_id:
            return

        url = f"{self.flink_rest_url}/jobs/{handle.flink_job_id}"
        log = logger.bind(job_id=handle.job_id, flink_job_id=handle.flink_job_id)

        try:
            response = self._client.patch(url, params={"mode": "cancel"})
            response.raise_for_status()
            log.info("flink_compaction_cancelled")
        except httpx.HTTPStatusError as exc:
            log.error(
                "flink_cancel_failed",
                status_code=exc.response.status_code,
                response_body=exc.response.text,
            )
        except httpx.RequestError as exc:
            log.error("flink_cancel_connection_error", error=str(exc))

    # ------------------------------------------------------------------
    # Cluster health
    # ------------------------------------------------------------------

    def is_cluster_available(self) -> bool:
        """Check whether the Flink cluster is reachable and healthy.

        Returns ``True`` if the JobManager REST API responds to ``GET /overview``.
        """
        try:
            response = self._client.get(f"{self.flink_rest_url}/overview")
            response.raise_for_status()
            return True
        except (httpx.HTTPStatusError, httpx.RequestError):
            return False

    def get_cluster_overview(self) -> dict | None:
        """Return the Flink cluster overview, or ``None`` on failure."""
        try:
            response = self._client.get(f"{self.flink_rest_url}/overview")
            response.raise_for_status()
            return response.json()
        except (httpx.HTTPStatusError, httpx.RequestError):
            return None

    # ------------------------------------------------------------------
    # Resource estimation
    # ------------------------------------------------------------------

    def _estimate_resources(self, job: CompactionJob) -> FlinkResources:
        """Calculate TaskManager memory and parallelism based on data size.

        Heuristics:
        - Parallelism scales with the number of files, capped by max_parallelism.
        - TaskManager memory is 2x the per-slot data share to allow for buffering.
        - At least one TaskManager per parallelism slot, but Flink can share.
        """
        # One slot per ~250 files, minimum default_parallelism
        parallelism = max(
            self.default_parallelism,
            job.estimated_file_count // 250,
        )
        parallelism = min(parallelism, self.max_parallelism)

        # Memory: distribute data across slots, add 2x buffer
        data_per_slot_mb = max(
            256,
            (job.estimated_data_size_bytes // parallelism) // (1024 * 1024),
        )
        taskmanager_memory_mb = data_per_slot_mb * 2

        # Cap memory at a reasonable maximum per TM
        taskmanager_memory_mb = min(taskmanager_memory_mb, 8192)

        # Number of TMs: one per slot for simplicity (Flink distributes)
        num_taskmanagers = max(1, parallelism // 2)

        return FlinkResources(
            parallelism=parallelism,
            taskmanager_memory_mb=taskmanager_memory_mb,
            num_taskmanagers=num_taskmanagers,
        )

    # ------------------------------------------------------------------
    # Argument serialisation
    # ------------------------------------------------------------------

    @staticmethod
    def _build_program_args(job: CompactionJob) -> str:
        """Serialise the CompactionJob into a JSON string for Flink program args.

        The ``FlinkCompactionJob`` Java main class parses this JSON to configure
        the Iceberg Actions API call.
        """
        args_dict = {
            "jobId": job.job_id,
            "tableId": job.table_id,
            "catalogUri": job.catalog_uri,
            "warehouse": job.warehouse,
            "catalogType": job.catalog_type,
            "catalogProperties": job.catalog_properties,
            "strategy": job.strategy,
            "targetFileSizeBytes": job.target_file_size_bytes,
            "minFileSizeBytes": job.min_file_size_bytes,
            "minInputFiles": job.min_input_files,
            "partitionFilter": job.partition_filter,
            "sortColumns": job.sort_columns,
            "partialProgress": job.partial_progress,
            "dryRun": job.dry_run,
        }
        return json.dumps(args_dict, separators=(",", ":"))

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()
