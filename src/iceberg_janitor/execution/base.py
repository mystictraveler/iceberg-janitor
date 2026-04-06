"""Abstract executor interface and shared data structures for compaction execution."""

from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


class JobStatus(Enum):
    """Lifecycle status of a compaction job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CompactionJob:
    """Execution context passed from the janitor orchestrator to a compaction executor.

    Contains everything an executor needs to connect to the catalog, locate the
    table, and run the chosen compaction strategy.
    """

    table_id: str
    catalog_uri: str
    warehouse: str
    catalog_type: str
    catalog_properties: dict[str, str]  # S3 endpoint, credentials, etc.

    # Compaction strategy
    strategy: str  # "binpack", "sort", "zorder"
    target_file_size_bytes: int
    min_file_size_bytes: int
    min_input_files: int
    partition_filter: str | None
    sort_columns: list[str]

    # Behaviour flags
    partial_progress: bool
    dry_run: bool

    # Estimated resource requirements (used by the router and Flink executor)
    estimated_data_size_bytes: int
    estimated_file_count: int

    # Auto-generated identifier
    job_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass
class CompactionHandle:
    """Opaque handle returned when a compaction job is submitted.

    The handle carries enough information to query status and cancel the job
    regardless of the underlying executor backend.
    """

    job_id: str
    executor_type: str  # "flink", "local", "spark"
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    flink_job_id: str | None = None  # Flink-specific job ID


class CompactionExecutor(ABC):
    """Abstract interface for compaction execution backends.

    Implementors must support the full job lifecycle: submit, poll status, and
    cancel.  Each backend is responsible for translating the generic
    :class:`CompactionJob` into whatever its runtime requires.
    """

    @abstractmethod
    def submit_compaction(self, job: CompactionJob) -> CompactionHandle:
        """Submit a compaction job and return a handle for tracking.

        Args:
            job: Fully populated compaction job descriptor.

        Returns:
            A handle that can be used with :meth:`get_status` and :meth:`cancel`.

        Raises:
            RuntimeError: If the submission fails.
        """

    @abstractmethod
    def get_status(self, handle: CompactionHandle) -> JobStatus:
        """Poll the current status of a previously submitted job.

        Args:
            handle: Handle returned by :meth:`submit_compaction`.

        Returns:
            Current :class:`JobStatus`.
        """

    @abstractmethod
    def cancel(self, handle: CompactionHandle) -> None:
        """Request cancellation of a running job.

        This is best-effort; the job may still complete before the cancellation
        takes effect.

        Args:
            handle: Handle returned by :meth:`submit_compaction`.
        """
