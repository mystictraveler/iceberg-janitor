"""Compaction execution layer — pluggable backends for running compaction jobs.

This package provides an abstract executor interface with concrete implementations
for local (PyIceberg) and remote (Flink) compaction, plus a smart router that
selects the appropriate backend based on table size and cluster availability.
"""

from iceberg_janitor.execution.base import (
    CompactionExecutor,
    CompactionHandle,
    CompactionJob,
    JobStatus,
)
from iceberg_janitor.execution.flink import FlinkCompactionExecutor
from iceberg_janitor.execution.local import LocalCompactionExecutor
from iceberg_janitor.execution.orchestrator import CompactionOrchestrator
from iceberg_janitor.execution.router import ExecutionRouter

__all__ = [
    "CompactionExecutor",
    "CompactionHandle",
    "CompactionJob",
    "CompactionOrchestrator",
    "ExecutionRouter",
    "FlinkCompactionExecutor",
    "JobStatus",
    "LocalCompactionExecutor",
]
