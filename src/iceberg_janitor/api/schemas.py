"""Pydantic request/response models for the Iceberg Janitor API."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared / generic
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    error: str
    detail: str
    table_id: str | None = None


class PaginatedResponse(BaseModel):
    items: list[Any]
    total: int
    limit: int
    offset: int


# ---------------------------------------------------------------------------
# Table info
# ---------------------------------------------------------------------------


class TableInfo(BaseModel):
    table_id: str
    namespace: str
    name: str
    location: str | None = None
    format_version: int | None = None
    last_updated: datetime | None = None


# ---------------------------------------------------------------------------
# Health report
# ---------------------------------------------------------------------------


class FileStatsResponse(BaseModel):
    file_count: int = 0
    total_bytes: int = 0
    avg_file_size_bytes: float = 0.0
    min_file_size_bytes: int = 0
    max_file_size_bytes: int = 0
    median_file_size_bytes: float = 0.0
    small_file_count: int = 0
    small_file_ratio: float = 0.0
    total_mb: float = 0.0


class SnapshotStatsResponse(BaseModel):
    snapshot_count: int = 0
    oldest_snapshot_ts: datetime | None = None
    newest_snapshot_ts: datetime | None = None


class SizeBucketResponse(BaseModel):
    bucket: str
    file_count: int
    total_bytes: int


class PartitionHealthResponse(BaseModel):
    partition: str
    file_count: int
    total_bytes: int
    avg_file_size_bytes: float


class HealthReportResponse(BaseModel):
    table_id: str
    assessed_at: datetime
    is_healthy: bool
    needs_compaction: bool
    needs_snapshot_expiry: bool
    file_stats: FileStatsResponse
    snapshot_stats: SnapshotStatsResponse
    size_distribution: list[SizeBucketResponse] = Field(default_factory=list)
    hot_partitions: list[PartitionHealthResponse] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class AnalyzeRequest(BaseModel):
    small_file_threshold_bytes: int = Field(
        default=8 * 1024 * 1024,
        description="Files below this size are considered small (default 8MB)",
    )
    partition_min_files: int = Field(
        default=10,
        description="Only report partitions with more files than this",
    )
    partition_limit: int = Field(
        default=20,
        description="Maximum number of hot partitions to return",
    )


# ---------------------------------------------------------------------------
# Task tracking
# ---------------------------------------------------------------------------


class TaskResponse(BaseModel):
    task_id: UUID
    status: str = "accepted"
    message: str | None = None


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------


class CompactionRequest(BaseModel):
    target_file_size_bytes: int = Field(
        default=128 * 1024 * 1024,
        description="Target file size after compaction (default 128MB)",
    )
    small_file_threshold_bytes: int = Field(
        default=8 * 1024 * 1024,
        description="Files below this threshold are compaction candidates (default 8MB)",
    )
    dry_run: bool = False


class CompactionResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    total_files: int | None = None
    small_files_found: int | None = None
    total_small_bytes: int | None = None
    estimated_output_files: int | None = None
    compacted: bool | None = None
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Snapshot expiration
# ---------------------------------------------------------------------------


class SnapshotExpirationRequest(BaseModel):
    retention_hours: int = Field(
        default=168,
        description="Expire snapshots older than this many hours (default 7 days)",
    )
    min_snapshots_to_keep: int = Field(
        default=5,
        description="Always retain at least this many recent snapshots",
    )
    dry_run: bool = False


class SnapshotExpirationResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    total_snapshots: int | None = None
    expired: int | None = None
    retained: int | None = None
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Orphan cleanup
# ---------------------------------------------------------------------------


class OrphanCleanupRequest(BaseModel):
    retention_hours: int = Field(
        default=72,
        description="Only consider files older than this as orphans (default 3 days)",
    )
    dry_run: bool = False


class OrphanCleanupResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    orphans_found: int | None = None
    removed: int | None = None
    files: list[str] | None = None
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Manifest rewrite
# ---------------------------------------------------------------------------


class RewriteManifestsRequest(BaseModel):
    dry_run: bool = False


class RewriteManifestsResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    manifests_rewritten: int | None = None
    manifests_kept: int | None = None
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Sort optimization
# ---------------------------------------------------------------------------


class OptimizeSortRequest(BaseModel):
    sort_order_id: int | None = Field(
        default=None,
        description="Iceberg sort order ID. If null, uses table default.",
    )
    dry_run: bool = False


class OptimizeSortResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    files_rewritten: int | None = None
    sort_order_id: int | None = None
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Run-all (full maintenance cycle)
# ---------------------------------------------------------------------------


class RunAllRequest(BaseModel):
    dry_run: bool = False


class MaintenanceStepResult(BaseModel):
    action: str
    status: str = "pending"
    result: dict[str, Any] | None = None


class RunAllResponse(BaseModel):
    task_id: UUID
    dry_run: bool
    steps: list[MaintenanceStepResult] = Field(default_factory=list)
    status: str = "accepted"


# ---------------------------------------------------------------------------
# Maintenance history
# ---------------------------------------------------------------------------


class MaintenanceHistoryEntry(BaseModel):
    id: UUID
    table_id: str
    action_type: str
    status: str
    started_at: datetime
    completed_at: datetime | None = None
    dry_run: bool = False
    result: dict[str, Any] | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


class PolicyResponse(BaseModel):
    table_id: str
    policy: TablePolicySchema
    is_override: bool


class TablePolicySchema(BaseModel):
    max_small_file_ratio: float = 0.3
    small_file_threshold_bytes: int = 8 * 1024 * 1024
    target_file_size_bytes: int = 128 * 1024 * 1024
    max_file_count: int = 10000
    max_snapshots: int = 100
    snapshot_retention_hours: int = 168
    min_snapshots_to_keep: int = 5
    orphan_retention_hours: int = 72
    max_manifest_count: int = 500
    enabled: bool = True


class PolicyUpdateRequest(TablePolicySchema):
    """Same fields as TablePolicySchema, all optional for partial updates."""

    max_small_file_ratio: float | None = None  # type: ignore[assignment]
    small_file_threshold_bytes: int | None = None  # type: ignore[assignment]
    target_file_size_bytes: int | None = None  # type: ignore[assignment]
    max_file_count: int | None = None  # type: ignore[assignment]
    max_snapshots: int | None = None  # type: ignore[assignment]
    snapshot_retention_hours: int | None = None  # type: ignore[assignment]
    min_snapshots_to_keep: int | None = None  # type: ignore[assignment]
    orphan_retention_hours: int | None = None  # type: ignore[assignment]
    max_manifest_count: int | None = None  # type: ignore[assignment]
    enabled: bool | None = None  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Metadata inspection
# ---------------------------------------------------------------------------


class SnapshotInfo(BaseModel):
    snapshot_id: int
    parent_snapshot_id: int | None = None
    timestamp_ms: int
    timestamp: datetime | None = None
    operation: str | None = None
    summary: dict[str, str] | None = None
    manifest_list_location: str | None = None


class FileInfo(BaseModel):
    file_path: str
    file_size_bytes: int
    file_format: str
    record_count: int | None = None
    partition: dict[str, str] | None = None
    sort_order_id: int | None = None


class PartitionInfo(BaseModel):
    partition: str
    file_count: int
    total_bytes: int
    avg_file_size_bytes: float = 0.0
    needs_compaction: bool = False


# ---------------------------------------------------------------------------
# Service health
# ---------------------------------------------------------------------------


class ServiceHealthResponse(BaseModel):
    status: str
    version: str | None = None
    catalog_connected: bool = False
    uptime_seconds: float = 0.0
