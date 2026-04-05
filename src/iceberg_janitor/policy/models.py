"""Pydantic models for maintenance policies and actions."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class ActionType(str, Enum):
    COMPACT_FILES = "compact_files"
    EXPIRE_SNAPSHOTS = "expire_snapshots"
    REMOVE_ORPHANS = "remove_orphans"
    REWRITE_MANIFESTS = "rewrite_manifests"


class TablePolicy(BaseModel):
    """Maintenance policy thresholds for a single table."""

    # Compaction thresholds
    max_small_file_ratio: float = Field(
        default=0.3, description="Trigger compaction when small file ratio exceeds this"
    )
    small_file_threshold_bytes: int = Field(
        default=8 * 1024 * 1024, description="Files smaller than this are 'small' (default 8MB)"
    )
    target_file_size_bytes: int = Field(
        default=128 * 1024 * 1024, description="Target file size after compaction (default 128MB)"
    )
    max_file_count: int = Field(
        default=10000, description="Trigger compaction when file count exceeds this"
    )

    # Snapshot expiration thresholds
    max_snapshots: int = Field(
        default=100, description="Expire snapshots when count exceeds this"
    )
    snapshot_retention_hours: int = Field(
        default=168, description="Keep snapshots newer than this (default 7 days)"
    )
    min_snapshots_to_keep: int = Field(
        default=5, description="Always keep at least this many snapshots"
    )

    # Orphan cleanup
    orphan_retention_hours: int = Field(
        default=72, description="Only remove orphans older than this (default 3 days)"
    )

    # Manifest rewriting
    max_manifest_count: int = Field(
        default=500, description="Trigger manifest rewrite when manifest count exceeds this"
    )

    # Scheduling
    enabled: bool = Field(default=True, description="Whether maintenance is enabled for this table")


class MaintenanceAction(BaseModel):
    """A maintenance action to be executed against a table."""

    action_type: ActionType
    table_id: str
    priority: int = Field(default=0, description="Higher priority runs first")
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="", description="Human-readable reason for this action")


class PolicyConfig(BaseModel):
    """Top-level policy configuration, supporting per-table overrides."""

    default_policy: TablePolicy = Field(default_factory=TablePolicy)
    table_overrides: dict[str, TablePolicy] = Field(
        default_factory=dict,
        description="Per-table policy overrides keyed by table identifier",
    )

    def get_policy(self, table_id: str) -> TablePolicy:
        """Get the effective policy for a table (override or default)."""
        return self.table_overrides.get(table_id, self.default_policy)
