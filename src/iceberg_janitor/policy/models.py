"""Pydantic models for maintenance policies and actions."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

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

    # --- Trigger configuration ---
    trigger_mode: Literal["auto", "manual", "scheduled"] = Field(
        default="auto",
        description=(
            "Trigger mode: 'auto' evaluates triggers continuously, "
            "'manual' only runs on explicit request, "
            "'scheduled' runs during maintenance windows only."
        ),
    )

    commit_count_trigger_threshold: int = Field(
        default=50,
        description="Fire maintenance after this many commits since last compaction (0 to disable)",
    )
    file_count_trigger_threshold: int = Field(
        default=500,
        description="Fire maintenance when small file count exceeds this (0 to disable)",
    )
    time_trigger_interval_minutes: int = Field(
        default=30,
        description="Fire maintenance at least every N minutes (0 to disable)",
    )
    size_trigger_threshold_bytes: int = Field(
        default=1_073_741_824,
        description="Fire maintenance when uncompacted data exceeds this in bytes (0 to disable, default 1 GiB)",
    )

    # --- Maintenance windows ---
    maintenance_window_start: str | None = Field(
        default=None,
        description="Start of maintenance window in UTC HH:MM format (e.g. '02:00')",
    )
    maintenance_window_end: str | None = Field(
        default=None,
        description="End of maintenance window in UTC HH:MM format (e.g. '06:00')",
    )

    # --- Rate limiting ---
    max_concurrent_compactions: int = Field(
        default=2,
        description="Maximum number of simultaneous compaction operations",
    )


class MaintenanceAction(BaseModel):
    """A maintenance action to be executed against a table."""

    action_type: ActionType
    table_id: str
    priority: int = Field(default=0, description="Higher priority runs first")
    params: dict[str, Any] = Field(default_factory=dict)
    reason: str = Field(default="", description="Human-readable reason for this action")


class AdaptivePolicyConfig(BaseModel):
    """Configuration for the adaptive access-frequency-based policy engine."""

    adaptive_policy_enabled: bool = Field(
        default=False,
        description="Enable adaptive policy engine that adjusts thresholds based on table access patterns",
    )
    hot_threshold_queries_per_hour: int = Field(
        default=100,
        description="Queries per hour above which a table is classified as 'hot'",
    )
    warm_threshold_queries_per_hour: int = Field(
        default=10,
        description="Queries per hour above which a table is 'warm' (below = 'cold')",
    )
    hot_multiplier: float = Field(
        default=0.5,
        description=(
            "Threshold multiplier for hot tables (< 1.0 = more aggressive compaction). "
            "E.g. 0.5 means hot tables trigger compaction at half the normal thresholds."
        ),
    )
    warm_multiplier: float = Field(
        default=1.0,
        description="Threshold multiplier for warm tables (1.0 = use default thresholds)",
    )
    cold_multiplier: float = Field(
        default=3.0,
        description=(
            "Threshold multiplier for cold tables (> 1.0 = less frequent compaction). "
            "E.g. 3.0 means cold tables only compact at 3x the normal thresholds."
        ),
    )
    feedback_loop_enabled: bool = Field(
        default=False,
        description=(
            "Enable the self-correcting feedback loop that measures compaction "
            "effectiveness and adjusts future aggressiveness automatically"
        ),
    )
    effectiveness_lookback_hours: int = Field(
        default=24,
        description="How many hours of effectiveness history to consider for trend analysis",
    )
    access_state_persistence_path: str | None = Field(
        default=None,
        description="File path for persisting access tracker state across restarts (JSON format)",
    )
    decay_half_life_hours: float = Field(
        default=6.0,
        description=(
            "Half-life in hours for the exponential decay of access counts. "
            "After this period, old events contribute half as much to the heat score."
        ),
    )


class ExecutionConfig(BaseModel):
    """Configuration for the compaction execution layer.

    Controls how the janitor routes compaction work between local (in-process
    PyIceberg) and remote (Flink) executors.
    """

    executor: Literal["auto", "local", "flink", "spark"] = Field(
        default="auto",
        description=(
            "Executor selection mode.  'auto' routes based on data size; "
            "'local' and 'flink' force a specific backend; 'spark' is reserved "
            "for future use."
        ),
    )
    flink_rest_url: str = Field(
        default="http://flink-jobmanager:8081",
        description="Base URL for the Flink JobManager REST API",
    )
    flink_jar_id: str = Field(
        default="",
        description=(
            "ID of the pre-uploaded compaction JAR on the Flink cluster.  "
            "Obtain via GET /jars after uploading the fat JAR."
        ),
    )
    local_max_data_bytes: int = Field(
        default=1_073_741_824,  # 1 GiB
        description=(
            "Maximum data size in bytes for which the local executor is preferred.  "
            "Tables larger than this are routed to Flink."
        ),
    )
    flink_default_parallelism: int = Field(
        default=4,
        description="Default Flink job parallelism when not auto-calculated from data size",
    )
    flink_max_parallelism: int = Field(
        default=32,
        description="Upper bound on Flink job parallelism regardless of data size",
    )
    max_concurrent_jobs: int = Field(
        default=5,
        description="Global cap on concurrent compaction jobs across all tables",
    )
    job_timeout_seconds: int = Field(
        default=3600,
        description="Maximum wall-clock seconds before a running job is considered timed out",
    )
    retry_max_attempts: int = Field(
        default=3,
        description="Maximum number of submission attempts (including the original)",
    )
    retry_backoff_seconds: int = Field(
        default=60,
        description="Base back-off in seconds between retries (doubles on each attempt)",
    )


class PolicyConfig(BaseModel):
    """Top-level policy configuration, supporting per-table overrides."""

    default_policy: TablePolicy = Field(default_factory=TablePolicy)
    table_overrides: dict[str, TablePolicy] = Field(
        default_factory=dict,
        description="Per-table policy overrides keyed by table identifier",
    )
    adaptive: AdaptivePolicyConfig = Field(
        default_factory=AdaptivePolicyConfig,
        description="Adaptive policy engine configuration for access-frequency-based scheduling",
    )
    execution: ExecutionConfig = Field(
        default_factory=ExecutionConfig,
        description="Compaction execution layer configuration (local vs. Flink routing)",
    )

    def get_policy(self, table_id: str) -> TablePolicy:
        """Get the effective policy for a table (override or default)."""
        return self.table_overrides.get(table_id, self.default_policy)
