"""Table health assessment using the Iceberg catalog API only.

No DuckDB, no direct S3 access. All metadata comes through PyIceberg's
catalog interface, which works identically against REST Catalog, AWS Glue,
Hive Metastore, or any other Iceberg catalog implementation.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import median

import structlog

from pyiceberg.catalog import Catalog

logger = structlog.get_logger()

# Default: files under 8 MB are considered "small" for streaming workloads
DEFAULT_SMALL_FILE_THRESHOLD_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class FileStats:
    file_count: int = 0
    total_bytes: int = 0
    avg_file_size_bytes: float = 0.0
    min_file_size_bytes: int = 0
    max_file_size_bytes: int = 0
    median_file_size_bytes: float = 0.0
    small_file_count: int = 0

    @property
    def small_file_ratio(self) -> float:
        return self.small_file_count / self.file_count if self.file_count > 0 else 0.0

    @property
    def total_mb(self) -> float:
        return self.total_bytes / (1024 * 1024)

    @property
    def avg_file_size_mb(self) -> float:
        return self.avg_file_size_bytes / (1024 * 1024)


@dataclass(frozen=True)
class SnapshotStats:
    snapshot_count: int = 0
    oldest_snapshot_ts: datetime | None = None
    newest_snapshot_ts: datetime | None = None


@dataclass(frozen=True)
class SizeBucket:
    bucket: str
    file_count: int
    total_bytes: int


@dataclass(frozen=True)
class PartitionHealth:
    partition: str
    file_count: int
    total_bytes: int
    avg_file_size_bytes: float


@dataclass(frozen=True)
class HealthReport:
    """Complete health assessment for an Iceberg table."""

    table_id: str
    assessed_at: datetime
    file_stats: FileStats
    snapshot_stats: SnapshotStats
    format_version: int = 1
    size_distribution: list[SizeBucket] = field(default_factory=list)
    hot_partitions: list[PartitionHealth] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def needs_compaction(self) -> bool:
        return self.file_stats.small_file_ratio > 0.3

    @property
    def needs_snapshot_expiry(self) -> bool:
        return self.snapshot_stats.snapshot_count > 100

    @property
    def is_healthy(self) -> bool:
        return not self.needs_compaction and not self.needs_snapshot_expiry and not self.errors


def _classify_size(size: int) -> str:
    """Classify a file size into a human-readable bucket."""
    if size < 1_048_576:
        return "< 1 MB"
    if size < 10_485_760:
        return "1-10 MB"
    if size < 67_108_864:
        return "10-64 MB"
    if size < 134_217_728:
        return "64-128 MB"
    if size < 268_435_456:
        return "128-256 MB"
    return "> 256 MB"


def assess_table(
    catalog: Catalog,
    table_id: str,
    small_file_threshold: int = DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
    partition_min_files: int = 10,
    partition_limit: int = 20,
) -> HealthReport:
    """Analyze an Iceberg table's health using the catalog API only.

    All metadata is retrieved through PyIceberg's catalog interface.
    Works with any catalog backend: REST, Glue, Hive, SQL.

    Args:
        catalog: PyIceberg catalog instance.
        table_id: Fully qualified table identifier (namespace.table).
        small_file_threshold: Files smaller than this (bytes) are counted as "small".
        partition_min_files: Only report partitions with more files than this.
        partition_limit: Max number of hot partitions to return.

    Returns:
        HealthReport with file stats, snapshot stats, size distribution, and hot partitions.
    """
    log = logger.bind(table_id=table_id)
    log.info("assessing_table_health")

    errors: list[str] = []
    assessed_at = datetime.now(timezone.utc)

    try:
        table = catalog.load_table(table_id)
    except Exception as e:
        log.error("table_load_failed", error=str(e))
        return HealthReport(
            table_id=table_id,
            assessed_at=assessed_at,
            file_stats=FileStats(),
            snapshot_stats=SnapshotStats(),
            errors=[f"Failed to load table: {e}"],
        )

    format_version = table.metadata.format_version

    # ── Snapshot stats (from table metadata) ────────────────────────
    try:
        snapshots = table.metadata.snapshots
        snap_timestamps = [
            datetime.fromtimestamp(s.timestamp_ms / 1000, tz=timezone.utc)
            for s in snapshots
        ]
        snapshot_stats = SnapshotStats(
            snapshot_count=len(snapshots),
            oldest_snapshot_ts=min(snap_timestamps) if snap_timestamps else None,
            newest_snapshot_ts=max(snap_timestamps) if snap_timestamps else None,
        )
    except Exception as e:
        log.error("snapshot_stats_failed", error=str(e))
        errors.append(f"Snapshot stats failed: {e}")
        snapshot_stats = SnapshotStats()

    # ── File stats (from plan_files via catalog) ────────────────────
    try:
        plan_files = list(table.scan().plan_files())
        sizes = [task.file.file_size_in_bytes for task in plan_files]

        if sizes:
            small_count = sum(1 for s in sizes if s < small_file_threshold)
            file_stats = FileStats(
                file_count=len(sizes),
                total_bytes=sum(sizes),
                avg_file_size_bytes=sum(sizes) / len(sizes),
                min_file_size_bytes=min(sizes),
                max_file_size_bytes=max(sizes),
                median_file_size_bytes=median(sizes),
                small_file_count=small_count,
            )
        else:
            file_stats = FileStats()
    except Exception as e:
        log.error("file_stats_failed", error=str(e))
        errors.append(f"File stats failed: {e}")
        file_stats = FileStats()
        sizes = []

    # ── Size distribution ───────────────────────────────────────────
    size_distribution: list[SizeBucket] = []
    if sizes:
        try:
            buckets: dict[str, list[int]] = defaultdict(list)
            for s in sizes:
                buckets[_classify_size(s)].append(s)

            # Sort by smallest bucket first
            bucket_order = ["< 1 MB", "1-10 MB", "10-64 MB", "64-128 MB", "128-256 MB", "> 256 MB"]
            for bucket_name in bucket_order:
                if bucket_name in buckets:
                    bucket_sizes = buckets[bucket_name]
                    size_distribution.append(SizeBucket(
                        bucket=bucket_name,
                        file_count=len(bucket_sizes),
                        total_bytes=sum(bucket_sizes),
                    ))
        except Exception as e:
            log.error("size_distribution_failed", error=str(e))
            errors.append(f"Size distribution failed: {e}")

    # ── Hot partitions (from plan_files partition info) ──────────────
    hot_partitions: list[PartitionHealth] = []
    if plan_files:
        try:
            part_files: dict[str, list[int]] = defaultdict(list)
            for task in plan_files:
                # Build partition key from the file's partition data
                part_key = str(task.file.partition) if task.file.partition else "unpartitioned"
                part_files[part_key].append(task.file.file_size_in_bytes)

            for part_key, part_sizes in sorted(
                part_files.items(), key=lambda kv: len(kv[1]), reverse=True
            ):
                if len(part_sizes) <= partition_min_files:
                    continue
                hot_partitions.append(PartitionHealth(
                    partition=part_key,
                    file_count=len(part_sizes),
                    total_bytes=sum(part_sizes),
                    avg_file_size_bytes=sum(part_sizes) / len(part_sizes),
                ))
                if len(hot_partitions) >= partition_limit:
                    break
        except Exception as e:
            log.error("partition_stats_failed", error=str(e))
            errors.append(f"Partition stats failed: {e}")

    report = HealthReport(
        table_id=table_id,
        assessed_at=assessed_at,
        file_stats=file_stats,
        snapshot_stats=snapshot_stats,
        format_version=format_version,
        size_distribution=size_distribution,
        hot_partitions=hot_partitions,
        errors=errors,
    )

    log.info(
        "assessment_complete",
        file_count=file_stats.file_count,
        small_files=file_stats.small_file_count,
        snapshots=snapshot_stats.snapshot_count,
        healthy=report.is_healthy,
    )
    return report
