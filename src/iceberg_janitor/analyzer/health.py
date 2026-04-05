"""Table health assessment using DuckDB to query Iceberg metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import duckdb
import structlog

from iceberg_janitor.analyzer.queries import (
    FILE_SIZE_DISTRIBUTION,
    FILE_STATS,
    PARTITION_FILE_COUNTS,
    SNAPSHOT_STATS,
)

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


def assess_table(
    table_path: str,
    small_file_threshold: int = DEFAULT_SMALL_FILE_THRESHOLD_BYTES,
    partition_min_files: int = 10,
    partition_limit: int = 20,
) -> HealthReport:
    """Analyze an Iceberg table's health using DuckDB.

    Args:
        table_path: Path to the Iceberg table (s3://bucket/table or local path).
        small_file_threshold: Files smaller than this (bytes) are counted as "small".
        partition_min_files: Only report partitions with more files than this.
        partition_limit: Max number of hot partitions to return.

    Returns:
        HealthReport with file stats, snapshot stats, size distribution, and hot partitions.
    """
    log = logger.bind(table_path=table_path)
    log.info("assessing_table_health")

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    errors: list[str] = []
    assessed_at = datetime.now()

    # File statistics
    try:
        result = conn.execute(
            FILE_STATS.format(
                table_path=table_path,
                small_file_threshold=small_file_threshold,
            )
        ).fetchone()
        file_stats = FileStats(*result) if result else FileStats()
    except Exception as e:
        log.error("file_stats_failed", error=str(e))
        errors.append(f"File stats query failed: {e}")
        file_stats = FileStats()

    # Snapshot statistics
    try:
        result = conn.execute(SNAPSHOT_STATS.format(table_path=table_path)).fetchone()
        snapshot_stats = SnapshotStats(*result) if result else SnapshotStats()
    except Exception as e:
        log.error("snapshot_stats_failed", error=str(e))
        errors.append(f"Snapshot stats query failed: {e}")
        snapshot_stats = SnapshotStats()

    # File size distribution
    size_distribution: list[SizeBucket] = []
    try:
        rows = conn.execute(
            FILE_SIZE_DISTRIBUTION.format(table_path=table_path)
        ).fetchall()
        size_distribution = [SizeBucket(*row) for row in rows]
    except Exception as e:
        log.error("size_distribution_failed", error=str(e))
        errors.append(f"Size distribution query failed: {e}")

    # Hot partitions
    hot_partitions: list[PartitionHealth] = []
    try:
        rows = conn.execute(
            PARTITION_FILE_COUNTS.format(
                table_path=table_path,
                min_files_per_partition=partition_min_files,
                limit=partition_limit,
            )
        ).fetchall()
        hot_partitions = [PartitionHealth(*row) for row in rows]
    except Exception as e:
        log.error("partition_stats_failed", error=str(e))
        errors.append(f"Partition stats query failed: {e}")

    conn.close()

    report = HealthReport(
        table_id=table_path,
        assessed_at=assessed_at,
        file_stats=file_stats,
        snapshot_stats=snapshot_stats,
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
