"""Partition-aware maintenance analysis and recommendations.

Instead of blindly compacting an entire table, the :class:`PartitionAnalyzer`
inspects per-partition file counts and sizes, identifies which partitions are
degraded, and produces targeted maintenance recommendations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from pyiceberg.catalog import Catalog

logger = structlog.get_logger()


@dataclass(frozen=True)
class PartitionStats:
    """Per-partition file statistics."""

    partition_key: str
    file_count: int
    total_bytes: int
    small_file_count: int
    avg_file_size_bytes: float

    @property
    def small_file_ratio(self) -> float:
        return self.small_file_count / self.file_count if self.file_count > 0 else 0.0

    @property
    def total_mb(self) -> float:
        return self.total_bytes / (1024 * 1024)


@dataclass(frozen=True)
class PartitionRecommendation:
    """A maintenance recommendation for a single partition."""

    partition_key: str
    action: str  # "compact", "prune", "reorder"
    reason: str
    priority: float
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PartitionAnalysis:
    """Complete partition-level analysis result for a table."""

    table_id: str
    analyzed_at: datetime
    partitions: list[PartitionStats]
    recommendations: list[PartitionRecommendation]
    total_partitions: int
    degraded_partitions: int

    @property
    def needs_partition_maintenance(self) -> bool:
        return self.degraded_partitions > 0


class PartitionAnalyzer:
    """Analyzes per-partition health and produces targeted maintenance recommendations.

    Parameters
    ----------
    small_file_threshold_bytes:
        Files smaller than this are considered "small" (default 8 MiB).
    small_file_ratio_threshold:
        Partitions exceeding this small-file ratio need compaction (default 0.3).
    min_files_to_evaluate:
        Only evaluate partitions with at least this many files (default 5).
    prune_empty_partitions:
        Whether to recommend pruning for partitions with 0 rows (default True).
    """

    def __init__(
        self,
        small_file_threshold_bytes: int = 8 * 1024 * 1024,
        small_file_ratio_threshold: float = 0.3,
        min_files_to_evaluate: int = 5,
        prune_empty_partitions: bool = True,
    ) -> None:
        self.small_file_threshold_bytes = small_file_threshold_bytes
        self.small_file_ratio_threshold = small_file_ratio_threshold
        self.min_files_to_evaluate = min_files_to_evaluate
        self.prune_empty_partitions = prune_empty_partitions

    def analyze(self, catalog: Catalog, table_id: str) -> PartitionAnalysis:
        """Analyze partition health for *table_id*.

        Reads the current plan files via PyIceberg, buckets them by partition,
        and produces recommendations for partitions that need attention.
        """
        log = logger.bind(table_id=table_id)
        log.info("partition_analysis_starting")

        table = catalog.load_table(table_id)
        scan = table.scan()
        plan_files = list(scan.plan_files())

        # Group files by partition value string
        partition_files: dict[str, list[Any]] = {}
        for task in plan_files:
            # Build a stable partition key from the record's partition data
            part_key = self._partition_key(task)
            partition_files.setdefault(part_key, []).append(task)

        partition_stats: list[PartitionStats] = []
        for part_key, files in partition_files.items():
            file_count = len(files)
            total_bytes = sum(t.file.file_size_in_bytes for t in files)
            small_count = sum(
                1 for t in files if t.file.file_size_in_bytes < self.small_file_threshold_bytes
            )
            avg_size = total_bytes / file_count if file_count > 0 else 0
            partition_stats.append(
                PartitionStats(
                    partition_key=part_key,
                    file_count=file_count,
                    total_bytes=total_bytes,
                    small_file_count=small_count,
                    avg_file_size_bytes=avg_size,
                )
            )

        # Sort so the most-files partitions come first (useful for display)
        partition_stats.sort(key=lambda p: p.file_count, reverse=True)

        # Generate recommendations
        recommendations = self._generate_recommendations(partition_stats)
        degraded = sum(1 for r in recommendations if r.action == "compact")

        analysis = PartitionAnalysis(
            table_id=table_id,
            analyzed_at=datetime.now(timezone.utc),
            partitions=partition_stats,
            recommendations=recommendations,
            total_partitions=len(partition_stats),
            degraded_partitions=degraded,
        )

        log.info(
            "partition_analysis_complete",
            total_partitions=analysis.total_partitions,
            degraded_partitions=analysis.degraded_partitions,
            recommendations=len(recommendations),
        )
        return analysis

    def filter_partitions(
        self,
        catalog: Catalog,
        table_id: str,
        partition_filter: str,
    ) -> PartitionAnalysis:
        """Analyze only partitions matching *partition_filter*.

        The filter is matched as a substring against partition key strings.
        """
        full = self.analyze(catalog, table_id)
        filtered_parts = [
            p for p in full.partitions if partition_filter in p.partition_key
        ]
        filtered_recs = [
            r for r in full.recommendations if partition_filter in r.partition_key
        ]
        return PartitionAnalysis(
            table_id=full.table_id,
            analyzed_at=full.analyzed_at,
            partitions=filtered_parts,
            recommendations=filtered_recs,
            total_partitions=len(filtered_parts),
            degraded_partitions=sum(1 for r in filtered_recs if r.action == "compact"),
        )

    def get_compaction_targets(
        self,
        catalog: Catalog,
        table_id: str,
    ) -> list[str]:
        """Return a list of partition keys that should be compacted."""
        analysis = self.analyze(catalog, table_id)
        return [
            r.partition_key
            for r in analysis.recommendations
            if r.action == "compact"
        ]

    def suggest_sort_order(
        self,
        catalog: Catalog,
        table_id: str,
    ) -> list[dict[str, Any]]:
        """Suggest sort order optimisations based on partition file layout.

        Returns a list of suggestions, each with a description and estimated
        improvement.
        """
        log = logger.bind(table_id=table_id)
        table = catalog.load_table(table_id)
        suggestions: list[dict[str, Any]] = []

        spec = table.metadata.default_sort_order
        if spec is None or len(spec.fields) == 0:
            suggestions.append({
                "suggestion": "add_sort_order",
                "description": (
                    "Table has no sort order defined. Adding a sort order on "
                    "frequently-filtered columns can improve query performance "
                    "and reduce the number of files scanned."
                ),
                "estimated_improvement": "high",
            })

        analysis = self.analyze(catalog, table_id)
        large_partitions = [p for p in analysis.partitions if p.file_count > 100]
        if large_partitions:
            suggestions.append({
                "suggestion": "review_partition_granularity",
                "description": (
                    f"{len(large_partitions)} partition(s) have >100 files. "
                    "Consider a finer-grained partition scheme or more frequent compaction."
                ),
                "estimated_improvement": "medium",
                "partitions": [p.partition_key for p in large_partitions[:10]],
            })

        log.info("sort_order_suggestions", count=len(suggestions))
        return suggestions

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _generate_recommendations(
        self, partitions: list[PartitionStats]
    ) -> list[PartitionRecommendation]:
        recs: list[PartitionRecommendation] = []
        for p in partitions:
            # Skip tiny partitions
            if p.file_count < self.min_files_to_evaluate:
                continue

            # Compaction recommendation
            if p.small_file_ratio > self.small_file_ratio_threshold:
                recs.append(
                    PartitionRecommendation(
                        partition_key=p.partition_key,
                        action="compact",
                        reason=(
                            f"Small file ratio {p.small_file_ratio:.1%} exceeds "
                            f"threshold {self.small_file_ratio_threshold:.1%} "
                            f"({p.small_file_count}/{p.file_count} files)"
                        ),
                        priority=p.small_file_ratio * 100 + p.small_file_count,
                        details={
                            "small_file_count": p.small_file_count,
                            "total_bytes": p.total_bytes,
                        },
                    )
                )

            # Prune empty-data partitions (0 total bytes but files exist)
            if self.prune_empty_partitions and p.total_bytes == 0 and p.file_count > 0:
                recs.append(
                    PartitionRecommendation(
                        partition_key=p.partition_key,
                        action="prune",
                        reason=f"Partition has {p.file_count} file(s) but 0 bytes of data",
                        priority=10,
                    )
                )

        # Sort by priority descending
        recs.sort(key=lambda r: r.priority, reverse=True)
        return recs

    @staticmethod
    def _partition_key(task: Any) -> str:
        """Extract a string partition key from a :class:`FileScanTask`.

        Falls back to ``"unpartitioned"`` when no partition data is available.
        """
        try:
            record = task.file.partition
            if record is None:
                return "unpartitioned"
            # record is a pyiceberg Record; convert to dict-style string
            as_dict = {k: v for k, v in record.items() if v is not None}
            if not as_dict:
                return "unpartitioned"
            return "/".join(f"{k}={v}" for k, v in sorted(as_dict.items()))
        except Exception:
            return "unpartitioned"
