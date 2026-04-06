"""Self-correcting feedback loop for compaction effectiveness.

After each compaction, the loop compares pre- and post-compaction metrics
to determine whether the maintenance actually helped.  Future scheduling
aggressiveness is then adjusted:

* **High effectiveness** — query latency dropped, file count shrank.
  Increase the table's maintenance priority further.
* **Low effectiveness** — little measurable improvement.
  Decrease priority to avoid wasting compute.
* **Negative effectiveness** — metrics worsened (rare, e.g., table grew
  significantly during compaction).  Skip the next maintenance cycle.
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any

import structlog

from iceberg_janitor.strategy.access_tracker import AccessTracker

logger = structlog.get_logger()


@dataclass
class CompactionMetrics:
    """Snapshot of table metrics taken before or after compaction."""

    timestamp: float = field(default_factory=time.time)
    file_count: int = 0
    small_file_count: int = 0
    total_bytes: int = 0
    avg_file_size_bytes: float = 0.0
    avg_query_latency_ms: float = 0.0
    snapshot_count: int = 0


@dataclass
class EffectivenessRecord:
    """Comparison of pre- and post-compaction metrics."""

    table_id: str
    recorded_at: float = field(default_factory=time.time)
    before: CompactionMetrics = field(default_factory=CompactionMetrics)
    after: CompactionMetrics = field(default_factory=CompactionMetrics)
    effectiveness_score: float = 0.0  # -1.0 to 1.0
    details: dict[str, float] = field(default_factory=dict)


@dataclass
class PolicyAdjustment:
    """Recommended threshold adjustment based on effectiveness history."""

    table_id: str
    threshold_multiplier: float = 1.0  # < 1 = more aggressive, > 1 = less
    skip_next_cycle: bool = False
    reason: str = ""
    trend: str = "stable"  # "improving", "stable", "degrading"
    avg_effectiveness: float = 0.0


class FeedbackLoop:
    """Tracks compaction effectiveness and adjusts future scheduling.

    Parameters
    ----------
    access_tracker:
        Used to read current latency metrics and set priority adjustments.
    lookback_hours:
        How far back to look when computing trend-based adjustments.
    settling_seconds:
        Minimum time to wait after compaction before measuring post-metrics.
    max_history_per_table:
        Maximum effectiveness records retained per table.
    """

    def __init__(
        self,
        access_tracker: AccessTracker,
        *,
        lookback_hours: int = 24,
        settling_seconds: int = 300,
        max_history_per_table: int = 100,
    ) -> None:
        self.access_tracker = access_tracker
        self.lookback_hours = lookback_hours
        self.settling_seconds = settling_seconds
        self.max_history_per_table = max_history_per_table

        self._lock = threading.Lock()
        self._history: dict[str, deque[EffectivenessRecord]] = defaultdict(
            lambda: deque(maxlen=max_history_per_table)
        )
        # Pending compactions awaiting post-measurement
        self._pending: dict[str, CompactionMetrics] = {}

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_pre_compaction(
        self, table_id: str, metrics: CompactionMetrics
    ) -> None:
        """Capture metrics before a compaction starts."""
        with self._lock:
            self._pending[table_id] = metrics
        logger.debug(
            "feedback_pre_compaction_recorded",
            table_id=table_id,
            file_count=metrics.file_count,
        )

    def record_post_compaction(
        self, table_id: str, metrics: CompactionMetrics
    ) -> EffectivenessRecord | None:
        """Capture metrics after compaction and compute effectiveness.

        Returns the new ``EffectivenessRecord`` or ``None`` if no
        pre-compaction baseline was stored.
        """
        with self._lock:
            before = self._pending.pop(table_id, None)
            if before is None:
                logger.warning(
                    "feedback_no_baseline",
                    table_id=table_id,
                )
                return None

            score, details = self._compute_effectiveness(before, metrics)
            record = EffectivenessRecord(
                table_id=table_id,
                before=before,
                after=metrics,
                effectiveness_score=score,
                details=details,
            )
            self._history[table_id].append(record)

            # Apply adjustment to the access tracker
            self._apply_adjustment_unlocked(table_id)

        logger.info(
            "feedback_effectiveness_recorded",
            table_id=table_id,
            score=round(score, 3),
            details=details,
        )
        return record

    # ------------------------------------------------------------------
    # Effectiveness computation
    # ------------------------------------------------------------------

    @staticmethod
    def compute_effectiveness(
        before: CompactionMetrics, after: CompactionMetrics
    ) -> float:
        """Public static helper — returns -1.0 to 1.0 effectiveness score."""
        score, _ = FeedbackLoop._compute_effectiveness(before, after)
        return score

    @staticmethod
    def _compute_effectiveness(
        before: CompactionMetrics, after: CompactionMetrics
    ) -> tuple[float, dict[str, float]]:
        """Compute effectiveness score and per-dimension breakdown.

        Dimensions (weighted):
        - File count reduction: 35 %
        - Small file count reduction: 25 %
        - Query latency improvement: 25 %
        - Average file size increase: 15 %

        Each dimension is normalised to [-1, 1] where positive = improvement.
        """
        details: dict[str, float] = {}

        # File count reduction
        if before.file_count > 0:
            file_delta = (before.file_count - after.file_count) / before.file_count
            details["file_count_reduction"] = round(file_delta, 4)
        else:
            file_delta = 0.0
            details["file_count_reduction"] = 0.0

        # Small file count reduction
        if before.small_file_count > 0:
            small_delta = (
                (before.small_file_count - after.small_file_count)
                / before.small_file_count
            )
            details["small_file_reduction"] = round(small_delta, 4)
        else:
            small_delta = 0.0
            details["small_file_reduction"] = 0.0

        # Query latency improvement
        if before.avg_query_latency_ms > 0:
            latency_delta = (
                (before.avg_query_latency_ms - after.avg_query_latency_ms)
                / before.avg_query_latency_ms
            )
            details["latency_improvement"] = round(latency_delta, 4)
        else:
            latency_delta = 0.0
            details["latency_improvement"] = 0.0

        # Average file size increase
        if before.avg_file_size_bytes > 0:
            size_delta = (
                (after.avg_file_size_bytes - before.avg_file_size_bytes)
                / before.avg_file_size_bytes
            )
            details["avg_file_size_increase"] = round(size_delta, 4)
        else:
            size_delta = 0.0
            details["avg_file_size_increase"] = 0.0

        # Weighted combination
        score = (
            0.35 * _clamp(file_delta)
            + 0.25 * _clamp(small_delta)
            + 0.25 * _clamp(latency_delta)
            + 0.15 * _clamp(size_delta)
        )
        return round(max(-1.0, min(1.0, score)), 4), details

    # ------------------------------------------------------------------
    # Recommendation
    # ------------------------------------------------------------------

    def recommend_adjustment(self, table_id: str) -> PolicyAdjustment:
        """Suggest a threshold adjustment based on recent effectiveness history."""
        with self._lock:
            records = list(self._history.get(table_id, []))

        if not records:
            return PolicyAdjustment(
                table_id=table_id,
                threshold_multiplier=1.0,
                reason="No effectiveness history available",
                trend="stable",
            )

        # Filter to lookback window
        cutoff = time.time() - (self.lookback_hours * 3600)
        recent = [r for r in records if r.recorded_at >= cutoff]
        if not recent:
            recent = records[-3:]  # fall back to last 3 records

        avg_score = sum(r.effectiveness_score for r in recent) / len(recent)

        # Determine trend from last few records
        if len(recent) >= 2:
            first_half = recent[: len(recent) // 2]
            second_half = recent[len(recent) // 2 :]
            avg_first = sum(r.effectiveness_score for r in first_half) / len(first_half)
            avg_second = sum(r.effectiveness_score for r in second_half) / len(second_half)
            if avg_second > avg_first + 0.1:
                trend = "improving"
            elif avg_second < avg_first - 0.1:
                trend = "degrading"
            else:
                trend = "stable"
        else:
            trend = "stable"

        # Build recommendation
        if avg_score > 0.5:
            # Compaction is very effective — be more aggressive
            multiplier = 0.7
            reason = (
                f"High avg effectiveness ({avg_score:.2f}): "
                "reducing thresholds for more frequent compaction"
            )
        elif avg_score > 0.2:
            # Moderately effective — slightly more aggressive
            multiplier = 0.85
            reason = (
                f"Moderate avg effectiveness ({avg_score:.2f}): "
                "slightly reducing thresholds"
            )
        elif avg_score > -0.1:
            # Minimal effect — keep defaults
            multiplier = 1.0
            reason = (
                f"Low avg effectiveness ({avg_score:.2f}): "
                "keeping default thresholds"
            )
        elif avg_score > -0.5:
            # Negative — relax thresholds
            multiplier = 1.5
            reason = (
                f"Negative avg effectiveness ({avg_score:.2f}): "
                "relaxing thresholds to reduce unnecessary compaction"
            )
        else:
            # Very negative — skip next cycle
            multiplier = 2.0
            reason = (
                f"Very negative avg effectiveness ({avg_score:.2f}): "
                "strongly relaxing thresholds, consider skipping next cycle"
            )

        skip = avg_score < -0.5 and trend == "degrading"

        return PolicyAdjustment(
            table_id=table_id,
            threshold_multiplier=multiplier,
            skip_next_cycle=skip,
            reason=reason,
            trend=trend,
            avg_effectiveness=round(avg_score, 4),
        )

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def get_history(self, table_id: str) -> list[dict[str, Any]]:
        """Return serialisable effectiveness history for *table_id*."""
        with self._lock:
            records = list(self._history.get(table_id, []))
        return [
            {
                "table_id": r.table_id,
                "recorded_at": r.recorded_at,
                "effectiveness_score": r.effectiveness_score,
                "details": r.details,
                "before": {
                    "file_count": r.before.file_count,
                    "small_file_count": r.before.small_file_count,
                    "total_bytes": r.before.total_bytes,
                    "avg_file_size_bytes": r.before.avg_file_size_bytes,
                    "avg_query_latency_ms": r.before.avg_query_latency_ms,
                },
                "after": {
                    "file_count": r.after.file_count,
                    "small_file_count": r.after.small_file_count,
                    "total_bytes": r.after.total_bytes,
                    "avg_file_size_bytes": r.after.avg_file_size_bytes,
                    "avg_query_latency_ms": r.after.avg_query_latency_ms,
                },
            }
            for r in records
        ]

    def get_metrics_summary(self) -> dict[str, Any]:
        """Return aggregate metrics for dashboarding."""
        with self._lock:
            table_count = len(self._history)
            total_records = sum(len(v) for v in self._history.values())
            all_scores = [
                r.effectiveness_score
                for records in self._history.values()
                for r in records
            ]

        if all_scores:
            avg_score = sum(all_scores) / len(all_scores)
            min_score = min(all_scores)
            max_score = max(all_scores)
        else:
            avg_score = min_score = max_score = 0.0

        return {
            "tracked_tables": table_count,
            "total_records": total_records,
            "pending_measurements": len(self._pending),
            "avg_effectiveness": round(avg_score, 4),
            "min_effectiveness": round(min_score, 4),
            "max_effectiveness": round(max_score, 4),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _apply_adjustment_unlocked(self, table_id: str) -> None:
        """Update the access tracker's priority adjustment based on recent records."""
        records = list(self._history.get(table_id, []))
        if not records:
            return

        # Use the last 5 records (or fewer)
        recent = records[-5:]
        avg = sum(r.effectiveness_score for r in recent) / len(recent)

        # Map effectiveness to priority adjustment:
        # avg 1.0 -> adjustment 1.5 (boost)
        # avg 0.0 -> adjustment 1.0 (neutral)
        # avg -1.0 -> adjustment 0.5 (reduce)
        adjustment = 1.0 + (avg * 0.5)
        adjustment = max(0.3, min(2.0, adjustment))

        self.access_tracker.set_priority_adjustment(table_id, adjustment)
        logger.debug(
            "feedback_priority_adjusted",
            table_id=table_id,
            avg_effectiveness=round(avg, 3),
            new_adjustment=round(adjustment, 3),
        )


def _clamp(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    """Clamp *value* to [lo, hi]."""
    return max(lo, min(hi, value))
