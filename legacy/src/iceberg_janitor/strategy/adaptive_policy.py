"""Adaptive policy engine — dynamically adjusts maintenance thresholds based on access patterns.

Hot tables get more aggressive compaction (lower thresholds) because query
performance matters most for frequently-accessed data.  Cold tables get
relaxed thresholds to conserve resources.  The engine wraps the existing
``PolicyConfig`` and transparently returns adjusted ``TablePolicy`` objects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import structlog

from iceberg_janitor.policy.models import PolicyConfig, TablePolicy
from iceberg_janitor.strategy.access_tracker import AccessClassification, AccessTracker

if TYPE_CHECKING:
    from iceberg_janitor.strategy.feedback_loop import FeedbackLoop

logger = structlog.get_logger()


class AdaptivePolicyEngine:
    """Wraps a static ``PolicyConfig`` and adapts thresholds per table heat.

    Parameters
    ----------
    base_config:
        The static policy configuration to use as a baseline.
    access_tracker:
        Source of access-frequency data.
    hot_multiplier:
        Thresholds for hot tables are multiplied by this value (< 1 means
        more aggressive compaction).
    warm_multiplier:
        Multiplier for warm tables — usually 1.0 (no change).
    cold_multiplier:
        Multiplier for cold tables (> 1 means less frequent compaction).
    feedback_loop:
        Optional :class:`FeedbackLoop` for further adjustment based on
        measured compaction effectiveness.
    """

    def __init__(
        self,
        base_config: PolicyConfig,
        access_tracker: AccessTracker,
        *,
        hot_multiplier: float = 0.5,
        warm_multiplier: float = 1.0,
        cold_multiplier: float = 3.0,
        feedback_loop: FeedbackLoop | None = None,
    ) -> None:
        self.base_config = base_config
        self.access_tracker = access_tracker
        self.hot_multiplier = hot_multiplier
        self.warm_multiplier = warm_multiplier
        self.cold_multiplier = cold_multiplier
        self.feedback_loop = feedback_loop

        # Audit log of recent adjustments (bounded)
        self._adjustment_log: list[dict[str, Any]] = []
        self._max_log_entries = 1000

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    def get_effective_policy(self, table_id: str) -> TablePolicy:
        """Return the dynamically-adjusted policy for *table_id*.

        The base policy (from ``PolicyConfig``) is modified by:

        1. Access-tier multiplier (hot / warm / cold).
        2. Feedback-loop refinement (if enabled).
        """
        base = self.base_config.get_policy(table_id)
        classification = self.access_tracker.get_classification(table_id)
        multiplier = self._tier_multiplier(classification)

        # Apply feedback-loop adjustment if available
        feedback_mult = 1.0
        if self.feedback_loop is not None:
            adjustment = self.feedback_loop.recommend_adjustment(table_id)
            feedback_mult = adjustment.threshold_multiplier

        effective_mult = multiplier * feedback_mult

        adjusted = self._apply_multiplier(base, effective_mult, classification)

        self._log_adjustment(
            table_id=table_id,
            classification=classification,
            tier_multiplier=multiplier,
            feedback_multiplier=feedback_mult,
            effective_multiplier=effective_mult,
        )

        return adjusted

    def get_priority_boost(self, table_id: str) -> float:
        """Return an additive priority boost for *table_id* based on heat.

        Returns a value in [0, 100] that should be added to the scheduler's
        base priority score.

        - Hot tables: up to +100 boost
        - Warm tables: up to +30 boost
        - Cold tables: 0 boost (rely on normal degradation scoring)
        """
        heat = self.access_tracker.get_heat_score(table_id)
        classification = self.access_tracker.get_classification(table_id)

        if classification == "hot":
            # Linear scale: heat 0.8->1.0 maps to boost 60->100
            boost = 60.0 + (heat - 0.0) * 40.0
        elif classification == "warm":
            boost = heat * 30.0
        else:
            boost = 0.0

        # Feedback adjustment
        adj = self.access_tracker.get_priority_adjustment(table_id)
        boost *= adj

        return round(max(0.0, min(100.0, boost)), 2)

    def get_adjustment_log(self, table_id: str | None = None) -> list[dict[str, Any]]:
        """Return recent policy adjustment entries, optionally filtered."""
        if table_id is None:
            return list(self._adjustment_log)
        return [e for e in self._adjustment_log if e.get("table_id") == table_id]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _tier_multiplier(self, classification: AccessClassification) -> float:
        """Return the threshold multiplier for the given tier."""
        if classification == "hot":
            return self.hot_multiplier
        if classification == "warm":
            return self.warm_multiplier
        return self.cold_multiplier

    def _apply_multiplier(
        self,
        base: TablePolicy,
        multiplier: float,
        classification: AccessClassification,
    ) -> TablePolicy:
        """Create a new ``TablePolicy`` with thresholds scaled by *multiplier*.

        - Trigger thresholds are multiplied (lower multiplier = trigger sooner).
        - Retention periods are inversely adjusted for hot tables (shorter
          retention) and directly for cold tables (longer retention).
        - Values are clamped to sane minimums.
        """
        data = base.model_dump()

        # --- Scale trigger thresholds (lower = more aggressive) ---
        int_fields_to_scale = [
            "commit_count_trigger_threshold",
            "file_count_trigger_threshold",
            "time_trigger_interval_minutes",
            "size_trigger_threshold_bytes",
            "max_file_count",
        ]
        for field_name in int_fields_to_scale:
            original = data.get(field_name, 0)
            if original > 0:
                data[field_name] = max(1, int(original * multiplier))

        data["max_small_file_ratio"] = max(
            0.05, min(0.95, base.max_small_file_ratio * multiplier)
        )

        # --- Adjust retention (hot = shorter, cold = longer) ---
        if classification == "hot":
            # Shorter retention for hot tables (divide by inverse of mult)
            retention_factor = max(multiplier, 0.25)
            data["snapshot_retention_hours"] = max(
                1, int(base.snapshot_retention_hours * retention_factor)
            )
            data["orphan_retention_hours"] = max(
                1, int(base.orphan_retention_hours * retention_factor)
            )
        elif classification == "cold":
            # Longer retention for cold tables
            retention_factor = min(multiplier, 5.0)
            data["snapshot_retention_hours"] = int(
                base.snapshot_retention_hours * retention_factor
            )
            data["orphan_retention_hours"] = int(
                base.orphan_retention_hours * retention_factor
            )
        # warm: keep defaults

        return TablePolicy(**data)

    def _log_adjustment(self, **kwargs: Any) -> None:
        """Append an entry to the audit log."""
        import time

        entry = {"timestamp": time.time(), **kwargs}
        self._adjustment_log.append(entry)
        if len(self._adjustment_log) > self._max_log_entries:
            self._adjustment_log = self._adjustment_log[-self._max_log_entries:]

        logger.debug(
            "adaptive_policy_applied",
            table_id=kwargs.get("table_id"),
            classification=kwargs.get("classification"),
            effective_multiplier=kwargs.get("effective_multiplier"),
        )
