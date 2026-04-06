"""Maintenance scheduler — tracks table state, evaluates triggers, and prioritises work.

The scheduler is the brain of the strategy module.  It keeps per-table state
(last compaction time, commit counts, etc.), evaluates triggers, and produces a
priority-ordered list of tables that need maintenance.

When an :class:`AccessTracker` and :class:`AdaptivePolicyEngine` are attached,
the scheduler boosts priority for hot tables and uses dynamically-adjusted
trigger thresholds.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dt_time, timezone
from typing import TYPE_CHECKING, Any

import structlog

from iceberg_janitor.policy.models import TablePolicy
from iceberg_janitor.strategy.triggers import Trigger, TriggerResult, build_triggers_from_policy

if TYPE_CHECKING:
    from iceberg_janitor.strategy.access_tracker import AccessTracker
    from iceberg_janitor.strategy.adaptive_policy import AdaptivePolicyEngine

logger = structlog.get_logger()


@dataclass
class TableState:
    """Observable state of a single Iceberg table for trigger evaluation."""

    table_id: str

    # Commit tracking
    commits_since_last_compaction: int = 0
    current_snapshot_id: int | None = None

    # File tracking
    small_file_count: int = 0
    total_file_count: int = 0
    uncompacted_bytes: int = 0

    # Time tracking
    last_compaction_at: datetime | None = None
    last_evaluated_at: datetime | None = None

    # Metadata
    last_health_score: float = 1.0  # 1.0 = healthy, 0.0 = worst


@dataclass
class ScheduledAction:
    """A maintenance action that has been prioritised by the scheduler."""

    table_id: str
    priority: float  # higher = more urgent
    trigger_result: TriggerResult
    requested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class MaintenanceScheduler:
    """Stateful maintenance scheduler.

    Features
    --------
    * Tracks per-table :class:`TableState` objects.
    * Evaluates :class:`Trigger` trees per table.
    * Prioritises tables: most degraded first.
    * Enforces maintenance windows (only act during off-peak hours).
    * Rate limits concurrent compactions.
    """

    def __init__(
        self,
        max_concurrent: int = 2,
        maintenance_window_start: dt_time | None = None,
        maintenance_window_end: dt_time | None = None,
        access_tracker: AccessTracker | None = None,
        adaptive_policy_engine: AdaptivePolicyEngine | None = None,
    ) -> None:
        self._states: dict[str, TableState] = {}
        self._triggers: dict[str, Trigger] = {}
        self._lock = threading.Lock()
        self._active_compactions: int = 0
        self.max_concurrent = max_concurrent
        self.maintenance_window_start = maintenance_window_start
        self.maintenance_window_end = maintenance_window_end
        self.access_tracker = access_tracker
        self.adaptive_policy_engine = adaptive_policy_engine

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def register_table(
        self,
        table_id: str,
        policy: TablePolicy,
        initial_state: TableState | None = None,
    ) -> None:
        """Register a table with its trigger tree derived from *policy*.

        When an :class:`AdaptivePolicyEngine` is attached, the effective
        (heat-adjusted) policy is used to build triggers instead of the
        static policy.
        """
        effective = policy
        if self.adaptive_policy_engine is not None:
            try:
                effective = self.adaptive_policy_engine.get_effective_policy(table_id)
            except Exception:
                logger.warning(
                    "adaptive_policy_fallback",
                    table_id=table_id,
                    reason="falling back to static policy",
                )
        with self._lock:
            self._states[table_id] = initial_state or TableState(table_id=table_id)
            self._triggers[table_id] = build_triggers_from_policy(effective)
        logger.info("table_registered", table_id=table_id)

    def unregister_table(self, table_id: str) -> None:
        with self._lock:
            self._states.pop(table_id, None)
            self._triggers.pop(table_id, None)

    def get_state(self, table_id: str) -> TableState | None:
        return self._states.get(table_id)

    def update_state(self, table_id: str, **updates: Any) -> None:
        """Merge *updates* into the table's current state."""
        with self._lock:
            state = self._states.get(table_id)
            if state is None:
                logger.warning("update_state_unknown_table", table_id=table_id)
                return
            for key, value in updates.items():
                if hasattr(state, key):
                    setattr(state, key, value)
                else:
                    logger.warning("unknown_state_field", table_id=table_id, field=key)

    def record_new_commit(self, table_id: str, snapshot_id: int) -> None:
        """Record that a new commit (snapshot) was observed for *table_id*."""
        with self._lock:
            state = self._states.get(table_id)
            if state is None:
                return
            if state.current_snapshot_id != snapshot_id:
                state.current_snapshot_id = snapshot_id
                state.commits_since_last_compaction += 1

    def record_compaction_done(self, table_id: str) -> None:
        """Record that a compaction was successfully completed."""
        with self._lock:
            state = self._states.get(table_id)
            if state is None:
                return
            state.commits_since_last_compaction = 0
            state.last_compaction_at = datetime.now(timezone.utc)
            state.uncompacted_bytes = 0

    # ------------------------------------------------------------------
    # Maintenance window
    # ------------------------------------------------------------------

    def is_within_maintenance_window(self) -> bool:
        """Return *True* when the current UTC time falls inside the maintenance window.

        If no window is configured, maintenance is always allowed.
        """
        if self.maintenance_window_start is None or self.maintenance_window_end is None:
            return True

        now = datetime.now(timezone.utc).time()
        start = self.maintenance_window_start
        end = self.maintenance_window_end

        if start <= end:
            # e.g. 02:00 – 06:00
            return start <= now <= end
        # Window crosses midnight, e.g. 22:00 – 04:00
        return now >= start or now <= end

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def acquire_compaction_slot(self) -> bool:
        """Try to acquire a compaction slot.  Returns *True* on success."""
        with self._lock:
            if self._active_compactions >= self.max_concurrent:
                return False
            self._active_compactions += 1
            return True

    def release_compaction_slot(self) -> None:
        with self._lock:
            self._active_compactions = max(0, self._active_compactions - 1)

    @property
    def active_compactions(self) -> int:
        return self._active_compactions

    # ------------------------------------------------------------------
    # Evaluation & prioritisation
    # ------------------------------------------------------------------

    def evaluate_table(self, table_id: str) -> TriggerResult | None:
        """Evaluate triggers for a single table and return the result."""
        trigger = self._triggers.get(table_id)
        state = self._states.get(table_id)
        if trigger is None or state is None:
            return None
        result = trigger.evaluate(state)
        with self._lock:
            state.last_evaluated_at = datetime.now(timezone.utc)
        return result

    def evaluate_all(self) -> list[ScheduledAction]:
        """Evaluate all registered tables and return priority-ordered actions.

        Tables whose triggers fire are returned sorted by a degradation score
        (most degraded first).  Tables outside the maintenance window or when
        no compaction slots are available are excluded.
        """
        if not self.is_within_maintenance_window():
            logger.debug("outside_maintenance_window")
            return []

        actions: list[ScheduledAction] = []

        for table_id in list(self._states):
            result = self.evaluate_table(table_id)
            if result is None or not result.fired:
                continue

            priority = self._compute_priority(table_id)
            actions.append(
                ScheduledAction(
                    table_id=table_id,
                    priority=priority,
                    trigger_result=result,
                )
            )

        # Sort descending by priority (most urgent first)
        actions.sort(key=lambda a: a.priority, reverse=True)
        return actions

    def _compute_priority(self, table_id: str) -> float:
        """Compute a degradation-based priority score for *table_id*.

        Higher score = more urgent.  The score blends:
        - small file count (normalised by a reference of 1000)
        - commits since last compaction (normalised by 100)
        - time since last compaction in hours (normalised by 24)
        - uncompacted data in GiB
        - access heat score (when an AccessTracker is attached)
        """
        state = self._states.get(table_id)
        if state is None:
            return 0.0

        score = 0.0
        score += min(state.small_file_count / 1000, 5.0) * 25
        score += min(state.commits_since_last_compaction / 100, 5.0) * 20

        if state.last_compaction_at is not None:
            hours = (datetime.now(timezone.utc) - state.last_compaction_at).total_seconds() / 3600
            score += min(hours / 24, 5.0) * 15
        else:
            score += 75  # never compacted — high urgency

        gib = state.uncompacted_bytes / (1024 ** 3)
        score += min(gib, 10.0) * 10

        # Inverse health score adds up to 30
        score += (1.0 - state.last_health_score) * 30

        # --- Access-frequency boost ---
        # Hot tables get up to +100 additional priority so they are compacted
        # sooner, creating the self-correcting feedback loop.
        if self.adaptive_policy_engine is not None:
            try:
                score += self.adaptive_policy_engine.get_priority_boost(table_id)
            except Exception:
                pass
        elif self.access_tracker is not None:
            try:
                heat = self.access_tracker.get_heat_score(table_id)
                score += heat * 50  # simpler boost without the full engine
            except Exception:
                pass

        return round(score, 2)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def table_ids(self) -> list[str]:
        return list(self._states.keys())

    def summary(self) -> dict[str, Any]:
        """Return a JSON-serialisable summary of the scheduler state."""
        rows = {}
        for tid, state in self._states.items():
            row: dict[str, Any] = {
                "commits_since_compaction": state.commits_since_last_compaction,
                "small_file_count": state.small_file_count,
                "uncompacted_bytes": state.uncompacted_bytes,
                "last_compaction_at": (
                    state.last_compaction_at.isoformat() if state.last_compaction_at else None
                ),
                "last_evaluated_at": (
                    state.last_evaluated_at.isoformat() if state.last_evaluated_at else None
                ),
                "health_score": state.last_health_score,
            }
            # Include access-tracking data when available
            if self.access_tracker is not None:
                try:
                    row["heat_score"] = self.access_tracker.get_heat_score(tid)
                    row["access_classification"] = self.access_tracker.get_classification(tid)
                except Exception:
                    pass
            rows[tid] = row
        return {
            "tables": rows,
            "active_compactions": self._active_compactions,
            "max_concurrent": self.max_concurrent,
            "in_maintenance_window": self.is_within_maintenance_window(),
            "adaptive_policy_enabled": self.adaptive_policy_engine is not None,
        }
