"""Trigger system for automatic maintenance scheduling.

Each trigger evaluates whether a table needs maintenance based on a single
dimension (commit count, file count, elapsed time, or data size).
``CompositeTrigger`` combines multiple triggers with AND/OR logic.
"""

from __future__ import annotations

import abc
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from iceberg_janitor.strategy.scheduler import TableState

logger = structlog.get_logger()


class Trigger(abc.ABC):
    """Base class for maintenance triggers."""

    @abc.abstractmethod
    def should_fire(self, state: TableState) -> bool:
        """Return *True* when this trigger's condition is met."""

    @abc.abstractmethod
    def describe(self) -> str:
        """Human-readable summary of the trigger condition."""

    def evaluate(self, state: TableState) -> TriggerResult:
        """Evaluate and return a structured result."""
        fired = self.should_fire(state)
        return TriggerResult(
            trigger_name=type(self).__name__,
            fired=fired,
            description=self.describe(),
            evaluated_at=datetime.now(timezone.utc),
        )


class TriggerResult:
    """Result of evaluating a single trigger against a table's state."""

    __slots__ = ("trigger_name", "fired", "description", "evaluated_at")

    def __init__(
        self,
        trigger_name: str,
        fired: bool,
        description: str,
        evaluated_at: datetime,
    ) -> None:
        self.trigger_name = trigger_name
        self.fired = fired
        self.description = description
        self.evaluated_at = evaluated_at

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"TriggerResult({self.trigger_name!r}, fired={self.fired}, "
            f"desc={self.description!r})"
        )


# ---------------------------------------------------------------------------
# Concrete triggers
# ---------------------------------------------------------------------------


class CommitCountTrigger(Trigger):
    """Fires after *threshold* commits since the last compaction."""

    def __init__(self, threshold: int = 50) -> None:
        if threshold < 1:
            raise ValueError("commit_count threshold must be >= 1")
        self.threshold = threshold

    def should_fire(self, state: TableState) -> bool:
        return state.commits_since_last_compaction >= self.threshold

    def describe(self) -> str:
        return f"commits_since_last_compaction >= {self.threshold}"


class FileCountTrigger(Trigger):
    """Fires when the number of small files exceeds *threshold*."""

    def __init__(self, threshold: int = 500) -> None:
        if threshold < 1:
            raise ValueError("file_count threshold must be >= 1")
        self.threshold = threshold

    def should_fire(self, state: TableState) -> bool:
        return state.small_file_count >= self.threshold

    def describe(self) -> str:
        return f"small_file_count >= {self.threshold}"


class TimeTrigger(Trigger):
    """Fires when at least *interval_minutes* have elapsed since the last compaction."""

    def __init__(self, interval_minutes: int = 30) -> None:
        if interval_minutes < 1:
            raise ValueError("interval_minutes must be >= 1")
        self.interval_minutes = interval_minutes

    def should_fire(self, state: TableState) -> bool:
        if state.last_compaction_at is None:
            # Never compacted — always fire.
            return True
        elapsed = (datetime.now(timezone.utc) - state.last_compaction_at).total_seconds()
        return elapsed >= self.interval_minutes * 60

    def describe(self) -> str:
        return f"time_since_last_compaction >= {self.interval_minutes}m"


class SizeTrigger(Trigger):
    """Fires when total uncompacted data exceeds *threshold_bytes*."""

    def __init__(self, threshold_bytes: int = 1_073_741_824) -> None:  # 1 GiB
        if threshold_bytes < 1:
            raise ValueError("threshold_bytes must be >= 1")
        self.threshold_bytes = threshold_bytes

    def should_fire(self, state: TableState) -> bool:
        return state.uncompacted_bytes >= self.threshold_bytes

    def describe(self) -> str:
        mb = self.threshold_bytes / (1024 * 1024)
        return f"uncompacted_bytes >= {mb:.0f} MB"


class CompositeTrigger(Trigger):
    """Combines multiple triggers with AND or OR logic.

    Parameters
    ----------
    triggers:
        One or more :class:`Trigger` instances.
    mode:
        ``"any"`` (default) means fire when *any* child fires (OR).
        ``"all"`` means fire only when *all* children fire (AND).
    """

    def __init__(
        self,
        triggers: list[Trigger],
        mode: str = "any",
    ) -> None:
        if not triggers:
            raise ValueError("CompositeTrigger requires at least one child trigger")
        if mode not in ("any", "all"):
            raise ValueError(f"mode must be 'any' or 'all', got {mode!r}")
        self.triggers = triggers
        self.mode = mode

    def should_fire(self, state: TableState) -> bool:
        evaluator = any if self.mode == "any" else all
        return evaluator(t.should_fire(state) for t in self.triggers)

    def describe(self) -> str:
        joiner = " OR " if self.mode == "any" else " AND "
        parts = [t.describe() for t in self.triggers]
        return f"({joiner.join(parts)})"

    def evaluate(self, state: TableState) -> TriggerResult:
        """Evaluate and also log child results."""
        child_results = [t.evaluate(state) for t in self.triggers]
        fired = self.should_fire(state)
        descriptions = [
            f"{'FIRED' if r.fired else 'ok'}: {r.description}"
            for r in child_results
        ]
        return TriggerResult(
            trigger_name=type(self).__name__,
            fired=fired,
            description=f"({(' OR ' if self.mode == 'any' else ' AND ').join(descriptions)})",
            evaluated_at=datetime.now(timezone.utc),
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_triggers_from_policy(policy) -> Trigger:
    """Build a :class:`CompositeTrigger` from a :class:`TablePolicy`.

    The composite uses OR logic by default — any single condition being met
    is enough to recommend maintenance.
    """
    triggers: list[Trigger] = []

    if policy.commit_count_trigger_threshold > 0:
        triggers.append(CommitCountTrigger(policy.commit_count_trigger_threshold))
    if policy.file_count_trigger_threshold > 0:
        triggers.append(FileCountTrigger(policy.file_count_trigger_threshold))
    if policy.time_trigger_interval_minutes > 0:
        triggers.append(TimeTrigger(policy.time_trigger_interval_minutes))
    if policy.size_trigger_threshold_bytes > 0:
        triggers.append(SizeTrigger(policy.size_trigger_threshold_bytes))

    if not triggers:
        # Fallback: at least use file-count trigger with the old max_file_count
        triggers.append(FileCountTrigger(policy.max_file_count))

    if len(triggers) == 1:
        return triggers[0]
    return CompositeTrigger(triggers, mode="any")
