"""Tests for the policy engine."""

from iceberg_janitor.policy.engine import evaluate
from iceberg_janitor.policy.models import ActionType, TablePolicy


def test_healthy_table_gets_orphan_cleanup_only(healthy_report, default_policy):
    actions = evaluate(healthy_report, default_policy)
    action_types = [a.action_type for a in actions]
    # Healthy tables still get routine orphan cleanup
    assert ActionType.REMOVE_ORPHANS in action_types
    assert ActionType.COMPACT_FILES not in action_types
    assert ActionType.EXPIRE_SNAPSHOTS not in action_types


def test_unhealthy_table_triggers_compaction(unhealthy_report, default_policy):
    actions = evaluate(unhealthy_report, default_policy)
    action_types = [a.action_type for a in actions]
    assert ActionType.COMPACT_FILES in action_types


def test_unhealthy_table_triggers_snapshot_expiry(unhealthy_report, default_policy):
    actions = evaluate(unhealthy_report, default_policy)
    action_types = [a.action_type for a in actions]
    assert ActionType.EXPIRE_SNAPSHOTS in action_types


def test_disabled_policy_returns_no_actions(unhealthy_report):
    policy = TablePolicy(enabled=False)
    actions = evaluate(unhealthy_report, policy)
    assert actions == []


def test_actions_sorted_by_priority(unhealthy_report, default_policy):
    actions = evaluate(unhealthy_report, default_policy)
    priorities = [a.priority for a in actions]
    assert priorities == sorted(priorities, reverse=True)


def test_compaction_reason_includes_ratio(unhealthy_report, default_policy):
    actions = evaluate(unhealthy_report, default_policy)
    compaction = next(a for a in actions if a.action_type == ActionType.COMPACT_FILES)
    assert "80.0%" in compaction.reason
    assert "400" in compaction.reason
