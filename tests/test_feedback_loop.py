"""Tests for the adaptive feedback loop mechanism.

Validates the self-correcting cycle:
1. AccessTracker classifies tables as hot/warm/cold
2. AdaptivePolicyEngine adjusts thresholds per classification
3. FeedbackLoop measures compaction effectiveness
4. Priority adjustments flow back to the AccessTracker

Run:
    pytest tests/test_feedback_loop.py -v -s
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest

from iceberg_janitor.analyzer.health import FileStats, HealthReport, SnapshotStats
from iceberg_janitor.policy.models import TablePolicy, PolicyConfig, AdaptivePolicyConfig
from iceberg_janitor.strategy.access_tracker import AccessEvent, AccessTracker
from iceberg_janitor.strategy.adaptive_policy import AdaptivePolicyEngine
from iceberg_janitor.strategy.feedback_loop import CompactionMetrics, FeedbackLoop


# ─── Fixtures ───────────────────────────────────────────────────────

@pytest.fixture
def tracker() -> AccessTracker:
    return AccessTracker(
        hot_threshold_qph=50,
        warm_threshold_qph=10,
        persistence_path=None,
        decay_half_life_seconds=3600,
    )


@pytest.fixture
def feedback(tracker) -> FeedbackLoop:
    return FeedbackLoop(access_tracker=tracker)


@pytest.fixture
def policy_config() -> PolicyConfig:
    return PolicyConfig(
        default_policy=TablePolicy(
            max_small_file_ratio=0.3,
            small_file_threshold_bytes=8 * 1024 * 1024,
            target_file_size_bytes=128 * 1024 * 1024,
            max_snapshots=100,
        ),
        adaptive=AdaptivePolicyConfig(
            adaptive_policy_enabled=True,
            hot_threshold_queries_per_hour=50,
            warm_threshold_queries_per_hour=10,
            hot_multiplier=0.5,
            cold_multiplier=3.0,
            feedback_loop_enabled=True,
        ),
    )


@pytest.fixture
def adaptive_engine(tracker, feedback, policy_config) -> AdaptivePolicyEngine:
    return AdaptivePolicyEngine(
        policy_config=policy_config,
        access_tracker=tracker,
        feedback_loop=feedback,
    )


def _make_events(table_id: str, count: int, latency_ms: float = 50.0) -> list[AccessEvent]:
    """Create synthetic access events spread over the last few seconds."""
    now = time.time()
    return [
        AccessEvent(
            table_id=table_id,
            timestamp=now - (count - i) * 0.1,
            query_latency_ms=latency_ms,
            client_id=f"client_{i % 5}",
            source="api",
        )
        for i in range(count)
    ]


# ─── Access Tracker Tests ───────────────────────────────────────────

class TestAccessTracker:

    def test_cold_by_default(self, tracker):
        """Tables with no access events are classified as cold."""
        assert tracker.get_classification("unknown.table") == "cold"
        assert tracker.get_heat_score("unknown.table") == 0.0

    def test_classify_hot(self, tracker):
        """Tables with high query rate are classified as hot."""
        events = _make_events("hot.table", 200)
        tracker.record_events(events)
        classification = tracker.get_classification("hot.table")
        assert classification == "hot"

    def test_classify_warm(self, tracker):
        """Tables with moderate query rate are classified as warm."""
        events = _make_events("warm.table", 30)
        tracker.record_events(events)
        classification = tracker.get_classification("warm.table")
        assert classification == "warm"

    def test_classify_cold_few_events(self, tracker):
        """Tables with very few events are cold."""
        events = _make_events("cold.table", 2)
        tracker.record_events(events)
        classification = tracker.get_classification("cold.table")
        assert classification == "cold"

    def test_heat_score_increases_with_frequency(self, tracker):
        """More queries → higher heat score."""
        tracker.record_events(_make_events("low.table", 5))
        tracker.record_events(_make_events("high.table", 100))

        low_score = tracker.get_heat_score("low.table")
        high_score = tracker.get_heat_score("high.table")
        assert high_score > low_score

    def test_latency_boosts_heat_score(self, tracker):
        """High-latency tables get a heat score boost (need compaction more)."""
        tracker.record_events(_make_events("fast.table", 50, latency_ms=10.0))
        tracker.record_events(_make_events("slow.table", 50, latency_ms=3000.0))

        fast_score = tracker.get_heat_score("fast.table")
        slow_score = tracker.get_heat_score("slow.table")
        assert slow_score > fast_score, "Slow table should have higher heat score"

    def test_rankings_ordered_by_heat(self, tracker):
        """Rankings should be sorted by heat score descending."""
        tracker.record_events(_make_events("hot.table", 200))
        tracker.record_events(_make_events("warm.table", 30))
        tracker.record_events(_make_events("cold.table", 2))

        rankings = tracker.get_all_rankings()
        scores = [r["heat_score"] for r in rankings]
        assert scores == sorted(scores, reverse=True)

    def test_priority_adjustment_modifies_heat(self, tracker):
        """Feedback loop priority adjustments should affect heat score."""
        events = _make_events("adjusted.table", 50)
        tracker.record_events(events)

        score_before = tracker.get_heat_score("adjusted.table")
        tracker.set_priority_adjustment("adjusted.table", 2.0)
        score_after = tracker.get_heat_score("adjusted.table")

        assert score_after > score_before, "Priority boost should increase heat score"

    def test_unique_clients_tracked(self, tracker):
        """Unique client IDs are tracked per table."""
        events = _make_events("multi.table", 20)
        tracker.record_events(events)

        stats = tracker.get_access_stats("multi.table")
        assert stats["unique_clients"] == 5  # client_0 through client_4


# ─── Adaptive Policy Tests ──────────────────────────────────────────

class TestAdaptivePolicy:

    def test_hot_table_gets_tighter_thresholds(self, tracker, adaptive_engine):
        """Hot tables should get multiplied-down thresholds (compact sooner)."""
        tracker.record_events(_make_events("hot.table", 200))

        policy = adaptive_engine.get_effective_policy("hot.table")
        default = TablePolicy()

        assert policy.max_small_file_ratio < default.max_small_file_ratio
        assert policy.max_snapshots < default.max_snapshots

    def test_cold_table_gets_relaxed_thresholds(self, tracker, adaptive_engine):
        """Cold tables should get multiplied-up thresholds (compact less)."""
        # Don't add events → cold
        policy = adaptive_engine.get_effective_policy("cold.table")
        default = TablePolicy()

        assert policy.max_small_file_ratio > default.max_small_file_ratio
        assert policy.max_snapshots > default.max_snapshots

    def test_warm_table_gets_default_thresholds(self, tracker, adaptive_engine):
        """Warm tables should get approximately default thresholds."""
        tracker.record_events(_make_events("warm.table", 30))

        policy = adaptive_engine.get_effective_policy("warm.table")
        default = TablePolicy()

        # Warm multiplier is 1.0 (no change)
        assert policy.max_small_file_ratio == default.max_small_file_ratio

    def test_hot_table_gets_priority_boost(self, tracker, adaptive_engine):
        """Hot tables should get a higher scheduling priority."""
        tracker.record_events(_make_events("hot.table", 200))
        tracker.record_events(_make_events("cold.table", 2))

        hot_boost = adaptive_engine.get_priority_boost("hot.table")
        cold_boost = adaptive_engine.get_priority_boost("cold.table")

        assert hot_boost > cold_boost


# ─── Feedback Loop Tests ────────────────────────────────────────────

class TestFeedbackLoop:

    def test_effective_compaction_boosts_priority(self, feedback, tracker):
        """When compaction improves metrics, priority should increase."""
        table_id = "improved.table"
        tracker.record_events(_make_events(table_id, 50))

        # Record pre-compaction: many small files, high latency
        pre = CompactionMetrics(
            file_count=500,
            small_file_count=400,
            avg_query_latency_ms=200.0,
            snapshot_count=100,
            avg_file_size_bytes=2 * 1024 * 1024,
        )
        feedback.record_pre_compaction(table_id, pre)

        # Record post-compaction: fewer files, lower latency
        post = CompactionMetrics(
            file_count=10,
            small_file_count=0,
            avg_query_latency_ms=50.0,
            snapshot_count=100,
            avg_file_size_bytes=100 * 1024 * 1024,
        )
        feedback.record_post_compaction(table_id, post)

        # Effectiveness should be positive
        history = feedback.get_history(table_id)
        assert len(history) == 1
        assert history[0].effectiveness > 0.0

        # Priority adjustment should have been boosted
        adj = tracker.get_priority_adjustment(table_id)
        assert adj >= 1.0, "Effective compaction should boost priority"

    def test_ineffective_compaction_reduces_priority(self, feedback, tracker):
        """When compaction doesn't help, priority should decrease."""
        table_id = "no_improvement.table"
        tracker.record_events(_make_events(table_id, 50))

        # Pre: already compacted (few files)
        pre = CompactionMetrics(
            file_count=10,
            small_file_count=2,
            avg_query_latency_ms=50.0,
            snapshot_count=10,
            avg_file_size_bytes=100 * 1024 * 1024,
        )
        feedback.record_pre_compaction(table_id, pre)

        # Post: no change (wasted compaction)
        post = CompactionMetrics(
            file_count=10,
            small_file_count=2,
            avg_query_latency_ms=55.0,  # slightly worse
            snapshot_count=10,
            avg_file_size_bytes=100 * 1024 * 1024,
        )
        feedback.record_post_compaction(table_id, post)

        history = feedback.get_history(table_id)
        assert len(history) == 1
        # Effectiveness should be near zero or negative
        assert history[0].effectiveness <= 0.1

    def test_effectiveness_score_range(self, feedback, tracker):
        """Effectiveness scores should be bounded between -1.0 and 1.0."""
        table_id = "bounded.table"
        tracker.record_events(_make_events(table_id, 10))

        # Extreme improvement
        pre = CompactionMetrics(
            file_count=10000, small_file_count=9999,
            avg_query_latency_ms=5000.0, snapshot_count=500,
            avg_file_size_bytes=100_000,
        )
        feedback.record_pre_compaction(table_id, pre)
        post = CompactionMetrics(
            file_count=1, small_file_count=0,
            avg_query_latency_ms=1.0, snapshot_count=500,
            avg_file_size_bytes=1_000_000_000,
        )
        feedback.record_post_compaction(table_id, post)

        history = feedback.get_history(table_id)
        assert -1.0 <= history[0].effectiveness <= 1.0

    def test_multiple_compaction_cycles_tracked(self, feedback, tracker):
        """Multiple compaction cycles should all be recorded."""
        table_id = "multi_cycle.table"
        tracker.record_events(_make_events(table_id, 50))

        for cycle in range(5):
            pre = CompactionMetrics(
                file_count=100 + cycle * 20,
                small_file_count=80 + cycle * 15,
                avg_query_latency_ms=100 + cycle * 10,
                snapshot_count=50,
                avg_file_size_bytes=5 * 1024 * 1024,
            )
            feedback.record_pre_compaction(table_id, pre)
            post = CompactionMetrics(
                file_count=5,
                small_file_count=0,
                avg_query_latency_ms=30.0,
                snapshot_count=50,
                avg_file_size_bytes=100 * 1024 * 1024,
            )
            feedback.record_post_compaction(table_id, post)

        history = feedback.get_history(table_id)
        assert len(history) == 5


# ─── End-to-End Feedback Loop Test ──────────────────────────────────

class TestFeedbackLoopEndToEnd:
    """Test the complete feedback cycle: track → adapt → compact → measure → adjust."""

    def test_full_feedback_cycle(self, tracker, feedback, adaptive_engine):
        """
        Simulate the complete self-correcting loop:
        1. Record access events (table becomes hot)
        2. Adaptive policy tightens thresholds
        3. Compaction runs and improves metrics
        4. Feedback loop boosts priority
        5. Next cycle: table gets even higher priority
        """
        table_id = "production.events"

        # ── Step 1: Table becomes hot ──
        tracker.record_events(_make_events(table_id, 200, latency_ms=300.0))
        assert tracker.get_classification(table_id) == "hot"

        # ── Step 2: Adaptive policy tightens ──
        policy = adaptive_engine.get_effective_policy(table_id)
        default = TablePolicy()
        assert policy.max_small_file_ratio < default.max_small_file_ratio, \
            "Hot table should have tighter compaction threshold"

        priority_before = adaptive_engine.get_priority_boost(table_id)

        # ── Step 3: Compaction runs (simulated) ──
        pre = CompactionMetrics(
            file_count=500, small_file_count=450,
            avg_query_latency_ms=300.0, snapshot_count=200,
            avg_file_size_bytes=2 * 1024 * 1024,
        )
        feedback.record_pre_compaction(table_id, pre)

        post = CompactionMetrics(
            file_count=5, small_file_count=0,
            avg_query_latency_ms=50.0, snapshot_count=200,
            avg_file_size_bytes=200 * 1024 * 1024,
        )
        feedback.record_post_compaction(table_id, post)

        # ── Step 4: Feedback loop boosted priority ──
        priority_after = adaptive_engine.get_priority_boost(table_id)
        assert priority_after >= priority_before, \
            "Effective compaction should maintain or boost priority"

        # ── Step 5: Effectiveness was positive ──
        history = feedback.get_history(table_id)
        assert len(history) == 1
        assert history[0].effectiveness > 0.5, \
            f"Major improvement should yield high effectiveness, got {history[0].effectiveness}"

    def test_cold_table_low_priority(self, tracker, feedback, adaptive_engine):
        """Cold tables should get low priority and relaxed thresholds."""
        table_id = "archive.old_data"

        # No events → cold
        assert tracker.get_classification(table_id) == "cold"

        policy = adaptive_engine.get_effective_policy(table_id)
        default = TablePolicy()

        assert policy.max_small_file_ratio > default.max_small_file_ratio
        assert adaptive_engine.get_priority_boost(table_id) < 10, \
            "Cold table should have very low priority boost"
