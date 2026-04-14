"""Tests for the health analyzer data models."""

from datetime import datetime

from iceberg_janitor.analyzer.health import FileStats, HealthReport, SnapshotStats


def test_file_stats_small_file_ratio():
    stats = FileStats(file_count=100, small_file_count=75, total_bytes=0)
    assert stats.small_file_ratio == 0.75


def test_file_stats_zero_files():
    stats = FileStats(file_count=0, small_file_count=0, total_bytes=0)
    assert stats.small_file_ratio == 0.0


def test_file_stats_total_mb():
    stats = FileStats(total_bytes=1024 * 1024 * 512, file_count=1)  # 512 MB
    assert stats.total_mb == 512.0


def test_healthy_report(healthy_report):
    assert healthy_report.is_healthy
    assert not healthy_report.needs_compaction
    assert not healthy_report.needs_snapshot_expiry


def test_unhealthy_report(unhealthy_report):
    assert not unhealthy_report.is_healthy
    assert unhealthy_report.needs_compaction
    assert unhealthy_report.needs_snapshot_expiry
