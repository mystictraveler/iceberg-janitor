"""Test fixtures for iceberg-janitor."""

import pytest

from iceberg_janitor.analyzer.health import FileStats, HealthReport, SnapshotStats
from iceberg_janitor.policy.models import TablePolicy
from datetime import datetime


@pytest.fixture
def healthy_report() -> HealthReport:
    """A health report for a table that needs no maintenance."""
    return HealthReport(
        table_id="test_ns.healthy_table",
        assessed_at=datetime.now(),
        file_stats=FileStats(
            file_count=50,
            total_bytes=5 * 1024 * 1024 * 1024,  # 5 GB
            avg_file_size_bytes=100 * 1024 * 1024,  # 100 MB
            min_file_size_bytes=50 * 1024 * 1024,
            max_file_size_bytes=150 * 1024 * 1024,
            median_file_size_bytes=100 * 1024 * 1024,
            small_file_count=2,
        ),
        snapshot_stats=SnapshotStats(snapshot_count=10),
    )


@pytest.fixture
def unhealthy_report() -> HealthReport:
    """A health report for a table with many small files and too many snapshots."""
    return HealthReport(
        table_id="test_ns.messy_table",
        assessed_at=datetime.now(),
        file_stats=FileStats(
            file_count=500,
            total_bytes=2 * 1024 * 1024 * 1024,  # 2 GB
            avg_file_size_bytes=4 * 1024 * 1024,  # 4 MB avg
            min_file_size_bytes=100 * 1024,  # 100 KB
            max_file_size_bytes=20 * 1024 * 1024,
            median_file_size_bytes=3 * 1024 * 1024,
            small_file_count=400,  # 80% small files
        ),
        snapshot_stats=SnapshotStats(snapshot_count=250),
    )


@pytest.fixture
def default_policy() -> TablePolicy:
    return TablePolicy()
