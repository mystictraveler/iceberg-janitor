"""Prometheus metrics for Iceberg table maintenance."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Table health gauges
table_file_count = Gauge(
    "janitor_table_file_count",
    "Number of data files in the table",
    ["table_id"],
)

table_small_file_count = Gauge(
    "janitor_table_small_file_count",
    "Number of small data files in the table",
    ["table_id"],
)

table_small_file_ratio = Gauge(
    "janitor_table_small_file_ratio",
    "Ratio of small files to total files",
    ["table_id"],
)

table_snapshot_count = Gauge(
    "janitor_table_snapshot_count",
    "Number of snapshots in the table",
    ["table_id"],
)

table_total_bytes = Gauge(
    "janitor_table_total_bytes",
    "Total size of data files in bytes",
    ["table_id"],
)

table_avg_file_size_bytes = Gauge(
    "janitor_table_avg_file_size_bytes",
    "Average data file size in bytes",
    ["table_id"],
)

# Maintenance operation counters
compaction_runs = Counter(
    "janitor_compaction_runs_total",
    "Total number of compaction operations",
    ["table_id", "status"],
)

snapshot_expirations = Counter(
    "janitor_snapshot_expirations_total",
    "Total number of snapshot expiration operations",
    ["table_id", "status"],
)

orphan_removals = Counter(
    "janitor_orphan_removals_total",
    "Total number of orphan file removal operations",
    ["table_id", "status"],
)

snapshots_expired_count = Counter(
    "janitor_snapshots_expired_count",
    "Total number of individual snapshots expired",
    ["table_id"],
)

orphans_removed_count = Counter(
    "janitor_orphans_removed_count",
    "Total number of orphan files removed",
    ["table_id"],
)

# Maintenance duration
maintenance_duration_seconds = Histogram(
    "janitor_maintenance_duration_seconds",
    "Duration of maintenance operations",
    ["table_id", "action_type"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800],
)


def update_health_metrics(table_id: str, report) -> None:
    """Update Prometheus gauges from a HealthReport."""
    table_file_count.labels(table_id=table_id).set(report.file_stats.file_count)
    table_small_file_count.labels(table_id=table_id).set(report.file_stats.small_file_count)
    table_small_file_ratio.labels(table_id=table_id).set(report.file_stats.small_file_ratio)
    table_snapshot_count.labels(table_id=table_id).set(report.snapshot_stats.snapshot_count)
    table_total_bytes.labels(table_id=table_id).set(report.file_stats.total_bytes)
    table_avg_file_size_bytes.labels(table_id=table_id).set(report.file_stats.avg_file_size_bytes)
