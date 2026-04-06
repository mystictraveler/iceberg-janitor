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


# Access tracking metrics
table_heat_score = Gauge(
    "janitor_table_heat_score",
    "Access-frequency heat score (0.0 = cold, 1.0 = hot)",
    ["table_id"],
)

table_access_classification = Gauge(
    "janitor_table_access_classification",
    "Numeric access classification (0 = cold, 1 = warm, 2 = hot)",
    ["table_id"],
)

table_queries_per_hour = Gauge(
    "janitor_table_queries_per_hour",
    "Decayed query rate over the last hour",
    ["table_id"],
)

access_events_total = Counter(
    "janitor_access_events_total",
    "Total number of access events recorded",
    ["table_id", "source"],
)

compaction_effectiveness = Gauge(
    "janitor_compaction_effectiveness",
    "Most recent compaction effectiveness score (-1.0 to 1.0)",
    ["table_id"],
)

adaptive_policy_multiplier = Gauge(
    "janitor_adaptive_policy_multiplier",
    "Effective threshold multiplier applied by the adaptive policy engine",
    ["table_id"],
)


def update_health_metrics(table_id: str, report) -> None:
    """Update Prometheus gauges from a HealthReport."""
    table_file_count.labels(table_id=table_id).set(report.file_stats.file_count)
    table_small_file_count.labels(table_id=table_id).set(report.file_stats.small_file_count)
    table_small_file_ratio.labels(table_id=table_id).set(report.file_stats.small_file_ratio)
    table_snapshot_count.labels(table_id=table_id).set(report.snapshot_stats.snapshot_count)
    table_total_bytes.labels(table_id=table_id).set(report.file_stats.total_bytes)
    table_avg_file_size_bytes.labels(table_id=table_id).set(report.file_stats.avg_file_size_bytes)


_CLASSIFICATION_MAP = {"cold": 0, "warm": 1, "hot": 2}


def update_access_metrics(table_id: str, heat_score: float, classification: str, qph: float) -> None:
    """Update Prometheus gauges from access tracker data."""
    table_heat_score.labels(table_id=table_id).set(heat_score)
    table_access_classification.labels(table_id=table_id).set(
        _CLASSIFICATION_MAP.get(classification, 0)
    )
    table_queries_per_hour.labels(table_id=table_id).set(qph)
