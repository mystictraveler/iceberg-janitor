"""Sensible default policies for different streaming workload profiles."""

from iceberg_janitor.policy.models import TablePolicy

# Conservative defaults for most streaming tables
STANDARD = TablePolicy()

# Aggressive maintenance for high-throughput streaming tables
HIGH_THROUGHPUT = TablePolicy(
    max_small_file_ratio=0.2,
    small_file_threshold_bytes=16 * 1024 * 1024,
    target_file_size_bytes=256 * 1024 * 1024,
    max_file_count=5000,
    max_snapshots=50,
    snapshot_retention_hours=24,
    min_snapshots_to_keep=3,
    orphan_retention_hours=24,
    max_manifest_count=200,
)

# Relaxed maintenance for low-volume tables
LOW_VOLUME = TablePolicy(
    max_small_file_ratio=0.5,
    max_file_count=50000,
    max_snapshots=500,
    snapshot_retention_hours=720,  # 30 days
    min_snapshots_to_keep=10,
    orphan_retention_hours=168,  # 7 days
    max_manifest_count=1000,
)

PROFILES = {
    "standard": STANDARD,
    "high-throughput": HIGH_THROUGHPUT,
    "low-volume": LOW_VOLUME,
}
