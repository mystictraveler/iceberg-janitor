"""DuckDB SQL queries for inspecting Iceberg table metadata health."""

# Count total data files and their size distribution
FILE_STATS = """
SELECT
    count(*) as file_count,
    coalesce(sum(file_size_in_bytes), 0) as total_bytes,
    coalesce(avg(file_size_in_bytes), 0) as avg_file_size_bytes,
    coalesce(min(file_size_in_bytes), 0) as min_file_size_bytes,
    coalesce(max(file_size_in_bytes), 0) as max_file_size_bytes,
    coalesce(median(file_size_in_bytes), 0) as median_file_size_bytes,
    count(*) FILTER (WHERE file_size_in_bytes < {small_file_threshold}) as small_file_count
FROM iceberg_scan('{table_path}', allow_moved_paths = true)
"""

# Count snapshots from metadata
SNAPSHOT_STATS = """
SELECT
    count(*) as snapshot_count,
    min(committed_at) as oldest_snapshot_ts,
    max(committed_at) as newest_snapshot_ts
FROM iceberg_snapshots('{table_path}')
"""

# List manifest files for analysis
MANIFEST_STATS = """
SELECT
    count(*) as manifest_count,
    coalesce(sum(length), 0) as total_manifest_bytes
FROM iceberg_metadata('{table_path}', manifest_list)
"""

# File size distribution buckets for reporting
FILE_SIZE_DISTRIBUTION = """
SELECT
    CASE
        WHEN file_size_in_bytes < 1048576 THEN '< 1 MB'
        WHEN file_size_in_bytes < 10485760 THEN '1-10 MB'
        WHEN file_size_in_bytes < 67108864 THEN '10-64 MB'
        WHEN file_size_in_bytes < 134217728 THEN '64-128 MB'
        WHEN file_size_in_bytes < 268435456 THEN '128-256 MB'
        ELSE '> 256 MB'
    END as size_bucket,
    count(*) as file_count,
    coalesce(sum(file_size_in_bytes), 0) as total_bytes
FROM iceberg_scan('{table_path}', allow_moved_paths = true)
GROUP BY size_bucket
ORDER BY min(file_size_in_bytes)
"""

# Partition-level file counts to find hot partitions
PARTITION_FILE_COUNTS = """
SELECT
    partition,
    count(*) as file_count,
    coalesce(sum(file_size_in_bytes), 0) as total_bytes,
    coalesce(avg(file_size_in_bytes), 0) as avg_file_size_bytes
FROM iceberg_scan('{table_path}', allow_moved_paths = true)
GROUP BY partition
HAVING count(*) > {min_files_per_partition}
ORDER BY file_count DESC
LIMIT {limit}
"""
