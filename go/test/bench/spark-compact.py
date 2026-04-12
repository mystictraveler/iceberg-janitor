"""
Spark rewriteDataFiles comparison job for iceberg-janitor bench.

Runs the Iceberg rewriteDataFiles stored procedure on each TPC-DS
fact table in the "without" warehouse. Measures wall time per table
so we can compare against the janitor's CompactHot on the same data.
"""
import sys
import time
import traceback
from pyspark.sql import SparkSession

WAREHOUSE = "s3://iceberg-janitor-605618833247-without"
CATALOG_NAME = "janitor_compare"
TABLES = ["store_sales", "store_returns", "catalog_sales"]
# iceberg-go's directory catalog uses <ns>.db/ as the prefix
NAMESPACE = "tpcds.db"

spark = (
    SparkSession.builder
    .appName("iceberg-janitor-spark-comparison")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", WAREHOUSE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .getOrCreate()
)

results = []
errors = []
total_start = time.time()

for table in TABLES:
    # The hadoop catalog resolves namespace "tpcds.db" as a directory
    # prefix, so the full table path is tpcds.db/<table>/
    fqn = f"{CATALOG_NAME}.`{NAMESPACE}`.{table}"
    print(f"\n{'='*60}")
    print(f"Compacting {fqn}")
    print(f"{'='*60}")

    # Pre-compact: list files via metadata table
    try:
        files_before = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {fqn}.files"
        ).collect()[0]["cnt"]
        print(f"  files before: {files_before}")
    except Exception as e:
        print(f"  warning: could not count files before: {e}")
        traceback.print_exc()
        files_before = -1

    start = time.time()
    try:
        result_df = spark.sql(f"""
            CALL {CATALOG_NAME}.system.rewrite_data_files(
                table => '`{NAMESPACE}`.{table}',
                options => map(
                    'target-file-size-bytes', '67108864',
                    'min-file-size-bytes',    '1048576',
                    'max-file-size-bytes',    '134217728'
                )
            )
        """)
        # rewriteDataFiles returns a result row with rewritten file counts
        result_df.show()
        elapsed = time.time() - start
        status = "success"
    except Exception as e:
        elapsed = time.time() - start
        status = f"error: {e}"
        print(f"  ERROR: {e}")
        traceback.print_exc()
        errors.append(f"{table}: {e}")

    # Post-compact file count
    try:
        files_after = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {fqn}.files"
        ).collect()[0]["cnt"]
        print(f"  files after: {files_after}")
    except Exception:
        files_after = -1

    result = {
        "table": table,
        "files_before": files_before,
        "files_after": files_after,
        "elapsed_s": round(elapsed, 1),
        "status": status,
    }
    results.append(result)
    print(f"  {result}")

total_elapsed = time.time() - total_start

print(f"\n{'='*60}")
print(f"SPARK COMPACTION SUMMARY")
print(f"{'='*60}")
print(f"Total wall time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
for r in results:
    print(f"  {r['table']}: {r['files_before']} -> {r['files_after']} files in {r['elapsed_s']}s [{r['status']}]")

if errors:
    print(f"\nFAILED tables: {len(errors)}")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

spark.stop()
