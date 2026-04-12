"""
Spark rewriteDataFiles comparison job for iceberg-janitor bench.

Runs the Iceberg rewriteDataFiles stored procedure on each TPC-DS
fact table in the "without" warehouse. Measures wall time per table
so we can compare against the janitor's CompactHot on the same data.

Usage (EMR Serverless):
  aws emr-serverless start-job-run \
    --application-id <app_id> \
    --execution-role-arn <role_arn> \
    --job-driver '{
      "sparkSubmit": {
        "entryPoint": "s3://<bucket>/bench-scripts/spark-compact.py",
        "sparkSubmitParameters": "--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
      }
    }'
"""
import sys
import time
from pyspark.sql import SparkSession

WAREHOUSE = "s3://iceberg-janitor-605618833247-without"
CATALOG_NAME = "janitor_compare"
TABLES = ["store_sales", "store_returns", "catalog_sales"]
NAMESPACE = "tpcds"

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
total_start = time.time()

for table in TABLES:
    fqn = f"{CATALOG_NAME}.{NAMESPACE}.{table}"
    print(f"\n{'='*60}")
    print(f"Compacting {fqn}")
    print(f"{'='*60}")

    # Pre-compact file count
    try:
        files_before = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {fqn}.files"
        ).collect()[0]["cnt"]
    except Exception as e:
        print(f"  warning: could not count files before: {e}")
        files_before = -1

    start = time.time()
    try:
        spark.sql(f"""
            CALL {CATALOG_NAME}.system.rewrite_data_files(
                table => '{NAMESPACE}.{table}',
                options => map(
                    'target-file-size-bytes', '67108864',
                    'min-file-size-bytes',    '1048576',
                    'max-file-size-bytes',    '134217728'
                )
            )
        """)
        elapsed = time.time() - start
        status = "success"
    except Exception as e:
        elapsed = time.time() - start
        status = f"error: {e}"
        print(f"  ERROR: {e}")

    # Post-compact file count
    try:
        files_after = spark.sql(
            f"SELECT COUNT(*) as cnt FROM {fqn}.files"
        ).collect()[0]["cnt"]
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
    print(f"  {r['table']}: {r['files_before']} → {r['files_after']} files in {r['elapsed_s']}s [{r['status']}]")

spark.stop()
