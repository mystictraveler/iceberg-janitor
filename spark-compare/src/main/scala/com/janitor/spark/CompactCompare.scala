package com.janitor.spark

import org.apache.spark.sql.SparkSession
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.iceberg.spark.Spark3Util
// RewriteDataFiles options are plain strings in Iceberg 1.7

/**
 * Spark Iceberg rewriteDataFiles comparison job.
 *
 * Compacts all three TPC-DS fact tables in the "without" warehouse
 * using Spark's native Iceberg Actions API (not SQL CALL). Measures
 * wall time per table for head-to-head comparison against the
 * janitor's CompactHot.
 *
 * Run on EMR Serverless:
 *   aws emr-serverless start-job-run \
 *     --application-id <app> \
 *     --execution-role-arn <role> \
 *     --job-driver '{
 *       "sparkSubmit": {
 *         "entryPoint": "s3://<bucket>/bench-scripts/spark-compare-assembly.jar",
 *         "mainClass": "com.janitor.spark.CompactCompare",
 *         "sparkSubmitParameters": "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.janitor_compare=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.janitor_compare.type=hadoop --conf spark.sql.catalog.janitor_compare.warehouse=s3://iceberg-janitor-605618833247-without"
 *       }
 *     }'
 */
object CompactCompare {

  val CatalogName = "janitor_compare"
  // iceberg-go uses <ns>.db/ as the directory prefix
  val Namespace = "tpcds.db"
  val Tables = Seq("store_sales", "store_returns", "catalog_sales")
  val TargetFileSizeBytes = 67108864L // 64 MB

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("iceberg-janitor-spark-comparison")
      .getOrCreate()

    val totalStart = System.currentTimeMillis()
    var failures = 0

    for (table <- Tables) {
      val fqn = s"$CatalogName.`$Namespace`.$table"
      println(s"\n${"=" * 60}")
      println(s"Compacting $fqn")
      println(s"${"=" * 60}")

      try {
        // Load the Iceberg table via Spark3Util
        val icebergTable = Spark3Util.loadIcebergTable(spark, fqn)

        // Pre-compact file count
        val filesBefore = spark.sql(s"SELECT COUNT(*) as cnt FROM $fqn.files")
          .collect()(0).getLong(0)
        println(s"  files before: $filesBefore")

        val start = System.currentTimeMillis()

        // Use the Actions API directly — no SQL CALL, no proc resolution
        val result = SparkActions.get(spark)
          .rewriteDataFiles(icebergTable)
          .option("target-file-size-bytes", TargetFileSizeBytes.toString)
          .option("max-file-size-bytes", (TargetFileSizeBytes * 2).toString)
          .option("min-file-size-bytes", (TargetFileSizeBytes / 64).toString)
          .execute()

        val elapsed = System.currentTimeMillis() - start

        // Post-compact file count
        val filesAfter = spark.sql(s"SELECT COUNT(*) as cnt FROM $fqn.files")
          .collect()(0).getLong(0)

        println(s"  files after:  $filesAfter")
        println(s"  rewritten:    ${result.rewrittenDataFilesCount()}")
        println(s"  added:        ${result.addedDataFilesCount()}")
        println(s"  elapsed:      ${elapsed}ms (${elapsed / 1000.0}s)")

      } catch {
        case e: Exception =>
          println(s"  ERROR: ${e.getMessage}")
          e.printStackTrace()
          failures += 1
      }
    }

    val totalElapsed = System.currentTimeMillis() - totalStart
    println(s"\n${"=" * 60}")
    println(s"SPARK COMPACTION SUMMARY")
    println(s"${"=" * 60}")
    println(s"Total wall time: ${totalElapsed}ms (${totalElapsed / 1000.0}s)")
    println(s"Failures: $failures/${Tables.size}")

    spark.stop()

    if (failures > 0) {
      System.exit(1)
    }
  }
}
