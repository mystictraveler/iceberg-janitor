package com.icebergjanitor;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink job that consumes JSON events from Kafka and writes them to an Iceberg table.
 *
 * This creates the streaming write pattern that generates the small file / metadata
 * bloat problem that iceberg-janitor is designed to solve.
 *
 * Environment variables:
 *   KAFKA_BOOTSTRAP: Kafka bootstrap servers (default: kafka:9092)
 *   KAFKA_TOPIC: Source topic (default: events)
 *   CATALOG_URI: Iceberg REST catalog URI (default: http://rest-catalog:8181)
 *   WAREHOUSE: S3 warehouse path (default: s3://warehouse/)
 *   S3_ENDPOINT: S3/MinIO endpoint (default: http://minio:9000)
 *   CHECKPOINT_INTERVAL_MS: Flink checkpoint interval (default: 10000 = 10s)
 */
public class FlinkIcebergSink {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String kafkaTopic = env("KAFKA_TOPIC", "events");
        String catalogUri = env("CATALOG_URI", "http://rest-catalog:8181");
        String warehouse = env("WAREHOUSE", "s3://warehouse/");
        String s3Endpoint = env("S3_ENDPOINT", "http://minio:9000");
        long checkpointInterval = Long.parseLong(env("CHECKPOINT_INTERVAL_MS", "10000"));

        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.enableCheckpointing(checkpointInterval);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
            execEnv,
            EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // Register Iceberg catalog
        tableEnv.executeSql(String.format(
            "CREATE CATALOG iceberg WITH (" +
            "  'type' = 'iceberg'," +
            "  'catalog-type' = 'rest'," +
            "  'uri' = '%s'," +
            "  'warehouse' = '%s'," +
            "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
            "  's3.endpoint' = '%s'," +
            "  's3.path-style-access' = 'true'" +
            ")",
            catalogUri, warehouse, s3Endpoint
        ));

        tableEnv.executeSql("USE CATALOG iceberg");

        // Create namespace if not exists
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS events_db");
        tableEnv.executeSql("USE events_db");

        // Create the Iceberg sink table
        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS events (" +
            "  event_id STRING," +
            "  event_type STRING," +
            "  user_id STRING," +
            "  payload STRING," +
            "  event_time TIMESTAMP(3)," +
            "  process_time AS PROCTIME()" +
            ") WITH (" +
            "  'format-version' = '2'," +
            "  'write.parquet.compression-codec' = 'snappy'" +
            ")"
        );

        // Create Kafka source table
        tableEnv.executeSql(String.format(
            "CREATE TEMPORARY TABLE kafka_events (" +
            "  event_id STRING," +
            "  event_type STRING," +
            "  user_id STRING," +
            "  payload STRING," +
            "  event_time TIMESTAMP(3)," +
            "  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND" +
            ") WITH (" +
            "  'connector' = 'kafka'," +
            "  'topic' = '%s'," +
            "  'properties.bootstrap.servers' = '%s'," +
            "  'properties.group.id' = 'flink-iceberg-sink'," +
            "  'scan.startup.mode' = 'earliest-offset'," +
            "  'format' = 'json'," +
            "  'json.timestamp-format.standard' = 'ISO-8601'" +
            ")",
            kafkaTopic, kafkaBootstrap
        ));

        // Stream from Kafka to Iceberg
        tableEnv.executeSql(
            "INSERT INTO events " +
            "SELECT event_id, event_type, user_id, payload, event_time " +
            "FROM kafka_events"
        );
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
