package com.icebergjanitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Flink batch job that performs Iceberg table compaction.
 *
 * <p>Receives execution context as a JSON program argument containing catalog
 * connection details, table identifier, compaction strategy, and target file
 * sizes.  The job connects to the Iceberg catalog via Flink SQL, then
 * executes a {@code CALL} to the Iceberg stored procedure for rewriting
 * data files.
 *
 * <h3>Supported strategies</h3>
 * <ul>
 *   <li><b>binpack</b> — merge small files without re-sorting</li>
 *   <li><b>sort</b>   — rewrite files sorted by specified columns</li>
 *   <li><b>zorder</b> — rewrite files using Z-order on specified columns</li>
 * </ul>
 *
 * <h3>Program arguments</h3>
 * A single JSON string with the following fields:
 * <pre>{@code
 * {
 *   "jobId": "uuid",
 *   "tableId": "namespace.table",
 *   "catalogUri": "http://rest-catalog:8181",
 *   "warehouse": "s3://warehouse/",
 *   "catalogType": "rest",
 *   "catalogProperties": {"s3.endpoint": "http://minio:9000", ...},
 *   "strategy": "binpack",
 *   "targetFileSizeBytes": 134217728,
 *   "minFileSizeBytes": 8388608,
 *   "minInputFiles": 5,
 *   "partitionFilter": null,
 *   "sortColumns": [],
 *   "partialProgress": true,
 *   "dryRun": false
 * }
 * }</pre>
 */
public class FlinkCompactionJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "Expected a single JSON argument with compaction configuration");
        }

        // Parse execution context
        JsonNode config = MAPPER.readTree(args[0]);

        String jobId = config.get("jobId").asText();
        String tableId = config.get("tableId").asText();
        String catalogUri = config.get("catalogUri").asText();
        String warehouse = config.get("warehouse").asText();
        String catalogType = config.get("catalogType").asText("rest");
        String strategy = config.get("strategy").asText("binpack");
        long targetFileSizeBytes = config.get("targetFileSizeBytes").asLong(134217728L);
        long minFileSizeBytes = config.get("minFileSizeBytes").asLong(8388608L);
        int minInputFiles = config.get("minInputFiles").asInt(5);
        boolean partialProgress = config.get("partialProgress").asBoolean(true);
        boolean dryRun = config.get("dryRun").asBoolean(false);

        String partitionFilter = null;
        if (config.has("partitionFilter") && !config.get("partitionFilter").isNull()) {
            partitionFilter = config.get("partitionFilter").asText();
        }

        // Collect sort columns
        StringBuilder sortColumnsBuilder = new StringBuilder();
        if (config.has("sortColumns") && config.get("sortColumns").isArray()) {
            for (int i = 0; i < config.get("sortColumns").size(); i++) {
                if (i > 0) sortColumnsBuilder.append(",");
                sortColumnsBuilder.append(config.get("sortColumns").get(i).asText());
            }
        }
        String sortColumns = sortColumnsBuilder.toString();

        // Collect catalog properties
        Map<String, String> catalogProperties = new HashMap<>();
        if (config.has("catalogProperties") && config.get("catalogProperties").isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields =
                    config.get("catalogProperties").fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                catalogProperties.put(entry.getKey(), entry.getValue().asText());
            }
        }

        System.out.printf("FlinkCompactionJob starting: jobId=%s table=%s strategy=%s%n",
                jobId, tableId, strategy);

        if (dryRun) {
            System.out.printf("DRY RUN mode — no files will be rewritten for table %s%n", tableId);
            return;
        }

        // Set up Flink environment in BATCH mode
        StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        execEnv.setRuntimeMode(RuntimeExecutionMode.BATCH);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                execEnv,
                EnvironmentSettings.newInstance().inBatchMode().build()
        );

        // Build the CREATE CATALOG statement
        StringBuilder catalogSql = new StringBuilder();
        catalogSql.append("CREATE CATALOG iceberg_compaction WITH (");
        catalogSql.append("'type' = 'iceberg',");
        catalogSql.append(String.format("'catalog-type' = '%s',", catalogType));
        catalogSql.append(String.format("'uri' = '%s',", catalogUri));
        catalogSql.append(String.format("'warehouse' = '%s'", warehouse));

        // Append extra catalog properties (S3 endpoint, credentials, etc.)
        for (Map.Entry<String, String> entry : catalogProperties.entrySet()) {
            catalogSql.append(String.format(",'%s' = '%s'", entry.getKey(), entry.getValue()));
        }
        catalogSql.append(")");

        tableEnv.executeSql(catalogSql.toString());
        tableEnv.executeSql("USE CATALOG iceberg_compaction");

        // Parse namespace and table name from tableId (e.g. "ns.table" or "db.schema.table")
        String[] parts = tableId.split("\\.");
        if (parts.length < 2) {
            throw new IllegalArgumentException(
                    "tableId must be in the form 'namespace.table', got: " + tableId);
        }
        String namespace = parts[0];
        String tableName = parts[parts.length - 1];

        tableEnv.executeSql(String.format("USE %s", namespace));

        // Build the rewrite_data_files CALL statement
        // Iceberg Flink stored procedure: system.rewrite_data_files
        StringBuilder callSql = new StringBuilder();
        callSql.append(String.format(
                "CALL iceberg_compaction.system.rewrite_data_files(" +
                "'%s'," +       // table
                "'%s'",         // strategy
                tableName, strategy));

        // Build options map
        StringBuilder options = new StringBuilder();
        options.append(String.format("'target-file-size-bytes'='%d'", targetFileSizeBytes));
        options.append(String.format(",'min-file-size-bytes'='%d'", minFileSizeBytes));
        options.append(String.format(",'min-input-files'='%d'", minInputFiles));
        options.append(String.format(",'partial-progress.enabled'='%s'", partialProgress));

        // Sort order for sort/zorder strategies
        if (("sort".equals(strategy) || "zorder".equals(strategy))
                && !sortColumns.isEmpty()) {
            options.append(String.format(",'sort-order'='%s'", sortColumns));
        }

        callSql.append(String.format(",map(%s)", options));

        // Partition filter (WHERE clause)
        if (partitionFilter != null && !partitionFilter.isEmpty()) {
            callSql.append(String.format(",'%s'", partitionFilter));
        }

        callSql.append(")");

        System.out.printf("Executing compaction: %s%n", callSql);
        tableEnv.executeSql(callSql.toString());

        System.out.printf("FlinkCompactionJob completed: jobId=%s table=%s%n", jobId, tableId);
    }
}
