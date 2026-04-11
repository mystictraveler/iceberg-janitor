#!/usr/bin/env bash
# bench-tpcds-athena.sh — TPC-DS streaming benchmark using Athena for queries.
#
# Same architecture as bench-tpcds.sh but replaces DuckDB with Athena:
#   - Streamers write to S3 (same)
#   - Janitor compacts the with-janitor warehouse (same)
#   - Athena queries both warehouses via Glue-registered Iceberg tables
#
# Required env:
#   WH_WITH_URL             s3:// URL for the with-janitor warehouse bucket
#   WH_WITHOUT_URL          s3:// URL for the without-janitor warehouse bucket
#   AWS_REGION              AWS region
#   ATHENA_WORKGROUP        Athena workgroup name
#   GLUE_DB_WITH            Glue database for with-janitor tables
#   GLUE_DB_WITHOUT         Glue database for without-janitor tables
#   ATHENA_RESULTS_BUCKET   S3 bucket for Athena query results

set -euo pipefail

# === Configuration ===

WH_WITH_URL="${WH_WITH_URL:?WH_WITH_URL is required (s3://bucket)}"
WH_WITHOUT_URL="${WH_WITHOUT_URL:?WH_WITHOUT_URL is required (s3://bucket)}"
AWS_REGION="${AWS_REGION:?AWS_REGION is required}"
ATHENA_WORKGROUP="${ATHENA_WORKGROUP:?ATHENA_WORKGROUP is required}"
GLUE_DB_WITH="${GLUE_DB_WITH:?GLUE_DB_WITH is required}"
GLUE_DB_WITHOUT="${GLUE_DB_WITHOUT:?GLUE_DB_WITHOUT is required}"
ATHENA_RESULTS_BUCKET="${ATHENA_RESULTS_BUCKET:?ATHENA_RESULTS_BUCKET is required}"

DURATION_SECONDS="${DURATION_SECONDS:-300}"
QUERY_INTERVAL_SECONDS="${QUERY_INTERVAL_SECONDS:-30}"
MAINTENANCE_INTERVAL_SECONDS="${MAINTENANCE_INTERVAL_SECONDS:-60}"
COMMITS_PER_MINUTE="${COMMITS_PER_MINUTE:-30}"
STORE_SALES_PER_BATCH="${STORE_SALES_PER_BATCH:-500}"
STORE_RETURNS_PER_BATCH="${STORE_RETURNS_PER_BATCH:-100}"
CATALOG_SALES_PER_BATCH="${CATALOG_SALES_PER_BATCH:-300}"
NAMESPACE="${NAMESPACE:-tpcds}"

RESULTS_DIR="${RESULTS_DIR:-/results}"
mkdir -p "$RESULTS_DIR"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_CSV="$RESULTS_DIR/bench-results-$TS.csv"
SUMMARY_TXT="$RESULTS_DIR/bench-summary-$TS.txt"
STREAMER_WITH_LOG="$RESULTS_DIR/streamer-with-$TS.log"
STREAMER_WITHOUT_LOG="$RESULTS_DIR/streamer-without-$TS.log"
JANITOR_LOG="$RESULTS_DIR/janitor-runs-$TS.log"

log() { printf '\033[1;36m[bench]\033[0m %s\n' "$*"; }

cleanup() {
  log "shutting down streamers..."
  [[ -n "${PID_WITH:-}" ]] && kill "$PID_WITH" 2>/dev/null || true
  [[ -n "${PID_WITHOUT:-}" ]] && kill "$PID_WITHOUT" 2>/dev/null || true
  wait 2>/dev/null || true
  log "streamers stopped"
}
trap cleanup EXIT INT TERM

# === Athena helpers ===

# run_athena_query runs a SQL query via Athena and waits for completion.
# Returns the query execution ID. Prints timing to stderr.
run_athena_query() {
  local database="$1" sql="$2"
  local query_id
  query_id=$(aws athena start-query-execution \
    --query-string "$sql" \
    --work-group "$ATHENA_WORKGROUP" \
    --query-execution-context "Database=$database" \
    --result-configuration "OutputLocation=s3://$ATHENA_RESULTS_BUCKET/results/" \
    --region "$AWS_REGION" \
    --query 'QueryExecutionId' --output text)

  # Poll until complete
  local state="RUNNING"
  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 1
    state=$(aws athena get-query-execution \
      --query-execution-id "$query_id" \
      --region "$AWS_REGION" \
      --query 'QueryExecution.Status.State' --output text)
  done

  if [[ "$state" != "SUCCEEDED" ]]; then
    local reason
    reason=$(aws athena get-query-execution \
      --query-execution-id "$query_id" \
      --region "$AWS_REGION" \
      --query 'QueryExecution.Status.StateChangeReason' --output text 2>/dev/null || echo "unknown")
    echo "FAILED:$reason" >&2
    echo "$query_id"
    return 1
  fi
  echo "$query_id"
}

# get_athena_timing returns execution time in ms for a completed query.
get_athena_timing() {
  local query_id="$1"
  aws athena get-query-execution \
    --query-execution-id "$query_id" \
    --region "$AWS_REGION" \
    --query 'QueryExecution.Statistics.EngineExecutionTimeInMillis' --output text
}

# get_athena_row_count returns the row count from a SELECT count(*) query.
get_athena_row_count() {
  local query_id="$1"
  aws athena get-query-results \
    --query-execution-id "$query_id" \
    --region "$AWS_REGION" \
    --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text 2>/dev/null || echo "-1"
}

# === Phase 0: Register Iceberg tables in Glue ===

register_iceberg_tables() {
  local db="$1" warehouse_url="$2"
  log "registering Iceberg tables in Glue database $db via janitor-cli"
  # warehouse_url includes the with/ or without/ prefix (e.g. s3://bucket/with).
  # gocloud s3blob treats the path component as a key prefix, so discover
  # finds tpcds.db/store_sales directly as a 2-part path.
  JANITOR_WAREHOUSE_URL="$warehouse_url" janitor-cli glue-register --database "$db"
}

# === Phase 1: Start streamers ===

log "starting streamers (bursty=${BURSTY:-true})"
JANITOR_WAREHOUSE_URL="$WH_WITH_URL" \
  NAMESPACE="$NAMESPACE" \
  COMMITS_PER_MINUTE="$COMMITS_PER_MINUTE" \
  STORE_SALES_PER_BATCH="$STORE_SALES_PER_BATCH" \
  STORE_RETURNS_PER_BATCH="$STORE_RETURNS_PER_BATCH" \
  CATALOG_SALES_PER_BATCH="$CATALOG_SALES_PER_BATCH" \
  DURATION_SECONDS="$((DURATION_SECONDS + 30))" \
  TRUNCATE_TABLES=false \
  BURSTY="${BURSTY:-true}" \
  BURST_MAX="${BURST_MAX:-10}" \
  janitor-streamer > "$STREAMER_WITH_LOG" 2>&1 &
PID_WITH=$!

JANITOR_WAREHOUSE_URL="$WH_WITHOUT_URL" \
  NAMESPACE="$NAMESPACE" \
  COMMITS_PER_MINUTE="$COMMITS_PER_MINUTE" \
  STORE_SALES_PER_BATCH="$STORE_SALES_PER_BATCH" \
  STORE_RETURNS_PER_BATCH="$STORE_RETURNS_PER_BATCH" \
  CATALOG_SALES_PER_BATCH="$CATALOG_SALES_PER_BATCH" \
  DURATION_SECONDS="$((DURATION_SECONDS + 30))" \
  TRUNCATE_TABLES=false \
  BURSTY="${BURSTY:-true}" \
  BURST_MAX="${BURST_MAX:-10}" \
  janitor-streamer > "$STREAMER_WITHOUT_LOG" 2>&1 &
PID_WITHOUT=$!

log "PIDs: with=$PID_WITH without=$PID_WITHOUT"
log "waiting 15s for streamers to start..."
sleep 15

# Glue tables are pre-registered (janitor-cli glue-register or manual).
# Skip registration here — just query.

# === Phase 3: Query + maintenance loop ===

echo "iteration,timestamp,mode,query,latency_ms,row_count,store_sales_files,store_returns_files,catalog_sales_files" > "$RESULTS_CSV"

BENCH_QUERIES=(
  "q1:SELECT count(*) FROM (SELECT c.c_customer_id, sum(sr.sr_return_amt) FROM store_returns sr JOIN customer c ON sr.sr_customer_sk=c.c_customer_sk JOIN store s ON sr.sr_store_sk=s.s_store_sk JOIN date_dim d ON sr.sr_returned_date_sk=d.d_date_sk WHERE d.d_year=2022 GROUP BY c.c_customer_id ORDER BY 2 DESC LIMIT 100)"
  "q3:SELECT count(*) FROM (SELECT d.d_year, i.i_brand, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE i.i_category='Electronics' GROUP BY 1,2 ORDER BY 1,3 DESC LIMIT 100)"
  "q7:SELECT count(*) FROM (SELECT i.i_item_id, avg(ss.ss_quantity) FROM store_sales ss JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN promotion p ON ss.ss_promo_sk=p.p_promo_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_gender='F' AND cd.cd_marital_status='S' AND d.d_year=2023 GROUP BY 1 LIMIT 100)"
  "q13:SELECT count(*) FROM (SELECT avg(ss.ss_quantity) FROM store_sales ss JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN customer_address ca ON ss.ss_addr_sk=ca.ca_address_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_marital_status='M' AND d.d_year=2022 AND ca.ca_state IN ('CA','NY','TX'))"
  "q55:SELECT count(*) FROM (SELECT i.i_brand_id, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_moy=6 AND d.d_year=2023 AND i.i_manager_id=15 GROUP BY 1 ORDER BY 2 DESC LIMIT 100)"
  "q96:SELECT count(*) FROM store_sales ss JOIN time_dim t ON ss.ss_sold_time_sk=t.t_time_sk JOIN household_demographics hd ON ss.ss_hdemo_sk=hd.hd_demo_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk WHERE t.t_hour=14 AND hd.hd_dep_count=3"
)

run_athena_benchmarks() {
  local label="$1" database="$2" iteration="$3"
  local warehouse_url
  [[ "$label" == "with" ]] && warehouse_url="$WH_WITH_URL" || warehouse_url="$WH_WITHOUT_URL"

  # File counts via janitor-cli
  local files_ss files_sr files_cs
  set +e
  files_ss=$(JANITOR_WAREHOUSE_URL="$warehouse_url" janitor-cli analyze "${NAMESPACE}.db/store_sales" 2>/dev/null | awk '/^Data files:/{print $3}')
  files_sr=$(JANITOR_WAREHOUSE_URL="$warehouse_url" janitor-cli analyze "${NAMESPACE}.db/store_returns" 2>/dev/null | awk '/^Data files:/{print $3}')
  files_cs=$(JANITOR_WAREHOUSE_URL="$warehouse_url" janitor-cli analyze "${NAMESPACE}.db/catalog_sales" 2>/dev/null | awk '/^Data files:/{print $3}')
  set -e
  [[ -z "$files_ss" ]] && files_ss="-1"
  [[ -z "$files_sr" ]] && files_sr="-1"
  [[ -z "$files_cs" ]] && files_cs="-1"

  for entry in "${BENCH_QUERIES[@]}"; do
    local qname="${entry%%:*}"
    local qsql="${entry#*:}"

    set +e
    local qid
    qid=$(run_athena_query "$database" "$qsql")
    local rc=$?
    set -e

    local latency_ms="-1" row_count="-1"
    if [[ $rc -eq 0 ]]; then
      latency_ms=$(get_athena_timing "$qid")
      row_count=$(get_athena_row_count "$qid")
    fi

    echo "$iteration,$(date -u +%FT%TZ),$label,$qname,$latency_ms,$row_count,$files_ss,$files_sr,$files_cs" >> "$RESULTS_CSV"
    log "  $label/$qname: ${latency_ms}ms (rows=$row_count)"
  done
}

MAINTAIN_ROUND=0
run_janitor_maintain() {
  MAINTAIN_ROUND=$((MAINTAIN_ROUND + 1))
  log "janitor maintain round ${MAINTAIN_ROUND}"
  # Call the maintain endpoint: rewrite → expire → compact → rewrite.
  # Server runs co-located with S3, so round trips are ~1ms.
  # Async: POST returns 202 with job_id, then poll GET /v1/jobs/{id}.
  set +e
  local server_url="${JANITOR_SERVER_URL:-http://server.iceberg-janitor.local:8080}"
  local query="keep_last=${EXPIRE_KEEP_LAST:-5}&keep_within=${EXPIRE_KEEP_WITHIN:-1m}&target_file_size=${TARGET_FILE_SIZE:-1MB}"
  local job_ids=()
  for tbl in store_sales store_returns catalog_sales; do
    local url="${server_url}/v1/tables/${NAMESPACE}/${tbl}/maintain?${query}"
    log "  POST $url"
    local resp
    resp=$(curl -s -X POST -H 'Accept: application/json' "$url" 2>&1) || true
    local job_id
    job_id=$(echo "$resp" | grep -o '"job_id"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
    if [[ -n "$job_id" ]]; then
      log "  $tbl: maintain job $job_id submitted"
      job_ids+=("$tbl:$job_id")
    else
      log "  warning: $tbl maintain submit failed: $resp"
    fi
  done
  # Poll all jobs until done (max 5 min total).
  local deadline=$(($(date +%s) + 300))
  local pending=("${job_ids[@]}")
  while [[ ${#pending[@]} -gt 0 ]] && (( $(date +%s) < deadline )); do
    sleep 5
    local still_pending=()
    for entry in "${pending[@]}"; do
      local tbl="${entry%%:*}" jid="${entry#*:}"
      local status
      status=$(curl -s "${server_url}/v1/jobs/${jid}" 2>&1)
      local state
      state=$(echo "$status" | grep -o '"status"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"status"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
      case "$state" in
        completed)
          log "  $tbl: job $jid completed"
          echo "$status" >> "$JANITOR_LOG"
          ;;
        failed)
          local err
          err=$(echo "$status" | grep -o '"error"[[:space:]]*:[[:space:]]*"[^"]*"' | sed 's/.*"error"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')
          log "  warning: $tbl: job $jid failed: $err"
          echo "$status" >> "$JANITOR_LOG"
          ;;
        *)
          still_pending+=("$entry")
          ;;
      esac
    done
    pending=("${still_pending[@]}")
  done
  if [[ ${#pending[@]} -gt 0 ]]; then
    log "  warning: ${#pending[@]} jobs still running after timeout"
  fi
  set -e
  log "compact round ${COMPACT_ROUND} done"
}

# === Main loop ===

START=$(date +%s)
NEXT_QUERY=$START
NEXT_MAINTENANCE=$((START + MAINTENANCE_INTERVAL_SECONDS))
ITERATION=0

while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  if (( ELAPSED >= DURATION_SECONDS )); then
    break
  fi

  if (( NOW >= NEXT_QUERY )); then
    ITERATION=$((ITERATION + 1))
    log "[t+${ELAPSED}s] iteration $ITERATION — querying both warehouses via Athena"
    run_athena_benchmarks "with"    "$GLUE_DB_WITH"    "$ITERATION"
    run_athena_benchmarks "without" "$GLUE_DB_WITHOUT" "$ITERATION"
    NEXT_QUERY=$((NOW + QUERY_INTERVAL_SECONDS))
  fi

  if (( NOW >= NEXT_MAINTENANCE )); then
    run_janitor_maintain
    NEXT_MAINTENANCE=$((NOW + MAINTENANCE_INTERVAL_SECONDS))
  fi

  sleep 2
done

# === Phase 4: Summary ===

log "generating summary"
{
  echo "=== TPC-DS Athena Benchmark — $TS ==="
  echo ""
  echo "Duration: ${DURATION_SECONDS}s, CPM: ${COMMITS_PER_MINUTE}"
  echo "Warehouse with: $WH_WITH_URL"
  echo "Warehouse without: $WH_WITHOUT_URL"
  echo ""
  echo "--- Average query latency (ms) ---"
  # Simple awk summary from the CSV
  awk -F, 'NR>1 {
    key=$3"/"$4; sum[key]+=$5; cnt[key]++
  } END {
    for (k in sum) printf "  %-20s %8.1f ms (n=%d)\n", k, sum[k]/cnt[k], cnt[k]
  }' "$RESULTS_CSV" | sort
  echo ""
  echo "--- Average file counts ---"
  awk -F, 'NR>1 {
    sum_ss[$3]+=$7; sum_sr[$3]+=$8; sum_cs[$3]+=$9; cnt[$3]++
  } END {
    for (m in cnt) printf "  %-10s ss=%.0f sr=%.0f cs=%.0f\n", m, sum_ss[m]/cnt[m], sum_sr[m]/cnt[m], sum_cs[m]/cnt[m]
  }' "$RESULTS_CSV"
} > "$SUMMARY_TXT"

log "summary:"
cat "$SUMMARY_TXT"
log "raw CSV: $RESULTS_CSV"
log "done"
