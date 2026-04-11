#!/usr/bin/env bash
# bench.sh — unified TPC-DS streaming benchmark harness for iceberg-janitor.
#
# Single entry point with three modes: local (fileblob), minio (Docker
# Compose), and aws (Fargate + Athena). All modes drive two parallel
# warehouses (with-janitor vs without-janitor), stream TPC-DS
# micro-batches into both, periodically call the janitor-server's
# /maintain endpoint on the with-janitor side, query both warehouses,
# and emit a comparison report.
#
# Usage
# =====
#
#   bench.sh local   # fileblob, no Docker, no network. Fastest.
#   bench.sh minio   # MinIO via docker compose. Mid-fidelity S3.
#   bench.sh aws     # Runs inside the bench Fargate container in AWS.
#
# Config (all modes)
# ==================
#
#   DURATION_SECONDS           Total bench duration        [300]
#   QUERY_INTERVAL_SECONDS     Seconds between query rounds [30]
#   MAINTENANCE_INTERVAL_SECONDS  Seconds between maintain  [60]
#   COMMITS_PER_MINUTE         Streamer commit rate        [60]
#   BURSTY                     Streamer bursty mode        [true]
#   BURST_MAX                  Max commits per burst       [10]
#   TARGET_FILE_SIZE           Pattern B threshold         [1MB]
#   NAMESPACE                  Iceberg namespace           [tpcds]
#
# Output
# ======
#
#   bench-results/bench-results-<ts>.csv    per-iteration metrics
#   bench-results/bench-summary-<ts>.txt    human-readable summary
#   bench-results/streamer-with-<ts>.log    streamer stdout (with-janitor)
#   bench-results/streamer-without-<ts>.log streamer stdout (without-janitor)
#   bench-results/janitor-runs-<ts>.log     janitor-server stdout + maintain I/O

set -euo pipefail

# === Argument parsing ===

MODE="${1:-}"
case "$MODE" in
  local|minio|aws) ;;
  "")
    echo "usage: $0 {local|minio|aws}" >&2
    exit 2 ;;
  *)
    echo "unknown mode: $MODE (want: local|minio|aws)" >&2
    exit 2 ;;
esac

# === Common config ===

DURATION_SECONDS="${DURATION_SECONDS:-300}"
QUERY_INTERVAL_SECONDS="${QUERY_INTERVAL_SECONDS:-30}"
MAINTENANCE_INTERVAL_SECONDS="${MAINTENANCE_INTERVAL_SECONDS:-60}"
COMMITS_PER_MINUTE="${COMMITS_PER_MINUTE:-60}"
STORE_SALES_PER_BATCH="${STORE_SALES_PER_BATCH:-500}"
STORE_RETURNS_PER_BATCH="${STORE_RETURNS_PER_BATCH:-100}"
CATALOG_SALES_PER_BATCH="${CATALOG_SALES_PER_BATCH:-300}"
NAMESPACE="${NAMESPACE:-tpcds}"
BURSTY="${BURSTY:-true}"
BURST_MAX="${BURST_MAX:-10}"
TARGET_FILE_SIZE="${TARGET_FILE_SIZE:-1MB}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." 2>/dev/null && pwd || echo /)"
GO_DIR="$REPO_ROOT/go"
RESULTS_DIR="${RESULTS_DIR:-${REPO_ROOT}/bench-results}"
mkdir -p "$RESULTS_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_CSV="$RESULTS_DIR/bench-results-$TS.csv"
SUMMARY_TXT="$RESULTS_DIR/bench-summary-$TS.txt"
STREAMER_WITH_LOG="$RESULTS_DIR/streamer-with-$TS.log"
STREAMER_WITHOUT_LOG="$RESULTS_DIR/streamer-without-$TS.log"
JANITOR_LOG="$RESULTS_DIR/janitor-runs-$TS.log"

log() { printf '\033[1;36m[bench:%s]\033[0m %s\n' "$MODE" "$*"; }

PID_WITH=""
PID_WITHOUT=""
JANITOR_SERVER_PID=""
cleanup() {
  log "shutting down..."
  [[ -n "$PID_WITH" ]] && kill "$PID_WITH" 2>/dev/null || true
  [[ -n "$PID_WITHOUT" ]] && kill "$PID_WITHOUT" 2>/dev/null || true
  [[ -n "$JANITOR_SERVER_PID" ]] && kill "$JANITOR_SERVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  log "stopped"
}
trap cleanup EXIT INT TERM

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }
}

# === Mode setup ===
#
# Each mode sets:
#   WH_WITH_URL, WH_WITHOUT_URL  — warehouse URLs for the streamers
#   JANITOR_LISTEN               — host:port the server listens on
#   SERVER_URL                   — how the bench calls the server
#   QUERY_ENGINE                 — "duckdb" or "athena"
#   JANITOR_CLI_BIN              — path to janitor-cli for analyze calls
#   JANITOR_SERVER_BIN           — path to janitor-server (local/minio only)
#   JANITOR_STREAMER_BIN         — path to janitor-streamer

case "$MODE" in
  local)
    require duckdb
    require go
    LOCAL_BASE="${WAREHOUSE_BASE:-/tmp/janitor-bench}"
    WH_WITH_URL="file://${LOCAL_BASE}-with"
    WH_WITHOUT_URL="file://${LOCAL_BASE}-without"
    log "wiping local warehouses at ${LOCAL_BASE}-{with,without}"
    rm -rf "${LOCAL_BASE}-with" "${LOCAL_BASE}-without"
    # fileblob requires the directory to exist before OpenBucket.
    mkdir -p "${LOCAL_BASE}-with" "${LOCAL_BASE}-without"
    JANITOR_LISTEN="${JANITOR_LISTEN:-127.0.0.1:8099}"
    SERVER_URL="http://${JANITOR_LISTEN}"
    QUERY_ENGINE="duckdb"

    cd "$GO_DIR"
    log "building Go binaries"
    go build -o /tmp/janitor-cli-b ./cmd/janitor-cli
    go build -o /tmp/janitor-server-b ./cmd/janitor-server
    go build -tags bench -o /tmp/janitor-streamer-b ./test/bench/streamer
    JANITOR_CLI_BIN=/tmp/janitor-cli-b
    JANITOR_SERVER_BIN=/tmp/janitor-server-b
    JANITOR_STREAMER_BIN=/tmp/janitor-streamer-b
    ;;

  minio)
    require duckdb
    require go
    require docker
    log "bringing up MinIO via docker compose"
    docker compose -f "$GO_DIR/test/mvp/docker-compose.yml" up -d >/dev/null
    export S3_ENDPOINT="http://localhost:9000"
    export S3_ACCESS_KEY="minioadmin"
    export S3_SECRET_KEY="minioadmin"
    export S3_REGION="us-east-1"
    export AWS_REGION="us-east-1"
    export AWS_DEFAULT_REGION="us-east-1"
    export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY"
    export AWS_S3_ENDPOINT="$S3_ENDPOINT"
    export AWS_ENDPOINT_URL_S3="$S3_ENDPOINT"
    # MinIO uses two separate buckets to work around gocloud.dev/blob's
    # lack of sub-prefix bucket scoping.
    WH_WITH_URL="s3://with-warehouse?endpoint=${S3_ENDPOINT}&s3ForcePathStyle=true&region=${S3_REGION}"
    WH_WITHOUT_URL="s3://without-warehouse?endpoint=${S3_ENDPOINT}&s3ForcePathStyle=true&region=${S3_REGION}"
    JANITOR_LISTEN="${JANITOR_LISTEN:-127.0.0.1:8099}"
    SERVER_URL="http://${JANITOR_LISTEN}"
    QUERY_ENGINE="duckdb"

    # Ensure buckets exist.
    log "ensuring MinIO buckets exist"
    docker exec janitor-mvp-minio mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 || true
    docker exec janitor-mvp-minio mc mb --ignore-existing local/with-warehouse >/dev/null 2>&1 || true
    docker exec janitor-mvp-minio mc mb --ignore-existing local/without-warehouse >/dev/null 2>&1 || true
    # Wipe any existing table data from previous runs.
    docker exec janitor-mvp-minio mc rm --recursive --force local/with-warehouse >/dev/null 2>&1 || true
    docker exec janitor-mvp-minio mc rm --recursive --force local/without-warehouse >/dev/null 2>&1 || true

    cd "$GO_DIR"
    log "building Go binaries"
    go build -o /tmp/janitor-cli-b ./cmd/janitor-cli
    go build -o /tmp/janitor-server-b ./cmd/janitor-server
    go build -tags bench -o /tmp/janitor-streamer-b ./test/bench/streamer
    JANITOR_CLI_BIN=/tmp/janitor-cli-b
    JANITOR_SERVER_BIN=/tmp/janitor-server-b
    JANITOR_STREAMER_BIN=/tmp/janitor-streamer-b
    ;;

  aws)
    # aws mode runs INSIDE the bench Fargate container in AWS. The
    # binaries are pre-built into the image, the warehouse buckets
    # and Glue databases are pre-provisioned by Terraform, and the
    # server runs as a separate Fargate service reachable via
    # Cloud Map at server.iceberg-janitor.local:8080.
    require aws
    : "${WH_WITH_URL:?WH_WITH_URL required in aws mode}"
    : "${WH_WITHOUT_URL:?WH_WITHOUT_URL required in aws mode}"
    : "${ATHENA_WORKGROUP:?ATHENA_WORKGROUP required}"
    : "${GLUE_DB_WITH:?GLUE_DB_WITH required}"
    : "${GLUE_DB_WITHOUT:?GLUE_DB_WITHOUT required}"
    : "${ATHENA_RESULTS_BUCKET:?ATHENA_RESULTS_BUCKET required}"
    SERVER_URL="${JANITOR_SERVER_URL:-http://server.iceberg-janitor.local:8080}"
    QUERY_ENGINE="athena"
    JANITOR_CLI_BIN="janitor-cli"
    JANITOR_STREAMER_BIN="janitor-streamer"
    # No local server in aws mode — server runs as a separate Fargate task.
    JANITOR_SERVER_BIN=""
    ;;
esac

# === Phase 1: janitor-server (local + minio) ===

if [[ "$MODE" != "aws" ]]; then
  log "starting janitor-server on ${JANITOR_LISTEN}"
  JANITOR_WAREHOUSE_URL="$WH_WITH_URL" \
  JANITOR_LISTEN="${JANITOR_LISTEN}" \
  S3_ENDPOINT="${S3_ENDPOINT:-}" \
  S3_REGION="${S3_REGION:-us-east-1}" \
  AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}" \
  AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}" \
  "$JANITOR_SERVER_BIN" >> "$JANITOR_LOG" 2>&1 &
  JANITOR_SERVER_PID=$!
  # Wait for readiness.
  for _ in $(seq 1 30); do
    if curl -sf "${SERVER_URL}/v1/healthz" >/dev/null 2>&1; then
      log "janitor-server ready (PID=$JANITOR_SERVER_PID)"
      break
    fi
    sleep 1
  done
fi

# === Phase 2: start streamers ===

start_streamer() {
  local label="$1" url="$2" catalog_db="$3" out_log="$4"
  log "starting streamer ($label) → $url"
  JANITOR_WAREHOUSE_URL="$url" \
  CATALOG_DB="$catalog_db" \
  NAMESPACE="$NAMESPACE" \
  COMMITS_PER_MINUTE="$COMMITS_PER_MINUTE" \
  STORE_SALES_PER_BATCH="$STORE_SALES_PER_BATCH" \
  STORE_RETURNS_PER_BATCH="$STORE_RETURNS_PER_BATCH" \
  CATALOG_SALES_PER_BATCH="$CATALOG_SALES_PER_BATCH" \
  DURATION_SECONDS="$((DURATION_SECONDS + 30))" \
  TRUNCATE_TABLES=true \
  BURSTY="$BURSTY" \
  BURST_MAX="$BURST_MAX" \
  "$JANITOR_STREAMER_BIN" > "$out_log" 2>&1 &
  echo $!
}

CATALOG_DB_WITH="${CATALOG_DB_WITH:-/tmp/janitor-bench-catalog-with.db}"
CATALOG_DB_WITHOUT="${CATALOG_DB_WITHOUT:-/tmp/janitor-bench-catalog-without.db}"
rm -f "$CATALOG_DB_WITH" "$CATALOG_DB_WITHOUT"

PID_WITH=$(start_streamer "with-janitor" "$WH_WITH_URL" "$CATALOG_DB_WITH" "$STREAMER_WITH_LOG")
PID_WITHOUT=$(start_streamer "without-janitor" "$WH_WITHOUT_URL" "$CATALOG_DB_WITHOUT" "$STREAMER_WITHOUT_LOG")
log "streamer PIDs: with=$PID_WITH without=$PID_WITHOUT"

log "waiting 15s for dimensions to seed and first fact commits to land..."
sleep 15

# === Phase 3: call_maintain helper ===
#
# Posts to /v1/tables/{ns}/{name}/maintain, polls for job completion.
# Returns 0 on success, 1 on failure.
#
# JSON parsing uses gawk so the bench container does not need python3.
# The server's responses are flat objects (`{"job_id":"...","status":"..."}`)
# so a regex extractor is sufficient.

json_field() {
  # Extract a flat string field from a JSON object.
  # Portable across BSD sed (macOS) and GNU sed (Linux).
  sed -n "s/.*\"$1\"[[:space:]]*:[[:space:]]*\"\([^\"]*\)\".*/\1/p" | head -1
}

call_maintain() {
  local ns_name="$1"     # e.g. "tpcds.db/store_sales"
  local ns table
  ns="${ns_name%%/*}"    # tpcds.db
  table="${ns_name#*/}"  # store_sales

  local url="${SERVER_URL}/v1/tables/${ns}/${table}/maintain"
  if [[ -n "$TARGET_FILE_SIZE" ]]; then
    url="${url}?target_file_size=${TARGET_FILE_SIZE}"
  fi

  local resp
  resp=$(curl -sf -X POST "$url" 2>>"$JANITOR_LOG")
  if [[ -z "$resp" ]]; then
    echo "maintain POST failed for $ns_name" >> "$JANITOR_LOG"
    return 1
  fi
  local job_id
  job_id=$(echo "$resp" | json_field job_id)
  if [[ -z "$job_id" ]]; then
    echo "no job_id in response: $resp" >> "$JANITOR_LOG"
    return 1
  fi

  local poll_url="${SERVER_URL}/v1/jobs/${job_id}"
  for _ in $(seq 1 300); do
    local status_resp status
    status_resp=$(curl -sf "$poll_url" 2>/dev/null || echo "")
    status=$(echo "$status_resp" | json_field status)
    case "$status" in
      completed) return 0 ;;
      failed)
        echo "maintain job $job_id failed: $status_resp" >> "$JANITOR_LOG"
        return 1 ;;
      pending|running) sleep 1 ;;
      *)
        echo "unexpected job status: $status_resp" >> "$JANITOR_LOG"
        return 1 ;;
    esac
  done
  echo "maintain job $job_id timed out" >> "$JANITOR_LOG"
  return 1
}

MAINTAIN_ROUND=0
run_maintain() {
  MAINTAIN_ROUND=$((MAINTAIN_ROUND + 1))
  log "maintain round ${MAINTAIN_ROUND}: server auto-classifies + picks mode"
  call_maintain "${NAMESPACE}.db/store_sales"   || log "  warning: maintain store_sales returned non-zero"
  call_maintain "${NAMESPACE}.db/store_returns" || log "  warning: maintain store_returns returned non-zero"
  call_maintain "${NAMESPACE}.db/catalog_sales" || log "  warning: maintain catalog_sales returned non-zero"
}

# === Phase 4: query engines ===

TPCDS_TABLES=(store_sales store_returns catalog_sales item customer customer_address customer_demographics household_demographics date_dim time_dim store promotion)
QUERY_NAMES=(q1 q3 q7 q13 q19 q25 q43 q46 q55 q96)
QUERY_SQLS=(
  "SELECT count(*) FROM (SELECT c.c_customer_id, sum(sr.sr_return_amt) FROM store_returns sr JOIN customer c ON sr.sr_customer_sk=c.c_customer_sk JOIN store s ON sr.sr_store_sk=s.s_store_sk JOIN date_dim d ON sr.sr_returned_date_sk=d.d_date_sk WHERE d.d_year=2022 GROUP BY c.c_customer_id ORDER BY 2 DESC LIMIT 100)"
  "SELECT count(*) FROM (SELECT d.d_year, i.i_brand, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE i.i_category='Electronics' GROUP BY 1,2 ORDER BY 1,3 DESC LIMIT 100)"
  "SELECT count(*) FROM (SELECT i.i_item_id, avg(ss.ss_quantity) FROM store_sales ss JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN promotion p ON ss.ss_promo_sk=p.p_promo_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_gender='F' AND cd.cd_marital_status='S' AND d.d_year=2023 GROUP BY 1 LIMIT 100)"
  "SELECT count(*) FROM (SELECT avg(ss.ss_quantity) FROM store_sales ss JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN customer_address ca ON ss.ss_addr_sk=ca.ca_address_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_marital_status='M' AND d.d_year=2022 AND ca.ca_state IN ('CA','NY','TX'))"
  "SELECT count(*) FROM (SELECT i.i_brand_id, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN customer c ON ss.ss_customer_sk=c.c_customer_sk JOIN customer_address ca ON c.c_current_addr_sk=ca.ca_address_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_moy=11 AND d.d_year=2023 AND ca.ca_state!=s.s_state GROUP BY 1 ORDER BY 2 DESC LIMIT 100)"
  "SELECT count(*) FROM (SELECT i.i_item_id, sum(ss.ss_net_profit), sum(sr.sr_net_loss) FROM store_sales ss JOIN store_returns sr ON ss.ss_customer_sk=sr.sr_customer_sk AND ss.ss_item_sk=sr.sr_item_sk AND ss.ss_ticket_number=sr.sr_ticket_number JOIN catalog_sales cs ON sr.sr_customer_sk=cs.cs_bill_customer_sk AND sr.sr_item_sk=cs.cs_item_sk JOIN date_dim d1 ON ss.ss_sold_date_sk=d1.d_date_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN item i ON ss.ss_item_sk=i.i_item_sk WHERE d1.d_year=2022 GROUP BY 1 ORDER BY 1 LIMIT 100)"
  "SELECT count(*) FROM (SELECT s.s_store_name, sum(CASE WHEN d.d_dow=0 THEN ss.ss_sales_price ELSE 0 END) FROM store_sales ss JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_year=2023 GROUP BY 1 ORDER BY 1 LIMIT 100)"
  "SELECT count(*) FROM (SELECT c.c_last_name, ca.ca_city, sum(ss.ss_net_paid) FROM store_sales ss JOIN customer c ON ss.ss_customer_sk=c.c_customer_sk JOIN customer_address ca ON c.c_current_addr_sk=ca.ca_address_sk JOIN household_demographics hd ON ss.ss_hdemo_sk=hd.hd_demo_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_year IN (2022,2023) AND hd.hd_dep_count>=2 GROUP BY 1,2 ORDER BY 3 DESC LIMIT 100)"
  "SELECT count(*) FROM (SELECT i.i_brand_id, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_moy=6 AND d.d_year=2023 AND i.i_manager_id=15 GROUP BY 1 ORDER BY 2 DESC LIMIT 100)"
  "SELECT count(*) FROM store_sales ss JOIN time_dim t ON ss.ss_sold_time_sk=t.t_time_sk JOIN household_demographics hd ON ss.ss_hdemo_sk=hd.hd_demo_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk WHERE t.t_hour=14 AND hd.hd_dep_count=3"
)

# --- DuckDB (local + minio) ---

run_duckdb_queries() {
  local label="$1" warehouse_url="$2" iteration="$3"
  local warehouse_path
  case "$warehouse_url" in
    file://*) warehouse_path="${warehouse_url#file://}" ;;
    *)        warehouse_path="$warehouse_url" ;;
  esac

  local setup_file query_file
  setup_file=$(mktemp)
  cat > "$setup_file" <<EOF
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET unsafe_enable_version_guessing = true;
EOF
  if [[ "$warehouse_url" == s3://* ]]; then
    local ep_clean="${S3_ENDPOINT#http://}"
    ep_clean="${ep_clean#https://}"
    cat >> "$setup_file" <<EOF
SET s3_endpoint='${ep_clean}';
SET s3_access_key_id='${S3_ACCESS_KEY:-${AWS_ACCESS_KEY_ID:-}}';
SET s3_secret_access_key='${S3_SECRET_KEY:-${AWS_SECRET_ACCESS_KEY:-}}';
SET s3_region='${AWS_REGION:-us-east-1}';
SET s3_url_style='path';
SET s3_use_ssl=false;
EOF
  fi
  for table in "${TPCDS_TABLES[@]}"; do
    echo "CREATE OR REPLACE VIEW ${table} AS SELECT * FROM iceberg_scan('${warehouse_path}/${NAMESPACE}.db/${table}');" >> "$setup_file"
  done

  # File counts via janitor-cli.
  local files_ss files_sr files_cs
  set +e
  files_ss=$(JANITOR_WAREHOUSE_URL="$warehouse_url" "$JANITOR_CLI_BIN" analyze "${NAMESPACE}.db/store_sales"    2>/dev/null | awk '/^Data files:/{print $3}')
  files_sr=$(JANITOR_WAREHOUSE_URL="$warehouse_url" "$JANITOR_CLI_BIN" analyze "${NAMESPACE}.db/store_returns"  2>/dev/null | awk '/^Data files:/{print $3}')
  files_cs=$(JANITOR_WAREHOUSE_URL="$warehouse_url" "$JANITOR_CLI_BIN" analyze "${NAMESPACE}.db/catalog_sales"  2>/dev/null | awk '/^Data files:/{print $3}')
  set -e
  [[ -z "$files_ss" ]] && files_ss="-1"
  [[ -z "$files_sr" ]] && files_sr="-1"
  [[ -z "$files_cs" ]] && files_cs="-1"

  for i in "${!QUERY_NAMES[@]}"; do
    local qname="${QUERY_NAMES[$i]}"
    local qsql="${QUERY_SQLS[$i]}"
    query_file=$(mktemp)
    cat "$setup_file" > "$query_file"
    echo "$qsql;" >> "$query_file"

    local start_ms end_ms elapsed_ms row_count
    # Epoch-millisecond clock via Perl's Time::HiRes (5 ms startup, cross-
    # platform, no coreutils dependency). CANNOT use Python's
    # time.monotonic_ns() here — the monotonic clock has an undefined
    # reference point across separate process invocations, which makes
    # differences nonsensical (and sometimes negative).
    start_ms=$(perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000')
    set +e
    row_count=$(duckdb -csv -noheader < "$query_file" 2>>"$RESULTS_DIR/duckdb-errors-$TS.log" | tail -1 | tr -d '\r" ')
    set -e
    end_ms=$(perl -MTime::HiRes=time -e 'printf "%d\n", time()*1000')
    elapsed_ms=$(( end_ms - start_ms ))
    [[ -z "$row_count" ]] && row_count="-1"

    echo "$iteration,$(date -u +%FT%TZ),$label,$qname,$elapsed_ms,$row_count,$files_ss,$files_sr,$files_cs" >> "$RESULTS_CSV"
    rm -f "$query_file"
  done
  rm -f "$setup_file"
}

# --- Athena (aws) ---

run_athena_query() {
  local database="$1" sql="$2"
  local query_id state
  query_id=$(aws athena start-query-execution \
    --query-string "$sql" \
    --work-group "$ATHENA_WORKGROUP" \
    --query-execution-context "Database=$database" \
    --result-configuration "OutputLocation=s3://${ATHENA_RESULTS_BUCKET}/results/" \
    --region "$AWS_REGION" \
    --query 'QueryExecutionId' --output text)
  state="RUNNING"
  while [[ "$state" == "RUNNING" || "$state" == "QUEUED" ]]; do
    sleep 1
    state=$(aws athena get-query-execution \
      --query-execution-id "$query_id" \
      --region "$AWS_REGION" \
      --query 'QueryExecution.Status.State' --output text)
  done
  [[ "$state" != "SUCCEEDED" ]] && { echo "$query_id"; return 1; }
  echo "$query_id"
}

run_athena_queries() {
  local label="$1" database="$2" iteration="$3"
  for i in "${!QUERY_NAMES[@]}"; do
    local qname="${QUERY_NAMES[$i]}"
    local qsql="${QUERY_SQLS[$i]}"
    local elapsed_ms row_count
    set +e
    local qid
    qid=$(run_athena_query "$database" "$qsql")
    local rc=$?
    set -e
    if [[ $rc -eq 0 ]]; then
      # Use Athena's own engine execution time (more accurate than
      # wall-clock, which would include network + polling overhead).
      elapsed_ms=$(aws athena get-query-execution \
        --query-execution-id "$qid" \
        --region "$AWS_REGION" \
        --query 'QueryExecution.Statistics.EngineExecutionTimeInMillis' --output text 2>/dev/null || echo "-1")
      row_count=$(aws athena get-query-results \
        --query-execution-id "$qid" \
        --region "$AWS_REGION" \
        --query 'ResultSet.Rows[1].Data[0].VarCharValue' --output text 2>/dev/null || echo "-1")
    else
      elapsed_ms="-1"
      row_count="-1"
    fi
    echo "$iteration,$(date -u +%FT%TZ),$label,$qname,$elapsed_ms,$row_count,-1,-1,-1" >> "$RESULTS_CSV"
  done
}

run_queries() {
  local label="$1" iteration="$2"
  case "$QUERY_ENGINE" in
    duckdb)
      local wh
      [[ "$label" == "with" ]] && wh="$WH_WITH_URL" || wh="$WH_WITHOUT_URL"
      run_duckdb_queries "$label" "$wh" "$iteration"
      ;;
    athena)
      local db
      [[ "$label" == "with" ]] && db="$GLUE_DB_WITH" || db="$GLUE_DB_WITHOUT"
      run_athena_queries "$label" "$db" "$iteration"
      ;;
  esac
}

# === Phase 5: main loop ===

echo "iteration,timestamp,mode,query,latency_ms,row_count,store_sales_files,store_returns_files,catalog_sales_files" > "$RESULTS_CSV"

START=$(date +%s)
NEXT_QUERY=$START
NEXT_MAINTENANCE=$((START + MAINTENANCE_INTERVAL_SECONDS))
ITERATION=0

while true; do
  NOW=$(date +%s)
  ELAPSED=$((NOW - START))
  if (( ELAPSED >= DURATION_SECONDS )); then break; fi

  if (( NOW >= NEXT_QUERY )); then
    ITERATION=$((ITERATION + 1))
    log "[t+${ELAPSED}s] iteration $ITERATION — querying both warehouses"
    run_queries "with"    "$ITERATION"
    run_queries "without" "$ITERATION"
    NEXT_QUERY=$((NOW + QUERY_INTERVAL_SECONDS))
  fi

  if (( NOW >= NEXT_MAINTENANCE )); then
    run_maintain
    NEXT_MAINTENANCE=$((NOW + MAINTENANCE_INTERVAL_SECONDS))
  fi

  sleep 2
done

# === Phase 6: summary ===

log "generating summary"

if command -v duckdb >/dev/null 2>&1; then
  duckdb <<SQL > "$SUMMARY_TXT" 2>&1
.mode markdown
SELECT 'TPC-DS streaming benchmark — ${MODE} mode — $TS' AS title;
.headers on
SELECT
  query,
  ROUND(AVG(CASE WHEN mode='without' THEN latency_ms END), 1) AS without_avg_ms,
  ROUND(AVG(CASE WHEN mode='with'    THEN latency_ms END), 1) AS with_avg_ms,
  ROUND(100.0 * (
    AVG(CASE WHEN mode='with'    THEN latency_ms END) -
    AVG(CASE WHEN mode='without' THEN latency_ms END)
  ) / AVG(CASE WHEN mode='without' THEN latency_ms END), 1) AS pct_change
FROM read_csv_auto('$RESULTS_CSV', header=true)
GROUP BY query
ORDER BY query;

SELECT
  mode,
  ROUND(AVG(store_sales_files), 1)   AS avg_ss_files,
  ROUND(AVG(store_returns_files), 1) AS avg_sr_files,
  ROUND(AVG(catalog_sales_files), 1) AS avg_cs_files
FROM read_csv_auto('$RESULTS_CSV', header=true)
GROUP BY mode;
SQL
else
  # aws mode inside Fargate — awk fallback (no duckdb).
  awk -F, 'NR>1 {
    key=$3"/"$4; sum[key]+=$5; cnt[key]++
  } END {
    printf "TPC-DS bench — %s\n", "'"$MODE"' $TS"
    for (k in sum) printf "  %-20s %8.1f ms (n=%d)\n", k, sum[k]/cnt[k], cnt[k]
  }' "$RESULTS_CSV" | sort > "$SUMMARY_TXT"
fi

log "summary: $SUMMARY_TXT"
log "raw CSV: $RESULTS_CSV"
log "==== summary ===="
cat "$SUMMARY_TXT"
log "================="
