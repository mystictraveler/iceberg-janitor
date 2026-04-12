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
# Pattern: phased A/B, NOT interleaved
# ====================================
#
# Phase 1  STREAM    Both streamers write to WITH and WITHOUT warehouses
#                    for STREAM_DURATION_SECONDS, then self-terminate.
#                    Identical data lands on both sides.
# Phase 2  PAUSE     Short pause (PAUSE_SECONDS) so in-flight S3 writes
#                    settle before we look at the warehouses.
# Phase 3  MAINTAIN  Call /maintain on the WITH warehouse only. Run
#                    MAINTAIN_ROUNDS rounds so the hot path gets the
#                    chance to stitch multiple times. WITHOUT warehouse
#                    stays raw — this is the baseline.
# Phase 4  QUERY     Run the TPC-DS query suite QUERY_ITERATIONS times
#                    against BOTH warehouses. Clean measurement with no
#                    streamer noise and no mid-query maintain calls.
#
# Config (all modes)
# ==================
#
#   STREAM_DURATION_SECONDS    How long to stream before stopping   [300]
#   PAUSE_SECONDS              Settle time between stream+maintain  [15]
#   MAINTAIN_ROUNDS            How many maintain passes on WITH     [2]
#   QUERY_ITERATIONS           How many query rounds after maintain [3]
#   COMMITS_PER_MINUTE         Streamer commit rate                 [60]
#   BURSTY                     Streamer bursty mode                 [true]
#   BURST_MAX                  Max commits per burst                [10]
#   TARGET_FILE_SIZE           Pattern B threshold                  [1MB]
#   NAMESPACE                  Iceberg namespace                    [tpcds]
#
# Legacy env vars (DURATION_SECONDS, QUERY_INTERVAL_SECONDS,
# MAINTENANCE_INTERVAL_SECONDS) are still honored: DURATION_SECONDS
# maps to STREAM_DURATION_SECONDS for backward compatibility.
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

STREAM_DURATION_SECONDS="${STREAM_DURATION_SECONDS:-${DURATION_SECONDS:-300}}"
PAUSE_SECONDS="${PAUSE_SECONDS:-15}"
MAINTAIN_ROUNDS="${MAINTAIN_ROUNDS:-2}"
QUERY_ITERATIONS="${QUERY_ITERATIONS:-3}"
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

# === Phase 2: Glue pre-registration + start streamers ===

# glue_refresh must be defined here (before first call) so the
# pre-stream registration can use it. The function is also called
# in the post-stream and post-maintain phases below.
glue_refresh() {
  [[ "$MODE" != "aws" ]] && return 0
  local label="$1" warehouse_url="$2" glue_db="$3"
  log "glue-register $label → $glue_db"
  JANITOR_WAREHOUSE_URL="$warehouse_url" \
  AWS_REGION="$AWS_REGION" \
    janitor-cli glue-register --database "$glue_db" "${NAMESPACE}.db" >> "$JANITOR_LOG" 2>&1 \
    || log "  warning: glue-register $label returned non-zero"
}

# Pre-register Glue tables BEFORE streamers start. Tables are empty
# (or don't exist yet on a fresh warehouse) so registration is
# instant — just creating a Glue table pointer to the initial
# metadata.json. The post-stream refresh then only updates the
# metadata_location pointer, not discovers from scratch.
if [[ "$MODE" == "aws" ]]; then
  log "pre-registering Glue tables (empty tables — instant)"
  glue_refresh "with (pre-stream)"    "$WH_WITH_URL"    "${GLUE_DB_WITH:-}"
  glue_refresh "without (pre-stream)" "$WH_WITHOUT_URL" "${GLUE_DB_WITHOUT:-}"
fi

start_streamer() {
  local label="$1" url="$2" catalog_db="$3" out_log="$4"
  # log goes to stderr so command substitution only captures the PID.
  # If we logged to stdout, `PID=$(start_streamer ...)` would capture
  # the log line AND the PID together, and kill -0 would fail on the
  # garbage string, making the stream phase exit at t+0.
  log "starting streamer ($label) → $url" >&2
  JANITOR_WAREHOUSE_URL="$url" \
  CATALOG_DB="$catalog_db" \
  NAMESPACE="$NAMESPACE" \
  COMMITS_PER_MINUTE="$COMMITS_PER_MINUTE" \
  STORE_SALES_PER_BATCH="$STORE_SALES_PER_BATCH" \
  STORE_RETURNS_PER_BATCH="$STORE_RETURNS_PER_BATCH" \
  CATALOG_SALES_PER_BATCH="$CATALOG_SALES_PER_BATCH" \
  DURATION_SECONDS="$STREAM_DURATION_SECONDS" \
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
for pid_var in PID_WITH PID_WITHOUT; do
  pid_val="${!pid_var}"
  if ! [[ "$pid_val" =~ ^[0-9]+$ ]]; then
    echo "FATAL: $pid_var is not a numeric PID: $pid_val" >&2
    exit 3
  fi
done
log "streamer PIDs: with=$PID_WITH without=$PID_WITHOUT"
log "streaming for ${STREAM_DURATION_SECONDS}s..."

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

# LAST_METADATA_LOCATION is set by call_maintain on success. The
# orchestration layer reads it to update external catalogs (Glue)
# with a direct pointer — no S3 prefix discovery needed.
LAST_METADATA_LOCATION=""

call_maintain() {
  local ns_name="$1"     # e.g. "tpcds.db/store_sales"
  local ns table
  ns="${ns_name%%/*}"    # tpcds.db
  table="${ns_name#*/}"  # store_sales
  LAST_METADATA_LOCATION=""

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
  for _ in $(seq 1 2400); do
    local status_resp status
    status_resp=$(curl -sf "$poll_url" 2>/dev/null || echo "")
    status=$(echo "$status_resp" | json_field status)
    case "$status" in
      completed)
        # Extract metadata_location from the job result so the
        # orchestration layer can update Glue directly without
        # running the expensive S3 prefix discovery.
        LAST_METADATA_LOCATION=$(echo "$status_resp" | json_field metadata_location)
        return 0 ;;
      failed)
        echo "maintain job $job_id failed: $status_resp" >> "$JANITOR_LOG"
        return 1 ;;
      pending|running) sleep 1 ;;
      *)
        echo "unexpected job status: $status_resp" >> "$JANITOR_LOG"
        return 1 ;;
    esac
  done
  echo "maintain job $job_id timed out after 2400s" >> "$JANITOR_LOG"
  return 1
}

# glue_refresh re-registers every table in a warehouse with its
# current Iceberg metadata_location. REQUIRED after any action that
# changes the current snapshot (streamer commits, janitor maintain)
# because Athena reads Iceberg tables via Glue, and the Glue table
# parameter `metadata_location` is pinned at registration time.
# Without this step, Athena reads the STALE metadata.json that was
# current when the tables were first registered — which makes every
# query measurement noise, not signal.
#
# Called as an EXTERNAL ORCHESTRATOR step after every successful
# maintain job in addition to the before/after-phase refreshes —
# downstream systems (Athena, Trino, any catalog-backed query
# engine) can't see a janitor compaction win until Glue is pointed
# at the new metadata_location. The janitor itself is catalog-less
# by design; propagating "metadata changed" is the orchestrator's
# job.
#
# The prefix argument to janitor-cli glue-register is NOT the
# namespace name — it's the bucket-relative path. iceberg-go's
# default location provider puts tables at `<ns>.db/<name>/`, so
# the correct discover prefix is `<ns>.db` (or empty for whole
# bucket). Passing just `<ns>` finds nothing — this was the bug
# in Run 18 that made the A/B signal zero.
#
# glue_refresh is defined in Phase 2 above (before first call).

# glue_update_metadata_location does a DIRECT Glue UpdateTable with
# the exact metadata_location from the job result. Milliseconds vs
# the 12+ minutes that glue_refresh takes (which does S3 prefix
# discovery). Only called in aws mode.
glue_update_metadata_location() {
  [[ "$MODE" != "aws" ]] && return 0
  local table="$1" glue_db="$2" metadata_loc="$3"
  if [[ -z "$metadata_loc" ]]; then
    log "  warning: no metadata_location for $table — falling back to glue_refresh"
    glue_refresh "with (fallback ${table})" "$WH_WITH_URL" "$glue_db"
    return
  fi
  log "  glue-update $table → $metadata_loc"
  # Use janitor-cli's fast path (--metadata-location) instead of raw
  # aws glue update-table. The CLI uses pkg/aws SigV4 signing which
  # resolves Fargate task role credentials the same way as every other
  # AWS call the bench makes — no separate Glue IAM policy needed.
  JANITOR_WAREHOUSE_URL="$WH_WITH_URL" \
  AWS_REGION="$AWS_REGION" \
    janitor-cli glue-register --database "$glue_db" --table "$table" --metadata-location "$metadata_loc" >> "$JANITOR_LOG" 2>&1 \
    || log "  warning: glue-update $table failed"
}

MAINTAIN_ROUND=0
run_maintain() {
  MAINTAIN_ROUND=$((MAINTAIN_ROUND + 1))
  log "maintain round ${MAINTAIN_ROUND}: server auto-classifies + picks mode"
  for table in store_sales store_returns catalog_sales; do
    if call_maintain "${NAMESPACE}.db/${table}"; then
      # Use the metadata_location from the job result to update
      # Glue directly — no S3 prefix discovery needed.
      glue_update_metadata_location "$table" "${GLUE_DB_WITH:-}" "$LAST_METADATA_LOCATION"
    else
      log "  warning: maintain ${table} returned non-zero — skipping glue refresh"
    fi
  done
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

# === Phase A: stream (wait for self-termination) ===

echo "iteration,timestamp,mode,query,latency_ms,row_count,store_sales_files,store_returns_files,catalog_sales_files" > "$RESULTS_CSV"

START=$(date +%s)

# Streamers are configured with DURATION_SECONDS=STREAM_DURATION_SECONDS
# and will self-exit. We just wait for them. Pass a small grace window
# for dimension seed + shutdown flush (~45s) before timing out.
STREAM_DEADLINE=$(( START + STREAM_DURATION_SECONDS + 45 ))
while (( $(date +%s) < STREAM_DEADLINE )); do
  if ! kill -0 "$PID_WITH" 2>/dev/null && ! kill -0 "$PID_WITHOUT" 2>/dev/null; then
    break
  fi
  sleep 2
done
kill "$PID_WITH" 2>/dev/null || true
kill "$PID_WITHOUT" 2>/dev/null || true
wait "$PID_WITH" 2>/dev/null || true
wait "$PID_WITHOUT" 2>/dev/null || true
PID_WITH=""
PID_WITHOUT=""
STREAM_ELAPSED=$(( $(date +%s) - START ))
log "stream phase done in ${STREAM_ELAPSED}s"

# Capture streamer totals from their log tail — last progress line has
# per-fact commit counts. Best-effort; logged for visibility only.
for label in with without; do
  log_file="$RESULTS_DIR/streamer-${label}-$TS.log"
  if [[ -f "$log_file" ]]; then
    tail_line=$(grep -E '^\[.+\] total [0-9]+ commits' "$log_file" | tail -1 || true)
    [[ -n "$tail_line" ]] && log "streamer[$label]: $tail_line"
  fi
done

# === Phase B: pause + glue pre-refresh (let S3 writes settle) ===

log "pause phase — sleeping ${PAUSE_SECONDS}s for S3 writes to settle"
sleep "$PAUSE_SECONDS"

# Re-register Glue for the WITHOUT warehouse only. The WITHOUT
# warehouse is never maintained, so this is its only Glue update
# (one-time cost via the slow discovery path — ~12 min on a
# warehouse with ~200 committed metadata versions).
#
# The WITH warehouse does NOT need a post-stream Glue refresh
# because the maintain step updates Glue directly per table via
# the metadata_location field from the job result. Athena queries
# only happen AFTER maintain, so the WITH pointer will be fresh.
glue_refresh "without" "$WH_WITHOUT_URL" "${GLUE_DB_WITHOUT:-}"

# === Phase C: maintain (WITH warehouse only, N rounds) ===

log "maintain phase — ${MAINTAIN_ROUNDS} rounds on WITH warehouse"
MAINTAIN_START=$(date +%s)
for round in $(seq 1 "$MAINTAIN_ROUNDS"); do
  log "maintain round ${round}/${MAINTAIN_ROUNDS}"
  run_maintain
done
MAINTAIN_ELAPSED=$(( $(date +%s) - MAINTAIN_START ))
log "maintain phase done in ${MAINTAIN_ELAPSED}s"

# Post-maintain Glue refresh for WITH is handled inside run_maintain
# via glue_update_metadata_location (direct UpdateTable with the
# metadata_location from the job result). No separate glue_refresh
# call needed — saves 12 min of S3 prefix discovery.

# === Phase D: query (both warehouses, back-to-back iterations) ===
#
# Iteration 1 is treated as a warmup and dropped from the summary
# aggregates. Athena has significant cold-start variance on the first
# query against a given table; including it inflates the with/without
# comparison with noise that has nothing to do with the maintain work.

log "query phase — ${QUERY_ITERATIONS} iterations against both warehouses (iter 1 = warmup, dropped from summary)"
QUERY_START=$(date +%s)
for ITERATION in $(seq 1 "$QUERY_ITERATIONS"); do
  log "query iteration ${ITERATION}/${QUERY_ITERATIONS}"
  run_queries "with"    "$ITERATION"
  run_queries "without" "$ITERATION"
done
QUERY_ELAPSED=$(( $(date +%s) - QUERY_START ))
log "query phase done in ${QUERY_ELAPSED}s"

TOTAL_ELAPSED=$(( $(date +%s) - START ))
log "total bench wall time: ${TOTAL_ELAPSED}s (stream=${STREAM_ELAPSED}s pause=${PAUSE_SECONDS}s maintain=${MAINTAIN_ELAPSED}s query=${QUERY_ELAPSED}s)"

# === Phase 6: summary ===

log "generating summary"

if command -v duckdb >/dev/null 2>&1; then
  duckdb <<SQL > "$SUMMARY_TXT" 2>&1
.mode markdown
SELECT 'TPC-DS streaming benchmark — ${MODE} mode — $TS (iter 1 dropped as warmup)' AS title;
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
WHERE iteration > 1
GROUP BY query
ORDER BY query;

SELECT
  mode,
  ROUND(AVG(store_sales_files), 1)   AS avg_ss_files,
  ROUND(AVG(store_returns_files), 1) AS avg_sr_files,
  ROUND(AVG(catalog_sales_files), 1) AS avg_cs_files
FROM read_csv_auto('$RESULTS_CSV', header=true)
WHERE iteration > 1
GROUP BY mode;
SQL
else
  # aws mode inside Fargate — awk fallback (no duckdb).
  # Drop iteration 1 (first column of each row, after header). The
  # first Athena run against a table is a cold-start outlier and
  # contaminates the average.
  awk -F, 'NR>1 && $1 > 1 {
    key=$3"/"$4; sum[key]+=$5; cnt[key]++
  } END {
    printf "TPC-DS bench — %s (iter 1 dropped as warmup)\n", "'"$MODE"' $TS"
    for (k in sum) printf "  %-20s %8.1f ms (n=%d)\n", k, sum[k]/cnt[k], cnt[k]
  }' "$RESULTS_CSV" | sort > "$SUMMARY_TXT"
fi

# Flush summary directly to stdout, belt-and-suspenders. CloudWatch
# log batch flushing on container shutdown can truncate the last
# few events; writing the summary via printf ensures each line is
# its own libc write() which the awslogs driver picks up reliably.
log "summary: $SUMMARY_TXT"
log "raw CSV: $RESULTS_CSV"
log "==== summary ===="
while IFS= read -r summary_line; do
  printf '%s\n' "$summary_line"
done < "$SUMMARY_TXT"
log "================="
# Also dump the raw CSV inline so the per-query rows survive in
# CloudWatch even if the summary aggregation fails. Small cost.
log "==== raw CSV ===="
while IFS= read -r csv_line; do
  printf '%s\n' "$csv_line"
done < "$RESULTS_CSV"
log "================="

# === Phase 7: persist artifacts to S3 (aws mode only) ===
#
# The bench container's /bench-results directory is ephemeral — it
# dies with the Fargate task. Without this upload, a future user who
# wants to revisit the numbers has to archaeology through CloudWatch
# for the raw CSV rows. Instead, copy everything to a durable S3
# prefix that survives the task and is keyed by the run timestamp.
#
# Bucket: ATHENA_RESULTS_BUCKET is already provisioned by terraform
# and already writable by the bench task role (it's used for Athena
# results). Reusing it avoids a new IAM policy.
#
# Layout:
#   s3://$ATHENA_RESULTS_BUCKET/bench-runs/$TS/
#     ├── bench-summary.txt
#     ├── bench-results.csv
#     ├── streamer-with.log
#     ├── streamer-without.log
#     └── janitor-runs.log
if [[ "$MODE" == "aws" && -n "${ATHENA_RESULTS_BUCKET:-}" ]]; then
  s3_prefix="s3://${ATHENA_RESULTS_BUCKET}/bench-runs/${TS}"
  log "persisting bench artifacts to ${s3_prefix}/"
  for f in "$SUMMARY_TXT" "$RESULTS_CSV" "$STREAMER_WITH_LOG" "$STREAMER_WITHOUT_LOG" "$JANITOR_LOG"; do
    [[ -f "$f" ]] || continue
    base=$(basename "$f")
    if aws s3 cp "$f" "${s3_prefix}/${base}" --region "$AWS_REGION" --only-show-errors 2>>"$JANITOR_LOG"; then
      log "  uploaded $base"
    else
      log "  warning: failed to upload $base"
    fi
  done
  log "bench artifacts persisted at ${s3_prefix}/"
fi
