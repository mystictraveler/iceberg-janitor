#!/usr/bin/env bash
# bench-tpcds.sh — TPC-DS streaming benchmark runner for the janitor.
#
# Drives two parallel warehouses ("with-janitor" and "without-janitor"),
# streams identical TPC-DS micro-batches into both, periodically runs the
# Go janitor's compaction on the with-janitor warehouse, runs the 10
# canonical TPC-DS benchmark queries via DuckDB on both at intervals,
# and records the results to a CSV. At the end it prints a comparison
# report showing the latency / file count divergence between the two
# warehouses over time.
#
# This is the AWS streaming benchmark harness — proof of life for the
# architectural claim that the janitor's middle-path remediation is
# operationally equivalent to moonlink for query freshness while
# preserving the catalog-less, multi-cloud, serverless cost model.
#
# Defaults are tuned for a 5-minute local fileblob smoke test. For
# AWS, set WAREHOUSE_BASE to s3://your-bucket and provide AWS
# credentials in the environment.
#
# Usage
# =====
#
#   # 5-min local smoke test (no Docker, no AWS):
#   ./go/test/bench/bench-tpcds.sh
#
#   # 1-hour AWS test:
#   WAREHOUSE_BASE=s3://my-warehouse-bench \
#   AWS_REGION=us-east-1 \
#   DURATION_SECONDS=3600 \
#   QUERY_INTERVAL_SECONDS=120 \
#   MAINTENANCE_INTERVAL_SECONDS=180 \
#   ./go/test/bench/bench-tpcds.sh
#
# Output
# ======
#
#   - bench-results-<timestamp>.csv  per-iteration metrics
#   - bench-summary-<timestamp>.txt  human-readable comparison report
#   - streamer-with.log              streamer stdout/stderr (with-janitor)
#   - streamer-without.log           streamer stdout/stderr (without-janitor)
#   - janitor-runs.log               every janitor compact invocation

set -euo pipefail

# === Configuration (environment-overridable) ===

WAREHOUSE_BASE="${WAREHOUSE_BASE:-/tmp/janitor-bench}"
DURATION_SECONDS="${DURATION_SECONDS:-300}"            # 5 min default
QUERY_INTERVAL_SECONDS="${QUERY_INTERVAL_SECONDS:-30}" # query both warehouses every 30s
MAINTENANCE_INTERVAL_SECONDS="${MAINTENANCE_INTERVAL_SECONDS:-60}" # janitor every 60s
COMMITS_PER_MINUTE="${COMMITS_PER_MINUTE:-30}"
STORE_SALES_PER_BATCH="${STORE_SALES_PER_BATCH:-500}"
STORE_RETURNS_PER_BATCH="${STORE_RETURNS_PER_BATCH:-100}"
CATALOG_SALES_PER_BATCH="${CATALOG_SALES_PER_BATCH:-300}"
NAMESPACE="${NAMESPACE:-tpcds}"

# Resolved paths.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
GO_DIR="$REPO_ROOT/go"
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/bench-results}"
mkdir -p "$RESULTS_DIR"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RESULTS_CSV="$RESULTS_DIR/bench-results-$TS.csv"
SUMMARY_TXT="$RESULTS_DIR/bench-summary-$TS.txt"
STREAMER_WITH_LOG="$RESULTS_DIR/streamer-with-$TS.log"
STREAMER_WITHOUT_LOG="$RESULTS_DIR/streamer-without-$TS.log"
JANITOR_LOG="$RESULTS_DIR/janitor-runs-$TS.log"

# Warehouse-specific URLs and paths. For S3 we treat WAREHOUSE_BASE as
# an s3:// URL prefix; we append -with and -without as subprefixes.
case "$WAREHOUSE_BASE" in
  s3://*)
    WH_WITH_URL="$WAREHOUSE_BASE/with"
    WH_WITHOUT_URL="$WAREHOUSE_BASE/without"
    LOCAL_BASE=""
    ;;
  /*)
    WH_WITH_URL="file://$WAREHOUSE_BASE-with"
    WH_WITHOUT_URL="file://$WAREHOUSE_BASE-without"
    LOCAL_BASE="$WAREHOUSE_BASE"
    ;;
  *)
    echo "WAREHOUSE_BASE must be an absolute path or an s3:// URL; got: $WAREHOUSE_BASE" >&2
    exit 2
    ;;
esac

CATALOG_DB_WITH="${CATALOG_DB_WITH:-/tmp/janitor-bench-catalog-with.db}"
CATALOG_DB_WITHOUT="${CATALOG_DB_WITHOUT:-/tmp/janitor-bench-catalog-without.db}"

# Export the cloud credentials globally so every subprocess (streamers,
# janitor-cli, duckdb) inherits the same set. The streamers also map
# S3_* into AWS_* internally, but analyze and duckdb both need the
# AWS_* variables directly.
export S3_ENDPOINT="${S3_ENDPOINT:-}"
export S3_ACCESS_KEY="${S3_ACCESS_KEY:-}"
export S3_SECRET_KEY="${S3_SECRET_KEY:-}"
export S3_REGION="${S3_REGION:-${AWS_REGION:-us-east-1}}"
export AWS_REGION="${AWS_REGION:-${S3_REGION}}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION}}"
[[ -n "$S3_ACCESS_KEY" ]] && export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-$S3_ACCESS_KEY}"
[[ -n "$S3_SECRET_KEY" ]] && export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-$S3_SECRET_KEY}"
[[ -n "$S3_ENDPOINT" ]] && export AWS_S3_ENDPOINT="$S3_ENDPOINT" && export AWS_ENDPOINT_URL_S3="$S3_ENDPOINT"

# === Helpers ===

log() { printf '\033[1;36m[bench]\033[0m %s\n' "$*"; }

cleanup() {
  log "shutting down streamers..."
  [[ -n "${PID_WITH:-}" ]] && kill "$PID_WITH" 2>/dev/null || true
  [[ -n "${PID_WITHOUT:-}" ]] && kill "$PID_WITHOUT" 2>/dev/null || true
  wait 2>/dev/null || true
  log "streamers stopped"
}
trap cleanup EXIT INT TERM

# === Phase 0: prerequisites ===

require() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }
}
require go
require duckdb

if [[ -n "$LOCAL_BASE" ]]; then
  log "wiping local warehouses at $LOCAL_BASE-{with,without}"
  rm -rf "$LOCAL_BASE-with" "$LOCAL_BASE-without"
  rm -f "$CATALOG_DB_WITH" "$CATALOG_DB_WITHOUT"
fi

cd "$GO_DIR"
log "building Go binaries"
go build -o /tmp/janitor-cli ./cmd/janitor-cli
go build -o /tmp/janitor-streamer ./cmd/janitor-streamer

# === Phase 1: kick off two streamers in the background ===

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
  /tmp/janitor-streamer > "$out_log" 2>&1 &
  echo $!
}

PID_WITH=$(start_streamer "with-janitor" "$WH_WITH_URL" "$CATALOG_DB_WITH" "$STREAMER_WITH_LOG")
PID_WITHOUT=$(start_streamer "without-janitor" "$WH_WITHOUT_URL" "$CATALOG_DB_WITHOUT" "$STREAMER_WITHOUT_LOG")

log "PIDs: with=$PID_WITH without=$PID_WITHOUT"
log "waiting 15s for dimensions to seed and first fact commits to land..."
sleep 15

# === Phase 2: query loop ===

# Initialize the results CSV.
echo "iteration,timestamp,mode,query,latency_ms,row_count,store_sales_files,store_returns_files,catalog_sales_files" > "$RESULTS_CSV"

# DuckDB query helper. Builds a script that creates views for the 12 TPC-DS
# tables under the given warehouse path, then runs each of the 10 benchmark
# queries with timing.
run_duckdb_queries() {
  local label="$1" warehouse_url="$2" iteration="$3"
  local warehouse_path
  case "$warehouse_url" in
    file://*) warehouse_path="${warehouse_url#file://}" ;;
    *)        warehouse_path="$warehouse_url" ;;  # s3:// passes through as-is
  esac

  local sql_file
  sql_file=$(mktemp)
  cat > "$sql_file" <<EOF
INSTALL iceberg; LOAD iceberg;
INSTALL httpfs; LOAD httpfs;
SET unsafe_enable_version_guessing = true;
EOF
  if [[ "$warehouse_url" == s3://* ]]; then
    cat >> "$sql_file" <<EOF
CREATE OR REPLACE SECRET bench_s3 (
  TYPE S3,
  REGION '${AWS_REGION:-us-east-1}'$([ -n "${S3_ENDPOINT:-}" ] && printf ",\n  ENDPOINT '%s'" "${S3_ENDPOINT#http*://}"),
  URL_STYLE 'path'$([ -n "${S3_ENDPOINT:-}" ] && printf ",\n  USE_SSL false")
);
EOF
  fi
  for table in store_sales store_returns catalog_sales item customer customer_address customer_demographics household_demographics date_dim time_dim store promotion; do
    echo "CREATE OR REPLACE VIEW ${table} AS SELECT * FROM iceberg_scan('${warehouse_path}/${NAMESPACE}.db/${table}');" >> "$sql_file"
  done

  # 10 TPC-DS benchmark queries — verbatim from pkg/tpcds.BenchmarkQueries
  # but with the {ns}. prefix removed (we use views in the default schema).
  cat >> "$sql_file" <<'EOF'
.mode csv
.headers off
.timer on
SELECT 'q1', NULL, COUNT(*) FROM (
  SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
         sum(sr.sr_return_amt) as total_returns
  FROM store_returns sr
  JOIN customer c ON sr.sr_customer_sk = c.c_customer_sk
  JOIN store s ON sr.sr_store_sk = s.s_store_sk
  JOIN date_dim d ON sr.sr_returned_date_sk = d.d_date_sk
  WHERE d.d_year = 2022
  GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
  ORDER BY total_returns DESC LIMIT 100
);
SELECT 'q3', NULL, COUNT(*) FROM (
  SELECT d.d_year, i.i_brand, i.i_brand_id, sum(ss.ss_ext_sales_price) as revenue
  FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE i.i_category = 'Electronics'
  GROUP BY d.d_year, i.i_brand, i.i_brand_id
  ORDER BY d.d_year, revenue DESC LIMIT 100
);
SELECT 'q7', NULL, COUNT(*) FROM (
  SELECT i.i_item_id, avg(ss.ss_quantity), avg(ss.ss_list_price), avg(ss.ss_coupon_amt), avg(ss.ss_sales_price)
  FROM store_sales ss
  JOIN customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE cd.cd_gender = 'F' AND cd.cd_marital_status = 'S' AND d.d_year = 2023
  GROUP BY i.i_item_id ORDER BY i.i_item_id LIMIT 100
);
SELECT 'q13', NULL, COUNT(*) FROM (
  SELECT avg(ss.ss_quantity), avg(ss.ss_ext_sales_price), avg(ss.ss_ext_wholesale_cost), sum(ss.ss_ext_wholesale_cost)
  FROM store_sales ss
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
  JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE cd.cd_marital_status = 'M' AND d.d_year = 2022 AND ca.ca_state IN ('CA','NY','TX')
);
SELECT 'q19', NULL, COUNT(*) FROM (
  SELECT i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact, sum(ss.ss_ext_sales_price) as revenue
  FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE d.d_moy = 11 AND d.d_year = 2023 AND ca.ca_state != s.s_state
  GROUP BY i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact
  ORDER BY revenue DESC LIMIT 100
);
SELECT 'q25', NULL, COUNT(*) FROM (
  SELECT i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name,
         sum(ss.ss_net_profit), sum(sr.sr_net_loss)
  FROM store_sales ss
  JOIN store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
       AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number
  JOIN catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
       AND sr.sr_item_sk = cs.cs_item_sk
  JOIN date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
  JOIN date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  WHERE d1.d_year = 2022
  GROUP BY i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name
  ORDER BY i.i_item_id LIMIT 100
);
SELECT 'q43', NULL, COUNT(*) FROM (
  SELECT s.s_store_name, s.s_store_id,
         sum(CASE WHEN d.d_dow = 0 THEN ss.ss_sales_price ELSE 0 END) as sun
  FROM store_sales ss
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE d.d_year = 2023
  GROUP BY s.s_store_name, s.s_store_id ORDER BY s.s_store_name LIMIT 100
);
SELECT 'q46', NULL, COUNT(*) FROM (
  SELECT c.c_last_name, c.c_first_name, ca.ca_city, sum(ss.ss_net_paid) as total_spend
  FROM store_sales ss
  JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
  JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  WHERE d.d_year IN (2022, 2023) AND hd.hd_dep_count >= 2
  GROUP BY c.c_last_name, c.c_first_name, ca.ca_city
  ORDER BY total_spend DESC LIMIT 100
);
SELECT 'q55', NULL, COUNT(*) FROM (
  SELECT i.i_brand_id, i.i_brand, sum(ss.ss_ext_sales_price)
  FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  WHERE d.d_moy = 6 AND d.d_year = 2023 AND i.i_manager_id = 15
  GROUP BY i.i_brand_id, i.i_brand
  ORDER BY 3 DESC LIMIT 100
);
SELECT 'q96', NULL, COUNT(*) FROM (
  SELECT count(*) FROM store_sales ss
  JOIN time_dim t ON ss.ss_sold_time_sk = t.t_time_sk
  JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  WHERE t.t_hour = 14 AND hd.hd_dep_count = 3
);
EOF

  # Run each query independently with time measurement so we can record
  # one row per query per iteration. Using a loop (not the consolidated
  # script above) for clean per-query timing.
  local query_names=(q1 q3 q7 q13 q19 q25 q43 q46 q55 q96)
  local query_sqls=(
    "SELECT count(*) FROM (SELECT c.c_customer_id, sum(sr.sr_return_amt) FROM store_returns sr JOIN customer c ON sr.sr_customer_sk=c.c_customer_sk JOIN store s ON sr.sr_store_sk=s.s_store_sk JOIN date_dim d ON sr.sr_returned_date_sk=d.d_date_sk WHERE d.d_year=2022 GROUP BY c.c_customer_id ORDER BY 2 DESC LIMIT 100);"
    "SELECT count(*) FROM (SELECT d.d_year, i.i_brand, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE i.i_category='Electronics' GROUP BY 1,2 ORDER BY 1,3 DESC LIMIT 100);"
    "SELECT count(*) FROM (SELECT i.i_item_id, avg(ss.ss_quantity) FROM store_sales ss JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN promotion p ON ss.ss_promo_sk=p.p_promo_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_gender='F' AND cd.cd_marital_status='S' AND d.d_year=2023 GROUP BY 1 LIMIT 100);"
    "SELECT count(*) FROM (SELECT avg(ss.ss_quantity) FROM store_sales ss JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN customer_demographics cd ON ss.ss_cdemo_sk=cd.cd_demo_sk JOIN customer_address ca ON ss.ss_addr_sk=ca.ca_address_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE cd.cd_marital_status='M' AND d.d_year=2022 AND ca.ca_state IN ('CA','NY','TX'));"
    "SELECT count(*) FROM (SELECT i.i_brand_id, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN customer c ON ss.ss_customer_sk=c.c_customer_sk JOIN customer_address ca ON c.c_current_addr_sk=ca.ca_address_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_moy=11 AND d.d_year=2023 AND ca.ca_state!=s.s_state GROUP BY 1 ORDER BY 2 DESC LIMIT 100);"
    "SELECT count(*) FROM (SELECT i.i_item_id, sum(ss.ss_net_profit), sum(sr.sr_net_loss) FROM store_sales ss JOIN store_returns sr ON ss.ss_customer_sk=sr.sr_customer_sk AND ss.ss_item_sk=sr.sr_item_sk AND ss.ss_ticket_number=sr.sr_ticket_number JOIN catalog_sales cs ON sr.sr_customer_sk=cs.cs_bill_customer_sk AND sr.sr_item_sk=cs.cs_item_sk JOIN date_dim d1 ON ss.ss_sold_date_sk=d1.d_date_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN item i ON ss.ss_item_sk=i.i_item_sk WHERE d1.d_year=2022 GROUP BY 1 ORDER BY 1 LIMIT 100);"
    "SELECT count(*) FROM (SELECT s.s_store_name, sum(CASE WHEN d.d_dow=0 THEN ss.ss_sales_price ELSE 0 END) FROM store_sales ss JOIN store s ON ss.ss_store_sk=s.s_store_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_year=2023 GROUP BY 1 ORDER BY 1 LIMIT 100);"
    "SELECT count(*) FROM (SELECT c.c_last_name, ca.ca_city, sum(ss.ss_net_paid) FROM store_sales ss JOIN customer c ON ss.ss_customer_sk=c.c_customer_sk JOIN customer_address ca ON c.c_current_addr_sk=ca.ca_address_sk JOIN household_demographics hd ON ss.ss_hdemo_sk=hd.hd_demo_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_year IN (2022,2023) AND hd.hd_dep_count>=2 GROUP BY 1,2 ORDER BY 3 DESC LIMIT 100);"
    "SELECT count(*) FROM (SELECT i.i_brand_id, sum(ss.ss_ext_sales_price) FROM store_sales ss JOIN item i ON ss.ss_item_sk=i.i_item_sk JOIN date_dim d ON ss.ss_sold_date_sk=d.d_date_sk WHERE d.d_moy=6 AND d.d_year=2023 AND i.i_manager_id=15 GROUP BY 1 ORDER BY 2 DESC LIMIT 100);"
    "SELECT count(*) FROM store_sales ss JOIN time_dim t ON ss.ss_sold_time_sk=t.t_time_sk JOIN household_demographics hd ON ss.ss_hdemo_sk=hd.hd_demo_sk JOIN store s ON ss.ss_store_sk=s.s_store_sk WHERE t.t_hour=14 AND hd.hd_dep_count=3;"
  )

  # Build a setup-only file (views, no queries). Use the SET form for
  # S3 config rather than CREATE SECRET because iceberg_scan in some
  # duckdb versions doesn't pick up secrets reliably; the SET form is
  # consistently honored by both httpfs and the iceberg extension.
  local setup_file
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
  for table in store_sales store_returns catalog_sales item customer customer_address customer_demographics household_demographics date_dim time_dim store promotion; do
    echo "CREATE OR REPLACE VIEW ${table} AS SELECT * FROM iceberg_scan('${warehouse_path}/${NAMESPACE}.db/${table}');" >> "$setup_file"
  done

  # File counts come from the janitor's discover output. Wrap each in
  # set +e because a transient failure (mid-commit catalog read,
  # network blip on s3) shouldn't crash the bench loop.
  local files_ss files_sr files_cs
  set +e
  files_ss=$(JANITOR_WAREHOUSE_URL="$warehouse_url" S3_ENDPOINT="${S3_ENDPOINT:-}" S3_REGION="${S3_REGION:-${AWS_REGION:-us-east-1}}" /tmp/janitor-cli analyze "${NAMESPACE}.db/store_sales" 2>/dev/null | awk '/^Data files:/{print $3}')
  files_sr=$(JANITOR_WAREHOUSE_URL="$warehouse_url" S3_ENDPOINT="${S3_ENDPOINT:-}" S3_REGION="${S3_REGION:-${AWS_REGION:-us-east-1}}" /tmp/janitor-cli analyze "${NAMESPACE}.db/store_returns" 2>/dev/null | awk '/^Data files:/{print $3}')
  files_cs=$(JANITOR_WAREHOUSE_URL="$warehouse_url" S3_ENDPOINT="${S3_ENDPOINT:-}" S3_REGION="${S3_REGION:-${AWS_REGION:-us-east-1}}" /tmp/janitor-cli analyze "${NAMESPACE}.db/catalog_sales" 2>/dev/null | awk '/^Data files:/{print $3}')
  set -e
  [[ -z "$files_ss" ]] && files_ss="-1"
  [[ -z "$files_sr" ]] && files_sr="-1"
  [[ -z "$files_cs" ]] && files_cs="-1"

  for i in "${!query_names[@]}"; do
    local qname="${query_names[$i]}"
    local qsql="${query_sqls[$i]}"
    local query_file
    query_file=$(mktemp)
    cat "$setup_file" > "$query_file"
    echo "$qsql" >> "$query_file"

    local start_ns end_ns elapsed_ms
    start_ns=$(python3 -c 'import time; print(int(time.monotonic_ns()))' 2>/dev/null || gdate +%s%N)
    # Use -csv -noheader so we get just the count value, no box drawing.
    # Wrap in set +e because a query failure (transient catalog read race
    # under concurrent writes) shouldn't kill the bench loop.
    local row_count
    set +e
    # Capture stderr to a debug log so the first time a query fails we
    # can diagnose without re-running. The success path is silent.
    row_count=$(duckdb -csv -noheader < "$query_file" 2>>"$RESULTS_DIR/duckdb-errors-$TS.log" | tail -1 | tr -d '\r" ')
    set -e
    end_ns=$(python3 -c 'import time; print(int(time.monotonic_ns()))' 2>/dev/null || gdate +%s%N)
    elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    [[ -z "$row_count" ]] && row_count="-1"

    echo "$iteration,$(date -u +%FT%TZ),$label,$qname,$elapsed_ms,$row_count,$files_ss,$files_sr,$files_cs" >> "$RESULTS_CSV"
    rm -f "$query_file"
  done
  rm -f "$setup_file" "$sql_file"
}

run_janitor_compact() {
  local warehouse_url="$1"
  log "running janitor compact on with-janitor warehouse"
  for table in store_sales store_returns catalog_sales; do
    JANITOR_WAREHOUSE_URL="$warehouse_url" /tmp/janitor-cli compact "${NAMESPACE}.db/${table}" \
      >> "$JANITOR_LOG" 2>&1 || log "  warning: compact ${table} returned non-zero (this is OK if the streamer was mid-commit)"
  done
}

# === Phase 3: main loop ===

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
    log "[t+${ELAPSED}s] iteration $ITERATION — querying both warehouses"
    run_duckdb_queries "with"    "$WH_WITH_URL"    "$ITERATION"
    run_duckdb_queries "without" "$WH_WITHOUT_URL" "$ITERATION"
    NEXT_QUERY=$((NOW + QUERY_INTERVAL_SECONDS))
  fi

  if (( NOW >= NEXT_MAINTENANCE )); then
    run_janitor_compact "$WH_WITH_URL"
    NEXT_MAINTENANCE=$((NOW + MAINTENANCE_INTERVAL_SECONDS))
  fi

  sleep 2
done

# === Phase 4: comparison report ===

log "generating comparison report"

duckdb <<SQL > "$SUMMARY_TXT" 2>&1
.mode markdown
SELECT 'TPC-DS streaming benchmark — bench-results-$TS.csv' AS title;
SELECT '' AS sep;
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
SELECT '' AS sep;
SELECT
  mode,
  ROUND(AVG(store_sales_files), 1)   AS avg_ss_files,
  ROUND(AVG(store_returns_files), 1) AS avg_sr_files,
  ROUND(AVG(catalog_sales_files), 1) AS avg_cs_files
FROM read_csv_auto('$RESULTS_CSV', header=true)
GROUP BY mode;
SQL

log "summary written to $SUMMARY_TXT"
log "raw results in $RESULTS_CSV"
log "==== summary ===="
cat "$SUMMARY_TXT"
log "================="
