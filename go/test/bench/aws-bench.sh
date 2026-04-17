#!/usr/bin/env bash
# aws-bench.sh — orchestrates a full TPC-DS bench on the AWS sandbox.
#
# Handles the sandbox networking workarounds automatically:
#   - Direct-IP server bypass (bench→server private IP, not NLB)
#   - External Glue registration at each phase transition
#   - All tasks pinned to subnet-045dd14e688b740b1 (1a only)
#
# Usage:
#   ./aws-bench.sh                  # full pipeline: build+push, rollout, wipe, bench, collect
#   ./aws-bench.sh --no-build       # skip image build (use existing :latest + :bench)
#   ./aws-bench.sh --no-rollout     # skip ECS service rollout
#   ./aws-bench.sh --no-wipe        # keep existing warehouse data
#
# Prerequisites:
#   - AWS profile "aws-sandbox" authenticated (run /aws-sandbox-connect first)
#   - Docker + buildx available
#   - aws CLI v2
#
# Wall time: ~15-20 min (build 5m + rollout 2m + stream 5m + maintain 3m + query 5m)
# Without build: ~12-15 min

set -euo pipefail

PROFILE="aws-sandbox"
REGION="us-east-1"
CLUSTER="iceberg-janitor"
SERVICE="iceberg-janitor-server"
BENCH_TASK_DEF="iceberg-janitor-bench"
SUBNET="subnet-045dd14e688b740b1"
SG="sg-09d4159ce0280fa20"
ECR="605618833247.dkr.ecr.us-east-1.amazonaws.com/iceberg-janitor"
WITH_BUCKET="iceberg-janitor-605618833247-with"
WITHOUT_BUCKET="iceberg-janitor-605618833247-without"
GLUE_WITH="iceberg_janitor_with"
GLUE_WITHOUT="iceberg_janitor_without"
RESULTS_BUCKET="iceberg-janitor-athena-results-605618833247"
BENCH_LOG="/ecs/iceberg-janitor-bench"
SERVER_LOG="/ecs/iceberg-janitor"
TABLES="store_sales store_returns catalog_sales"

DO_BUILD=true
DO_ROLLOUT=true
DO_WIPE=true

for arg in "$@"; do
  case "$arg" in
    --no-build)   DO_BUILD=false ;;
    --no-rollout) DO_ROLLOUT=false ;;
    --no-wipe)    DO_WIPE=false ;;
    *) echo "unknown arg: $arg" >&2; exit 2 ;;
  esac
done

log() { printf '\033[1;36m[aws-bench]\033[0m %s\n' "$*"; }
aws_() { aws --profile "$PROFILE" --region "$REGION" "$@"; }

# === Step 0: verify creds ===
log "verifying AWS credentials"
ACCOUNT=$(aws_ sts get-caller-identity --query Account --output text 2>&1) || {
  echo "AWS credentials expired or missing. Run: /aws-sandbox-connect" >&2; exit 1
}
log "account: $ACCOUNT"

# === Step 1: build + push ===
if $DO_BUILD; then
  log "building + pushing images"
  cd "$(dirname "$0")/../../"
  aws_ ecr get-login-password | docker login --username AWS --password-stdin "$ECR" >/dev/null 2>&1
  SHA=$(git rev-parse --short=12 HEAD)
  log "  server image → ${ECR}:latest (SHA ${SHA})"
  docker buildx build --platform linux/amd64 --target server \
    -t "${ECR}:latest" -t "${ECR}:${SHA}" --push . >/dev/null 2>&1
  log "  bench image → ${ECR}:bench"
  docker buildx build --platform linux/amd64 --target bench \
    -t "${ECR}:bench" -t "${ECR}:bench-${SHA}" --push . >/dev/null 2>&1
  log "  images pushed"
else
  log "skipping build (--no-build)"
fi

# === Step 2: rollout ===
if $DO_ROLLOUT; then
  log "forcing ECS service rollout"
  aws_ ecs update-service --cluster "$CLUSTER" --service "$SERVICE" \
    --force-new-deployment >/dev/null 2>&1
  log "  waiting for rollout to stabilize..."
  aws_ ecs wait services-stable --cluster "$CLUSTER" --services "$SERVICE"
  log "  rollout stable"
else
  log "skipping rollout (--no-rollout)"
fi

# === Step 3: wipe warehouses ===
if $DO_WIPE; then
  log "wiping both warehouses"
  aws_ s3 rm "s3://$WITH_BUCKET" --recursive --quiet &
  aws_ s3 rm "s3://$WITHOUT_BUCKET" --recursive --quiet &
  wait
  log "  warehouses clean"
else
  log "skipping wipe (--no-wipe)"
fi

# === Step 4: get server private IP ===
log "getting server task private IP"
TASK_ARN=$(aws_ ecs list-tasks --cluster "$CLUSTER" --service-name "$SERVICE" \
  --query 'taskArns[0]' --output text)
SERVER_IP=$(aws_ ecs describe-tasks --cluster "$CLUSTER" --tasks "$TASK_ARN" \
  --query 'tasks[0].attachments[0].details[?name==`privateIPv4Address`].value' --output text)
log "  server IP: $SERVER_IP (direct bypass, same subnet)"

# === Step 5: launch bench task ===
log "launching bench task (direct-IP, pinned to 1a)"
BENCH_ARN=$(aws_ ecs run-task \
  --cluster "$CLUSTER" \
  --task-definition "$BENCH_TASK_DEF" \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[$SUBNET],securityGroups=[$SG],assignPublicIp=DISABLED}" \
  --overrides "{\"containerOverrides\":[{\"name\":\"bench\",\"environment\":[{\"name\":\"JANITOR_SERVER_URL\",\"value\":\"http://${SERVER_IP}:8080\"}]}]}" \
  --query 'tasks[0].taskArn' --output text)
log "  bench task: $BENCH_ARN"

# === Helper: register Glue tables from here ===
register_glue() {
  local label="$1"
  log "registering Glue tables ($label)"
  for wh in with without; do
    local bucket="iceberg-janitor-605618833247-$wh"
    local db="iceberg_janitor_$wh"
    for table in $TABLES; do
      local meta
      meta=$(aws_ s3 ls "s3://$bucket/tpcds.db/$table/metadata/" 2>/dev/null \
        | grep 'metadata.json$' | sort -k1,2 | tail -1 | awk '{print $NF}')
      if [ -n "$meta" ]; then
        local loc="s3://$bucket/tpcds.db/$table/metadata/$meta"
        aws_ glue update-table --database-name "$db" \
          --table-input "{\"Name\":\"$table\",\"StorageDescriptor\":{\"Location\":\"s3://$bucket/tpcds.db/$table\",\"InputFormat\":\"org.apache.hadoop.mapred.FileInputFormat\",\"OutputFormat\":\"org.apache.hadoop.mapred.FileOutputFormat\",\"SerdeInfo\":{\"SerializationLibrary\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"}},\"TableType\":\"EXTERNAL_TABLE\",\"Parameters\":{\"table_type\":\"ICEBERG\",\"metadata_location\":\"$loc\"}}" 2>/dev/null || true
        log "  $wh/$table → $meta"
      fi
    done
  done
}

# === Step 6: monitor + interleave Glue registration ===
log "monitoring bench (polling every 30s, registering Glue at phase transitions)"
STREAM_DONE=false
MAINTAIN_DONE=false

while true; do
  STATUS=$(aws_ ecs describe-tasks --cluster "$CLUSTER" --tasks "$BENCH_ARN" \
    --query 'tasks[0].lastStatus' --output text 2>/dev/null || echo "UNKNOWN")

  if [ "$STATUS" = "STOPPED" ]; then
    EXIT=$(aws_ ecs describe-tasks --cluster "$CLUSTER" --tasks "$BENCH_ARN" \
      --query 'tasks[0].containers[0].exitCode' --output text)
    log "bench STOPPED (exit $EXIT)"
    break
  fi

  # Check latest bench log line for phase transitions
  LATEST=$(aws_ logs tail "$BENCH_LOG" --since 10m --format short 2>/dev/null \
    | grep 'bench:aws' | tail -1 || true)

  if ! $STREAM_DONE && echo "$LATEST" | grep -q "stream phase done\|pause phase"; then
    STREAM_DONE=true
    register_glue "post-stream"
  fi

  if ! $MAINTAIN_DONE && echo "$LATEST" | grep -q "maintain phase done\|query phase"; then
    MAINTAIN_DONE=true
    register_glue "post-maintain"
  fi

  # Brief status
  PHASE=$(echo "$LATEST" | sed 's/.*\[bench:aws\] //' | head -c 60)
  log "  $STATUS | $PHASE"

  sleep 30
done

# Final Glue registration (post-compact snapshot)
register_glue "final"

# === Step 7: collect results ===
log "collecting results"
sleep 5  # S3 eventual consistency
TS=$(aws_ s3 ls "s3://$RESULTS_BUCKET/bench-runs/" 2>/dev/null \
  | awk '{print $2}' | sort | tail -1)
if [ -n "$TS" ]; then
  SUMMARY_KEY="bench-runs/${TS}bench-summary-${TS%/}.txt"
  aws_ s3 cp "s3://$RESULTS_BUCKET/$SUMMARY_KEY" /tmp/aws-bench-summary.txt 2>/dev/null || true
  log "=== BENCH SUMMARY ==="
  cat /tmp/aws-bench-summary.txt 2>/dev/null || log "(summary not found on S3)"
  log "====================="
fi

# === Step 8: check new observability fields ===
log "checking server logs for new obs fields"
QID=$(aws_ logs start-query --log-group-name "$SERVER_LOG" \
  --start-time $(( $(date +%s) - 7200 )) --end-time $(( $(date +%s) )) \
  --query-string 'fields @timestamp, msg, table, master_overall, skipped, dvs_applied, attempts, before_files, after_files | filter msg = "compact job completed" | sort @timestamp desc | limit 5' \
  --output text 2>/dev/null || echo "")
if [ -n "$QID" ]; then
  sleep 8
  MATCHED=$(aws_ logs get-query-results --query-id "$QID" \
    --query 'statistics.recordsMatched' --output text 2>/dev/null || echo 0)
  log "  compact-completed events with obs fields: $MATCHED"
else
  log "  (could not query server logs)"
fi

log "done. Bench task: $BENCH_ARN"
