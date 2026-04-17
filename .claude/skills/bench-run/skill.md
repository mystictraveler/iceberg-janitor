---
name: bench-run
description: Run an iceberg-janitor TPC-DS benchmark. Single bench harness with three modes (local/minio/aws). Use when the user asks to run a bench, start a benchmark, or test compaction.
---

# Run iceberg-janitor TPC-DS Benchmark

## Mode selection

| User says | Mode | Wall time |
|---|---|---|
| "run the bench", "quick smoke test" | `local` | ~10 min |
| "run bench on MinIO", "test S3 path" | `minio` | ~10 min |
| "bench on AWS", "deploy and bench" | `aws` | ~30-40 min |

## Local / MinIO (simple — just run bench.sh)

```bash
cd /Users/jp/code/iceberg-janitor
bash go/test/bench/bench.sh local   # or: minio
# For V2 delete workload:
WORKLOAD=deletes bash go/test/bench/bench.sh minio
```

## AWS (one command — `aws-bench.sh` handles the workarounds)

```bash
cd /Users/jp/code/iceberg-janitor
bash go/test/bench/aws-bench.sh                 # full: build+push+rollout+wipe+bench+collect
bash go/test/bench/aws-bench.sh --no-build      # reuse existing images
bash go/test/bench/aws-bench.sh --no-rollout    # skip ECS rollout
bash go/test/bench/aws-bench.sh --no-wipe       # keep existing warehouse data
```

The script handles all sandbox networking workarounds automatically:
direct-IP server bypass, external Glue registration at each phase
transition, 1a subnet pinning, result collection from S3, and obs
field verification via Logs Insights. Wall time: ~15-20 min (with
build) or ~12-15 min (--no-build).

### Manual pipeline (if the script doesn't fit)

### Pipeline

1. `/aws-sandbox-connect` — ensure creds are fresh
2. **Build + push** both images (server `:latest` + bench `:bench`)
3. **Force rollout** of janitor-server ECS service
4. **Wipe warehouses** for a clean slate
5. **Get server private IP** — bypass the NLB (sandbox NACL blocks bench→NLB)
6. **Launch bench task** with `JANITOR_SERVER_URL=http://<IP>:8080` override, pinned to subnet `subnet-045dd14e688b740b1` (1a only)
7. **Register Glue tables from local** after each phase (post-stream, post-maintain) — Fargate can't reach Glue in this sandbox
8. **Collect results** from S3 bench-runs prefix + CloudWatch Logs Insights

### Key commands

**Build + push:**
```bash
cd /Users/jp/code/iceberg-janitor/go
ECR="605618833247.dkr.ecr.us-east-1.amazonaws.com/iceberg-janitor"
aws ecr get-login-password --profile aws-sandbox --region us-east-1 \
  | docker login --username AWS --password-stdin 605618833247.dkr.ecr.us-east-1.amazonaws.com
docker buildx build --platform linux/amd64 --target server -t "${ECR}:latest" --push .
docker buildx build --platform linux/amd64 --target bench -t "${ECR}:bench" --push .
```

**Get server IP + launch bench:**
```bash
TASK=$(aws ecs list-tasks --profile aws-sandbox --region us-east-1 --cluster iceberg-janitor --service-name iceberg-janitor-server --query 'taskArns[0]' --output text)
SERVER_IP=$(aws ecs describe-tasks --profile aws-sandbox --region us-east-1 --cluster iceberg-janitor --tasks "$TASK" --query 'tasks[0].attachments[0].details[?name==`privateIPv4Address`].value' --output text)
aws ecs run-task --profile aws-sandbox --region us-east-1 --cluster iceberg-janitor --task-definition iceberg-janitor-bench --launch-type FARGATE \
  --network-configuration 'awsvpcConfiguration={subnets=[subnet-045dd14e688b740b1],securityGroups=[sg-09d4159ce0280fa20],assignPublicIp=DISABLED}' \
  --overrides "{\"containerOverrides\":[{\"name\":\"bench\",\"environment\":[{\"name\":\"JANITOR_SERVER_URL\",\"value\":\"http://${SERVER_IP}:8080\"}]}]}"
```

**Register Glue (run after stream + after maintain):**
```bash
for wh in with without; do
  BUCKET="iceberg-janitor-605618833247-$wh"; DB="iceberg_janitor_$wh"
  for table in store_sales store_returns catalog_sales; do
    META=$(aws s3 ls "s3://$BUCKET/tpcds.db/$table/metadata/" --profile aws-sandbox --region us-east-1 | grep 'metadata.json$' | sort -k1,2 | tail -1 | awk '{print $NF}')
    aws glue update-table --profile aws-sandbox --region us-east-1 --database-name "$DB" \
      --table-input "{\"Name\":\"$table\",\"StorageDescriptor\":{\"Location\":\"s3://$BUCKET/tpcds.db/$table\",\"InputFormat\":\"org.apache.hadoop.mapred.FileInputFormat\",\"OutputFormat\":\"org.apache.hadoop.mapred.FileOutputFormat\",\"SerdeInfo\":{\"SerializationLibrary\":\"org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe\"}},\"TableType\":\"EXTERNAL_TABLE\",\"Parameters\":{\"table_type\":\"ICEBERG\",\"metadata_location\":\"s3://$BUCKET/tpcds.db/$table/metadata/$META\"}}"
  done
done
```

**Collect results:**
```bash
TS=$(aws s3 ls s3://iceberg-janitor-athena-results-605618833247/bench-runs/ --profile aws-sandbox --region us-east-1 | awk '{print $2}' | sort | tail -1)
aws s3 cp "s3://iceberg-janitor-athena-results-605618833247/bench-runs/${TS}bench-summary-${TS%/}.txt" /tmp/aws-bench-summary.txt --profile aws-sandbox --region us-east-1
cat /tmp/aws-bench-summary.txt
```

## AWS resources

| Resource | Value |
|---|---|
| ECS cluster | `iceberg-janitor` |
| Server service | `iceberg-janitor-server` (3 replicas) |
| Bench task-def | `iceberg-janitor-bench` (use latest revision) |
| Server logs | `/ecs/iceberg-janitor` |
| Bench logs | `/ecs/iceberg-janitor-bench` |
| Athena workgroup | `iceberg-janitor` |
| Glue DB (with) | `iceberg_janitor_with` |
| Glue DB (without) | `iceberg_janitor_without` |
| S3 with-warehouse | `iceberg-janitor-605618833247-with` |
| S3 without-warehouse | `iceberg-janitor-605618833247-without` |
| S3 bench results | `iceberg-janitor-athena-results-605618833247/bench-runs/` |
| Subnet (1a ONLY) | `subnet-045dd14e688b740b1` |
| Security group | `sg-09d4159ce0280fa20` |
| ECR | `605618833247.dkr.ecr.us-east-1.amazonaws.com/iceberg-janitor` |
| Dashboard | `iceberg-janitor-overview` |

## Env vars (all modes)

| Var | Default |
|---|---|
| `STREAM_DURATION_SECONDS` | 300 |
| `MAINTAIN_ROUNDS` | 2 |
| `QUERY_ITERATIONS` | 5 (iter 1 dropped as warmup) |
| `COMMITS_PER_MINUTE` | 60 |
| `WORKLOAD` | `tpcds` (or `deletes` for V2 bench) |

## Standing instructions

- MinIO TPC-DS bench is a **gating** ship-checklist item per `.github/pull_request_template.md`
- AWS bench: ALWAYS use direct-IP bypass + external Glue registration
- If creds expire mid-bench: re-run `/aws-sandbox-connect`, monitor picks up new creds automatically
