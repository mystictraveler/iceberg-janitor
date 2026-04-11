# ECS task definition for running the Athena bench as a Fargate task.
# Kicked off via: aws ecs run-task --cluster iceberg-janitor --task-definition iceberg-janitor-bench ...

resource "aws_ecs_task_definition" "bench" {
  family                   = "${var.project}-bench"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 2048  # 2 vCPU — Athena does the heavy lifting, not the container
  memory                   = 4096  # 4 GB
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "bench"
    image = "${aws_ecr_repository.janitor.repository_url}:bench"

    environment = [
      {
        name  = "WH_WITH_URL"
        value = "s3://${aws_s3_bucket.warehouse_with.id}"
      },
      {
        name  = "WH_WITHOUT_URL"
        value = "s3://${aws_s3_bucket.warehouse_without.id}"
      },
      {
        name  = "AWS_REGION"
        value = var.region
      },
      # Bench reaches the server via the internal NLB (port 80 → 8080 on
      # Fargate). Cloud Map registration was flaky after force-new-
      # deployment (stale A record) so we bypass it. The NLB is in the
      # same private subnets as the bench, no API Gateway hop, direct TCP.
      {
        name  = "JANITOR_SERVER_URL"
        value = "http://${aws_lb.internal.dns_name}"
      },
      {
        name  = "ATHENA_WORKGROUP"
        value = aws_athena_workgroup.main.name
      },
      {
        name  = "GLUE_DB_WITH"
        value = aws_glue_catalog_database.with_janitor.name
      },
      {
        name  = "GLUE_DB_WITHOUT"
        value = aws_glue_catalog_database.without_janitor.name
      },
      {
        name  = "ATHENA_RESULTS_BUCKET"
        value = aws_s3_bucket.athena_results.id
      },
      # Phased bench: STREAM -> GLUE -> PAUSE -> MAINTAIN -> GLUE -> QUERY.
      # Stream for 20 min to give the classifier's 15-min short window
      # enough activity to pick streaming (which triggers the hot path),
      # and to produce enough manifests for rewrite-manifests and
      # compact_cold to have real work. The streamer is S3-PUT-bound at
      # ~40 commits/min, so CPM=1000 is aspirational — actual output is
      # ~800 commits per streamer over 20 min.
      {
        name  = "STREAM_DURATION_SECONDS"
        value = "1200"
      },
      {
        name  = "PAUSE_SECONDS"
        value = "15"
      },
      {
        name  = "MAINTAIN_ROUNDS"
        value = "2"
      },
      # 5 iterations with iter 1 dropped as warmup gives n=4 per query
      # for the A/B aggregates. Each iteration is ~100-150 s of Athena
      # wall time (20 queries × ~5-7 s), so query phase is ~10 min.
      {
        name  = "QUERY_ITERATIONS"
        value = "5"
      },
      {
        name  = "COMMITS_PER_MINUTE"
        value = "1000"
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.bench.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "bench"
      }
    }
  }])
}

resource "aws_cloudwatch_log_group" "bench" {
  name              = "/ecs/${var.project}-bench"
  retention_in_days = 14
}
