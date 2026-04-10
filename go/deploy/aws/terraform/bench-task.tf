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
      # JANITOR_API_URL omitted — compact runs in-process from the bench
      # container (co-located with S3 in the same region, ~5ms latency).
      # API Gateway compact will be tested separately.
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
      {
        name  = "DURATION_SECONDS"
        value = "300"
      },
      {
        name  = "QUERY_INTERVAL_SECONDS"
        value = "30"
      },
      {
        name  = "MAINTENANCE_INTERVAL_SECONDS"
        value = "60"
      },
      {
        name  = "COMMITS_PER_MINUTE"
        value = "60"
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
