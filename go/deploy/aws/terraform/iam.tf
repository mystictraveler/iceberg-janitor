# --- ECS execution role (pull images, write logs) ---

resource "aws_iam_role" "ecs_execution" {
  name = "${var.project}-ecs-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_execution" {
  role       = aws_iam_role.ecs_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# --- ECS task role (what the janitor-server container can do) ---

resource "aws_iam_role" "ecs_task" {
  name = "${var.project}-ecs-task"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "ecs_task_s3" {
  name = "${var.project}-s3-access"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.warehouse_with.arn,
          "${aws_s3_bucket.warehouse_with.arn}/*",
          aws_s3_bucket.warehouse_without.arn,
          "${aws_s3_bucket.warehouse_without.arn}/*",
          "arn:aws:s3:::iceberg-janitor-warehouse-605618833247",
          "arn:aws:s3:::iceberg-janitor-warehouse-605618833247/*",
        ]
      }
    ]
  })
}

# --- EC2 bench instance profile ---

resource "aws_iam_role" "bench" {
  name = "${var.project}-bench"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

# SSM for shell access without SSH keys
resource "aws_iam_role_policy_attachment" "bench_ssm" {
  role       = aws_iam_role.bench.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# S3 access for the bench (streamer writes, CLI reads/writes, bench scripts)
resource "aws_iam_role_policy" "bench_s3" {
  name = "${var.project}-bench-s3"
  role = aws_iam_role.bench.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.warehouse_with.arn,
          "${aws_s3_bucket.warehouse_with.arn}/*",
          aws_s3_bucket.warehouse_without.arn,
          "${aws_s3_bucket.warehouse_without.arn}/*",
          "arn:aws:s3:::iceberg-janitor-warehouse-605618833247",
          "arn:aws:s3:::iceberg-janitor-warehouse-605618833247/*",
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "bench" {
  name = "${var.project}-bench"
  role = aws_iam_role.bench.name
}

# --- Operator policy for invoking the private API Gateway ---

resource "aws_iam_policy" "api_invoke" {
  name        = "${var.project}-api-invoke"
  description = "Allow invoking the iceberg-janitor private API Gateway"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "execute-api:Invoke"
      Resource = "${aws_api_gateway_rest_api.janitor.execution_arn}/*"
    }]
  })
}

# Attach to the bench EC2 role and ECS task role so both can call the API
resource "aws_iam_role_policy_attachment" "bench_api_invoke" {
  role       = aws_iam_role.bench.name
  policy_arn = aws_iam_policy.api_invoke.arn
}

resource "aws_iam_role_policy_attachment" "ecs_task_api_invoke" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.api_invoke.arn
}

# --- Athena + Glue permissions for the bench/ECS task roles ---

resource "aws_iam_role_policy" "bench_athena" {
  name = "${var.project}-bench-athena"
  role = aws_iam_role.bench.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = [aws_athena_workgroup.main.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.with_janitor.name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.without_janitor.name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.with_janitor.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.without_janitor.name}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*",
        ]
      },
    ]
  })
}

# Same Athena/Glue permissions for the ECS task role (bench Fargate task)
resource "aws_iam_role_policy" "ecs_task_athena" {
  name = "${var.project}-ecs-task-athena"
  role = aws_iam_role.ecs_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
        ]
        Resource = [aws_athena_workgroup.main.arn]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
        ]
        Resource = [
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.with_janitor.name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.without_janitor.name}",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.with_janitor.name}/*",
          "arn:aws:glue:${var.region}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.without_janitor.name}/*",
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*",
        ]
      },
    ]
  })
}
