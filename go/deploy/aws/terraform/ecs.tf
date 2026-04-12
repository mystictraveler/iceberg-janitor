resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "janitor" {
  name              = "/ecs/${var.project}"
  retention_in_days = 90
}

resource "aws_ecs_task_definition" "janitor_server" {
  family                   = "${var.project}-server"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.fargate_cpu
  memory                   = var.fargate_memory
  execution_role_arn       = aws_iam_role.ecs_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name  = "janitor-server"
    image = "${aws_ecr_repository.janitor.repository_url}:${var.server_image_tag}"

    portMappings = [{
      containerPort = 8080
      protocol      = "tcp"
    }]

    environment = [
      {
        name  = "JANITOR_WAREHOUSE_URL"
        value = "s3://${aws_s3_bucket.warehouse_with.id}?region=${var.region}"
      },
      {
        name  = "AWS_REGION"
        value = var.region
      },
      {
        name  = "JANITOR_PARTITION_CONCURRENCY"
        value = "16"
      },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.janitor.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "server"
      }
    }
    # No container-level healthCheck — distroless image has no wget/curl
    # to run the CMD-SHELL probe. The NLB target group already does an
    # HTTP probe against /v1/healthz on the same port.
  }])
}

# Cloud Map for service discovery — API Gateway VPC link routes here.
resource "aws_service_discovery_private_dns_namespace" "main" {
  name = "${var.project}.local"
  vpc  = local.vpc_id
}

resource "aws_service_discovery_service" "janitor_server" {
  name = "server"

  dns_config {
    namespace_id = aws_service_discovery_private_dns_namespace.main.id

    dns_records {
      ttl  = 10
      type = "A"
    }

    routing_policy = "MULTIVALUE"
  }

  health_check_custom_config {
    failure_threshold = 1
  }
}

resource "aws_ecs_service" "janitor_server" {
  name            = "${var.project}-server"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.janitor_server.arn
  # 3 replicas exercises the lease + persistent jobrecord
  # cross-replica path. The in-process inflight guard alone is
  # insufficient for multi-replica deployments — it dedupes
  # within one process, not across the fleet. With desired_count
  # = 3, concurrent client POSTs to different replicas land on
  # the lease primitive (pkg/lease), which uses S3
  # If-None-Match: * for cross-process atomicity. See
  # _janitor/state/leases/ for the live lease files and
  # _janitor/state/jobs/ for the persistent job records.
  desired_count = 3
  launch_type   = "FARGATE"

  network_configuration {
    # Pinned to 1a only: cross-AZ traffic to 1a from 1b was silently dropped
    # in this sandbox VPC (NACL or routing — not a config we control).
    # Keeping the server single-AZ avoids the bench-vs-server AZ mismatch
    # that caused bench runs 2+ to fail with 131 s TCP SYN timeouts.
    subnets          = [aws_subnet.private[0].id]
    security_groups  = [aws_security_group.fargate.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.fargate.arn
    container_name   = "janitor-server"
    container_port   = 8080
  }

  service_registries {
    registry_arn = aws_service_discovery_service.janitor_server.arn
  }
}
