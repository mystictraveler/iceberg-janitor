resource "aws_ecs_cluster" "main" {
  name = var.project

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_cloudwatch_log_group" "janitor" {
  name              = "/ecs/${var.project}"
  retention_in_days = 14
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
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.janitor.name
        "awslogs-region"        = var.region
        "awslogs-stream-prefix" = "server"
      }
    }

    healthCheck = {
      command     = ["CMD-SHELL", "wget -q -O /dev/null http://localhost:8080/v1/healthz || exit 1"]
      interval    = 15
      timeout     = 5
      retries     = 3
      startPeriod = 10
    }
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
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.private_subnets
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
