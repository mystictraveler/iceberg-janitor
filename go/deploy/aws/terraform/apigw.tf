# Private REST API Gateway — accessible ONLY from within the VPC.
# Uses a VPC link to route to Fargate (via Cloud Map NLB-less integration
# isn't available on REST, so we front Fargate with an internal NLB).

# --- Internal NLB (bridges API Gateway VPC link → Fargate) ---

resource "aws_lb" "internal" {
  name               = "${var.project}-internal"
  internal           = true
  load_balancer_type = "network"
  subnets            = local.private_subnets

  tags = { Name = "${var.project}-internal-nlb" }
}

resource "aws_lb_target_group" "fargate" {
  name        = "${var.project}-fargate"
  port        = 8080
  protocol    = "TCP"
  vpc_id      = local.vpc_id
  target_type = "ip"

  health_check {
    protocol            = "HTTP"
    path                = "/v1/healthz"
    port                = "8080"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    interval            = 15
  }

  tags = { Name = "${var.project}-fargate-tg" }
}

resource "aws_lb_listener" "fargate" {
  load_balancer_arn = aws_lb.internal.arn
  port              = 80
  protocol          = "TCP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.fargate.arn
  }
}

# --- REST API Gateway (PRIVATE) ---

resource "aws_api_gateway_rest_api" "janitor" {
  name = "${var.project}-api"

  endpoint_configuration {
    types            = ["PRIVATE"]
    vpc_endpoint_ids = [data.aws_vpc_endpoint.execute_api.id]
  }

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Deny"
        Principal = "*"
        Action    = "execute-api:Invoke"
        Resource  = "execute-api:/*"
        Condition = {
          StringNotEquals = {
            "aws:sourceVpce" = data.aws_vpc_endpoint.execute_api.id
          }
        }
      },
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "execute-api:Invoke"
        Resource  = "execute-api:/*"
      },
    ]
  })
}

# VPC link for the REST API → internal NLB
resource "aws_api_gateway_vpc_link" "fargate" {
  name        = "${var.project}-vpclink"
  target_arns = [aws_lb.internal.arn]
}

# Proxy resource: catch all paths and methods → Fargate
resource "aws_api_gateway_resource" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.janitor.id
  parent_id   = aws_api_gateway_rest_api.janitor.root_resource_id
  path_part   = "{proxy+}"
}

resource "aws_api_gateway_method" "proxy" {
  rest_api_id   = aws_api_gateway_rest_api.janitor.id
  resource_id   = aws_api_gateway_resource.proxy.id
  http_method   = "ANY"
  authorization = "AWS_IAM"

  request_parameters = {
    "method.request.path.proxy" = true
  }
}

resource "aws_api_gateway_integration" "proxy" {
  rest_api_id = aws_api_gateway_rest_api.janitor.id
  resource_id = aws_api_gateway_resource.proxy.id
  http_method = aws_api_gateway_method.proxy.http_method

  type                    = "HTTP_PROXY"
  integration_http_method = "ANY"
  uri                     = "http://${aws_lb.internal.dns_name}/{proxy}"
  connection_type         = "VPC_LINK"
  connection_id           = aws_api_gateway_vpc_link.fargate.id

  request_parameters = {
    "integration.request.path.proxy" = "method.request.path.proxy"
  }
}

resource "aws_api_gateway_deployment" "main" {
  rest_api_id = aws_api_gateway_rest_api.janitor.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.proxy.id,
      aws_api_gateway_method.proxy.id,
      aws_api_gateway_integration.proxy.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }
}

resource "aws_api_gateway_stage" "main" {
  rest_api_id   = aws_api_gateway_rest_api.janitor.id
  deployment_id = aws_api_gateway_deployment.main.id
  stage_name    = "v1"

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.apigw.arn
    format = jsonencode({
      requestId      = "$context.requestId"
      ip             = "$context.identity.sourceIp"
      httpMethod     = "$context.httpMethod"
      path           = "$context.path"
      status         = "$context.status"
      responseLength = "$context.responseLength"
      latency        = "$context.responseLatency"
    })
  }
}

resource "aws_cloudwatch_log_group" "apigw" {
  name              = "/apigateway/${var.project}"
  retention_in_days = 90
}

# --- VPC endpoint for execute-api (reuse existing in default VPC) ---

data "aws_vpc_endpoint" "execute_api" {
  vpc_id       = local.vpc_id
  service_name = "com.amazonaws.${var.region}.execute-api"
}
