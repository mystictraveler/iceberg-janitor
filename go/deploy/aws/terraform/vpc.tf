# Use the existing default VPC — sandbox has a VPC limit.
# Public subnets (existing) → EC2 bench runner.
# Private subnets (new) → Fargate. NAT gateway for ECR image pulls.
# S3 gateway endpoint keeps warehouse traffic off the internet.

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "public" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

locals {
  vpc_id          = data.aws_vpc.default.id
  vpc_cidr        = data.aws_vpc.default.cidr_block
  public_subnets  = slice(sort(data.aws_subnets.public.ids), 0, 2)
  private_subnets = aws_subnet.private[*].id
}

# --- Private subnets for Fargate (172.31.96.0/20 and 172.31.112.0/20) ---

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = local.vpc_id
  cidr_block        = cidrsubnet("172.31.96.0/19", 1, count.index) # .96.0/20, .112.0/20
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = { Name = "${var.project}-private-${count.index}" }
}

# --- NAT gateway so Fargate can pull images from ECR ---

resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = { Name = "${var.project}-nat-eip" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = local.public_subnets[0]
  tags          = { Name = "${var.project}-nat" }
}

resource "aws_route_table" "private" {
  vpc_id = local.vpc_id
  tags   = { Name = "${var.project}-private-rt" }
}

resource "aws_route" "private_nat" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

# --- S3 gateway endpoint (free, keeps warehouse traffic in AWS backbone) ---

data "aws_route_tables" "all" {
  vpc_id = local.vpc_id
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id       = local.vpc_id
  service_name = "com.amazonaws.${var.region}.s3"

  route_table_ids = setunion(data.aws_route_tables.all.ids, [aws_route_table.private.id])

  tags = { Name = "${var.project}-s3-endpoint" }
}

# --- Security groups ---

resource "aws_security_group" "fargate" {
  name_prefix = "${var.project}-fargate-"
  vpc_id      = local.vpc_id
  description = "Fargate janitor-server"

  ingress {
    description = "From NLB (health checks + traffic)"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-fargate-sg" }
}

resource "aws_security_group" "bench" {
  name_prefix = "${var.project}-bench-"
  vpc_id      = local.vpc_id
  description = "EC2 bench runner"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-bench-sg" }
}

# --- VPC endpoints for SSM (shell access to EC2 without SSH) ---

resource "aws_security_group" "vpc_endpoints" {
  name_prefix = "${var.project}-vpce-"
  vpc_id      = local.vpc_id
  description = "Interface VPC endpoints"

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [local.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-vpce-sg" }
}

# --- VPC endpoints for ECR (Fargate image pulls from private subnets) ---

resource "aws_vpc_endpoint" "ecr_api" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.ecr.api"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-ecr-api-endpoint" }
}

resource "aws_vpc_endpoint" "ecr_dkr" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.ecr.dkr"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-ecr-dkr-endpoint" }
}

# --- VPC endpoints for CloudWatch Logs (Fargate log driver) ---

resource "aws_vpc_endpoint" "logs" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.logs"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-logs-endpoint" }
}

resource "aws_vpc_endpoint" "ssm" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.ssm"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.public_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-ssm-endpoint" }
}

resource "aws_vpc_endpoint" "ssm_messages" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.ssmmessages"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.public_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-ssmmessages-endpoint" }
}

resource "aws_vpc_endpoint" "ec2_messages" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.ec2messages"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.public_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-ec2messages-endpoint" }
}
