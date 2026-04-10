# EC2 bench runner — sits in the public subnet, runs janitor-streamer
# to seed data into S3, then runs the bench script which calls
# janitor-server through the private API Gateway.

data "aws_ssm_parameter" "al2023_ami" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "aws_instance" "bench" {
  ami                    = data.aws_ssm_parameter.al2023_ami.value
  instance_type          = var.bench_instance_type
  subnet_id              = local.public_subnets[0]
  vpc_security_group_ids = [aws_security_group.bench.id]
  iam_instance_profile   = aws_iam_instance_profile.bench.name
  key_name               = var.bench_key_name != "" ? var.bench_key_name : null

  root_block_device {
    volume_size = 100
    volume_type = "gp3"
  }

  user_data = base64encode(templatefile("${path.module}/bench-userdata.sh.tpl", {
    region         = var.region
    go_version     = "1.25.0"
    repo_url       = "https://github.com/mystictraveler/iceberg-janitor.git"
    repo_branch    = "feature/go-rewrite-mvp"
    warehouse_url  = "s3://${aws_s3_bucket.warehouse_with.id}?region=${var.region}"
    api_gateway_url = "https://${aws_api_gateway_rest_api.janitor.id}.execute-api.${var.region}.amazonaws.com/${aws_api_gateway_stage.main.stage_name}"
    bucket_name    = aws_s3_bucket.warehouse_with.id
  }))

  tags = { Name = "${var.project}-bench" }
}
