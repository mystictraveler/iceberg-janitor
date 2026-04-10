output "ecr_repository_url" {
  description = "ECR repository URL — push images here"
  value       = aws_ecr_repository.janitor.repository_url
}

output "api_gateway_url" {
  description = "Private API Gateway invoke URL (only reachable from within the VPC)"
  value       = "https://${aws_api_gateway_rest_api.janitor.id}.execute-api.${var.region}.amazonaws.com/${aws_api_gateway_stage.main.stage_name}"
}

output "warehouse_with_bucket" {
  description = "S3 bucket for the with-janitor warehouse"
  value       = aws_s3_bucket.warehouse_with.id
}

output "warehouse_without_bucket" {
  description = "S3 bucket for the without-janitor warehouse"
  value       = aws_s3_bucket.warehouse_without.id
}

output "warehouse_with_url" {
  description = "gocloud.dev/blob URL for the with-janitor warehouse"
  value       = "s3://${aws_s3_bucket.warehouse_with.id}?region=${var.region}"
}

output "warehouse_without_url" {
  description = "gocloud.dev/blob URL for the without-janitor warehouse"
  value       = "s3://${aws_s3_bucket.warehouse_without.id}?region=${var.region}"
}

output "bench_instance_id" {
  description = "EC2 instance ID — use 'aws ssm start-session --target <id>' to connect"
  value       = aws_instance.bench.id
}

output "bench_public_ip" {
  description = "EC2 public IP (for SSH if key_name was provided)"
  value       = aws_instance.bench.public_ip
}

output "ecs_cluster" {
  description = "ECS cluster name"
  value       = aws_ecs_cluster.main.name
}

output "ecs_service" {
  description = "ECS service name"
  value       = aws_ecs_service.janitor_server.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

output "athena_results_bucket" {
  description = "S3 bucket for Athena query results"
  value       = aws_s3_bucket.athena_results.id
}

output "glue_database_with" {
  description = "Glue database for with-janitor tables"
  value       = aws_glue_catalog_database.with_janitor.name
}

output "glue_database_without" {
  description = "Glue database for without-janitor tables"
  value       = aws_glue_catalog_database.without_janitor.name
}

output "deploy_commands" {
  description = "Quick-start commands to build, push, and connect"
  value       = <<-EOT
    # 1. Build + push image to ECR:
    aws ecr get-login-password --region ${var.region} | docker login --username AWS --password-stdin ${aws_ecr_repository.janitor.repository_url}
    docker build -t ${aws_ecr_repository.janitor.repository_url}:latest ./go/
    docker push ${aws_ecr_repository.janitor.repository_url}:latest

    # 2. Force new Fargate deployment (picks up :latest):
    aws ecs update-service --cluster ${aws_ecs_cluster.main.name} --service ${aws_ecs_service.janitor_server.name} --force-new-deployment --region ${var.region}

    # 3. Connect to bench EC2 via SSM:
    aws ssm start-session --target ${aws_instance.bench.id} --region ${var.region}

    # 4. On the EC2, seed + bench:
    source /etc/profile.d/janitor-bench.sh
    janitor-streamer --warehouse-url $JANITOR_WAREHOUSE_URL --duration 300s --commits-per-minute 60
    janitor-cli compact $JANITOR_WAREHOUSE_URL
  EOT
}
