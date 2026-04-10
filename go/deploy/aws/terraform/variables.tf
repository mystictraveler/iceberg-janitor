variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project" {
  description = "Project name, used as prefix for all resources"
  type        = string
  default     = "iceberg-janitor"
}

variable "bench_instance_type" {
  description = "EC2 instance type for the bench runner"
  type        = string
  default     = "m5.xlarge"
}

variable "bench_key_name" {
  description = "EC2 key pair name for SSH access (optional, SSM is available regardless)"
  type        = string
  default     = ""
}

variable "fargate_cpu" {
  description = "Fargate task CPU units (1024 = 1 vCPU)"
  type        = number
  default     = 1024
}

variable "fargate_memory" {
  description = "Fargate task memory in MiB"
  type        = number
  default     = 4096
}

variable "server_image_tag" {
  description = "Container image tag for janitor-server (after pushing to ECR)"
  type        = string
  default     = "latest"
}

variable "warehouse_bucket_name" {
  description = "S3 bucket name for the iceberg warehouse. Auto-generated if empty."
  type        = string
  default     = ""
}
