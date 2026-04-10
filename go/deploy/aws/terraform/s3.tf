locals {
  bucket_prefix = var.warehouse_bucket_name != "" ? var.warehouse_bucket_name : "${var.project}-${data.aws_caller_identity.current.account_id}"
}

# Two separate buckets — two lakehouses.
resource "aws_s3_bucket" "warehouse_with" {
  bucket = "${local.bucket_prefix}-with"
  tags   = { Name = "${var.project}-warehouse-with" }
}

resource "aws_s3_bucket" "warehouse_without" {
  bucket = "${local.bucket_prefix}-without"
  tags   = { Name = "${var.project}-warehouse-without" }
}

resource "aws_s3_bucket_versioning" "warehouse_with" {
  bucket = aws_s3_bucket.warehouse_with.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_versioning" "warehouse_without" {
  bucket = aws_s3_bucket.warehouse_without.id
  versioning_configuration { status = "Enabled" }
}
