# Athena workgroup + Glue catalog for querying Iceberg tables on S3.
# Athena reads iceberg metadata directly from S3; Glue just stores
# the table-location pointers (doesn't conflict with the directory catalog).

resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project}-athena-results-${data.aws_caller_identity.current.account_id}"
  tags   = { Name = "${var.project}-athena-results" }
}

resource "aws_athena_workgroup" "main" {
  name = var.project

  configuration {
    enforce_workgroup_configuration = true
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.id}/results/"
    }
  }

  tags = { Name = "${var.project}-workgroup" }
}

# Glue database — one for with-janitor, one for without-janitor.
# VPC endpoint for Athena API (bench task is in private subnets)
resource "aws_vpc_endpoint" "athena" {
  vpc_id              = local.vpc_id
  service_name        = "com.amazonaws.${var.region}.athena"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = local.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.project}-athena-endpoint" }
}

resource "aws_glue_catalog_database" "with_janitor" {
  name = "${replace(var.project, "-", "_")}_with"
}

resource "aws_glue_catalog_database" "without_janitor" {
  name = "${replace(var.project, "-", "_")}_without"
}
