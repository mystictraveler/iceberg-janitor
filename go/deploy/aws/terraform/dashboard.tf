# CloudWatch dashboard for the iceberg-janitor system.
# Combines ECS service metrics, server log-derived metrics (jobs, HTTP),
# compact phase timings (from observe spans), and S3 warehouse stats.

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project}-overview"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: ECS service health
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 8
        height = 6
        properties = {
          title = "Fargate CPU (server)"
          view  = "timeSeries"
          stacked = false
          region = var.region
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average" }],
            ["...", { stat = "Maximum" }],
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 0
        width  = 8
        height = 6
        properties = {
          title = "Fargate Memory (server)"
          view  = "timeSeries"
          region = var.region
          metrics = [
            ["AWS/ECS", "MemoryUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average" }],
            ["...", { stat = "Maximum" }],
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "S3 Warehouse Bytes"
          view   = "timeSeries"
          region = var.region
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.warehouse_with.id, "StorageType", "StandardStorage", { label = "with-janitor" }],
            ["...", aws_s3_bucket.warehouse_without.id, ".", ".", { label = "without-janitor" }],
          ]
          period = 86400
        }
      },

      # Row 2: HTTP request volume + maintain job status (log-derived)
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "HTTP requests by path"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "http"
            | stats count() by path, bin(1m)
            | sort @timestamp desc
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Job lifecycle (created/completed/failed)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /job (created|started|completed|failed)/
            | stats count() by msg, bin(1m)
          EOQ
          view = "timeSeries"
        }
      },

      # Row 3: Compact + Maintain timings
      {
        type   = "log"
        x      = 0
        y      = 12
        width  = 12
        height = 6
        properties = {
          title  = "Compact job duration (ms)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed"
            | stats avg(elapsed_ms), max(elapsed_ms), min(elapsed_ms), count() by table, bin(5m)
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 12
        width  = 12
        height = 6
        properties = {
          title  = "Maintain pipeline phases (ms)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /maintain:/
            | parse msg "maintain: * done" as phase
            | stats avg(elapsed_ms) as avg_ms, max(elapsed_ms) as max_ms, count() as runs by phase, table
          EOQ
          view = "table"
        }
      },

      # Row 4: Compact effectiveness (file count before/after) + CAS attempts
      {
        type   = "log"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Compact effectiveness (file reduction)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed"
            | stats avg(before_files) as avg_before, avg(after_files) as avg_after, max(before_files) as max_before, count() as runs by table
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Manifest rewrite consolidation"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "rewrite-manifests job completed" or msg = "maintain: rewrite-manifests done" or msg = "maintain: rewrite-manifests (pre-compact) done" or msg = "maintain: rewrite-manifests (post-compact) done"
            | stats avg(before_manifests) as avg_before, avg(after_manifests) as avg_after, count() as runs by table
          EOQ
          view = "table"
        }
      },

      # Row 5: Errors + bench results
      {
        type   = "log"
        x      = 0
        y      = 24
        width  = 12
        height = 6
        properties = {
          title  = "Errors in last hour"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter level = "ERROR"
            | sort @timestamp desc
            | limit 50
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 24
        width  = 12
        height = 6
        properties = {
          title  = "Bench: Athena query latency by query"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.bench.name}'
            | parse @message /(?<side>with|without)\/(?<query>q\d+):\s+(?<latency_ms>\d+)ms/
            | filter ispresent(query)
            | stats avg(latency_ms) by side, query
          EOQ
          view = "table"
        }
      },
    ]
  })
}

output "dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}
