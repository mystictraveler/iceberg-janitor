# CloudWatch dashboard for the iceberg-janitor system.
#
# Layout:
#   Row 1 — System health (Fargate CPU/mem, S3 bytes)
#   Row 2 — Namespace overview: job throughput by operation
#   Row 3 — Maintain pipeline: phase timings rolled up across all tables
#   Row 4 — Per-table compact effectiveness (file count reduction)
#   Row 5 — Per-table manifest consolidation
#   Row 6 — Per-table maintain wall time (table-level drill-down)
#   Row 7 — Errors (last 50)
#
# Drill-down model: top widgets are namespace rollups; bottom widgets
# break down by table. Filter the dashboard time range or click a row
# in a table widget to scope further.

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project}-overview"

  dashboard_body = jsonencode({
    widgets = [
      # === Row 1: System health ===
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "Server Fargate CPU"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average", label = "avg" }],
            ["...", { stat = "Maximum", label = "max" }],
          ]
          period = 60
          yAxis = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "Server Fargate Memory"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/ECS", "MemoryUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average", label = "avg" }],
            ["...", { stat = "Maximum", label = "max" }],
          ]
          period = 60
          yAxis = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 0
        width  = 8
        height = 6
        properties = {
          title   = "Warehouse S3 Bytes"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/S3", "BucketSizeBytes", "BucketName", aws_s3_bucket.warehouse_with.id, "StorageType", "StandardStorage", { label = "with-janitor" }],
            ["...", aws_s3_bucket.warehouse_without.id, ".", ".", { label = "without-janitor" }],
          ]
          period = 86400
        }
      },

      # === Row 2: Namespace overview — job throughput by operation ===
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Job throughput by operation (last hour)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /job (created|completed|failed)/
            | parse msg "* job *" as op, state
            | stats count() by op, state
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
          title  = "Active jobs over time"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /job (created|completed|failed)/
            | parse msg "* job *" as op, state
            | stats count() by op, state, bin(2m)
          EOQ
          view = "timeSeries"
          stacked = true
        }
      },

      # === Row 3: Maintain pipeline — phase timings rolled up ===
      {
        type   = "log"
        x      = 0
        y      = 12
        width  = 24
        height = 6
        properties = {
          title  = "Maintain pipeline phase timings (avg/max ms across all tables)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /^maintain:/
            | parse msg "maintain: * done" as phase
            | stats avg(elapsed_ms) as avg_ms, max(elapsed_ms) as max_ms, count() as runs by phase
          EOQ
          view = "table"
        }
      },

      # === Row 4: Per-table compact effectiveness ===
      {
        type   = "log"
        x      = 0
        y      = 18
        width  = 24
        height = 6
        properties = {
          title  = "Compact effectiveness per table (file reduction)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed" or msg = "maintain: compact done"
            | stats avg(before_files) as avg_before, avg(after_files) as avg_after, avg(before_files - after_files) as avg_files_removed, max(before_files) as max_before, count() as runs by table
            | sort avg_files_removed desc
          EOQ
          view = "table"
        }
      },

      # === Row 5: Per-table manifest consolidation ===
      {
        type   = "log"
        x      = 0
        y      = 24
        width  = 24
        height = 6
        properties = {
          title  = "Manifest rewrite consolidation per table (before → after)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /rewrite-manifests.*done/ or msg = "rewrite-manifests job completed"
            | stats avg(before_manifests) as avg_before, avg(after_manifests) as avg_after, max(before_manifests) as max_before, count() as runs by table
            | sort max_before desc
          EOQ
          view = "table"
        }
      },

      # === Row 6: Per-table maintain wall time ===
      {
        type   = "log"
        x      = 0
        y      = 30
        width  = 12
        height = 6
        properties = {
          title  = "Maintain total duration per table (ms)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job completed"
            | stats avg(total_ms) as avg_ms, max(total_ms) as max_ms, count() as runs by table
            | sort avg_ms desc
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 30
        width  = 12
        height = 6
        properties = {
          title  = "Expire per table (snapshots removed)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "expire job completed" or msg = "maintain: expire done"
            | stats avg(removed) as avg_removed, sum(removed) as total_removed, count() as runs by table
            | sort total_removed desc
          EOQ
          view = "table"
        }
      },

      # === Row 7: Errors ===
      {
        type   = "log"
        x      = 0
        y      = 36
        width  = 24
        height = 6
        properties = {
          title  = "Errors (last 50)"
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
    ]
  })
}

output "dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.region}.console.aws.amazon.com/cloudwatch/home?region=${var.region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}
