# CloudWatch dashboard for the iceberg-janitor system.
#
# Design principle: the top rows are ALWAYS populated (ECS metrics,
# HTTP request rate). You open the dashboard and immediately see
# "3 replicas, 12 req/s, 0 active jobs" or "3 replicas, 45 req/s,
# 2 active jobs running for 3m". The bottom rows are operational
# detail that populates during and after maintain cycles.
#
# Layout:
#   Row 1 — ECS service health (running/desired, CPU, memory)
#   Row 2 — HTTP request rate by path + active/completed jobs
#   Row 3 — File count sawtooth (before/after per maintain job)
#   Row 4 — Maintain pipeline phase timings
#   Row 5 — Classifier decisions + mode mix
#   Row 6 — Hot-path: CompactHot partitions stitched + wall time
#   Row 7 — Cold-path: trigger firings
#   Row 8 — Manifest consolidation + expire
#   Row 9 — Cross-replica dedup + dry-run
#   Row 10 — Errors

resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project}-overview"

  dashboard_body = jsonencode({
    widgets = [

      # === Row 1: ECS service health (ALWAYS POPULATED) ===
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 6
        height = 6
        properties = {
          title   = "Server Replicas (running vs desired)"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["ECS/ContainerInsights", "RunningTaskCount", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { label = "running", stat = "Average" }],
            [".", "DesiredTaskCount", ".", ".", ".", ".", { label = "desired", stat = "Average" }],
          ]
          period = 60
          yAxis  = { left = { min = 0 } }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 0
        width  = 6
        height = 6
        properties = {
          title   = "CPU Utilization %"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/ECS", "CPUUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average", label = "avg" }],
            ["...", { stat = "Maximum", label = "max" }],
          ]
          period = 60
          yAxis  = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 6
        height = 6
        properties = {
          title   = "Memory Utilization %"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/ECS", "MemoryUtilization", "ServiceName", aws_ecs_service.janitor_server.name, "ClusterName", aws_ecs_cluster.main.name, { stat = "Average", label = "avg" }],
            ["...", { stat = "Maximum", label = "max" }],
          ]
          period = 60
          yAxis  = { left = { min = 0, max = 100 } }
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 0
        width  = 6
        height = 6
        properties = {
          title   = "NLB Healthy Hosts"
          view    = "timeSeries"
          region  = var.region
          metrics = [
            ["AWS/NetworkELB", "HealthyHostCount", "TargetGroup", aws_lb_target_group.janitor.arn_suffix, "LoadBalancer", aws_lb.internal.arn_suffix, { stat = "Average" }],
          ]
          period = 60
          yAxis  = { left = { min = 0 } }
        }
      },

      # === Row 2: HTTP request rate + job lifecycle (ALWAYS POPULATED) ===
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "HTTP request rate by endpoint"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "http"
            | stats count() as requests by path, bin(1m)
          EOQ
          view = "timeSeries"
          stacked = true
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 6
        width  = 12
        height = 6
        properties = {
          title  = "Jobs: created / completed / failed over time"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /job (created|completed|failed)/
            | parse msg "* job *" as op, state
            | stats count() by state, bin(2m)
          EOQ
          view    = "timeSeries"
          stacked = false
        }
      },

      # === Row 3: File count sawtooth (the money graph) ===
      #
      # This is what operators care about: files accumulate during
      # streaming, then drop after each maintain. The sawtooth should
      # be visible in real time. Uses before_files/after_files from
      # the maintain job completed log line.
      {
        type   = "log"
        x      = 0
        y      = 12
        width  = 24
        height = 6
        properties = {
          title  = "File count: before → after maintain (per table)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job completed"
            | stats latest(before_files) as before, latest(after_files) as after by table, bin(5m)
          EOQ
          view = "timeSeries"
        }
      },

      # === Row 4: Maintain pipeline phase timings ===
      {
        type   = "log"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Phase timings (avg/max ms, all tables)"
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
      {
        type   = "log"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Maintain total wall time per table"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job completed"
            | stats avg(total_ms) as avg_ms, max(total_ms) as max_ms, count() by table, mode
            | sort avg_ms desc
          EOQ
          view = "table"
        }
      },

      # === Row 5: Classifier + mode dispatch ===
      {
        type   = "log"
        x      = 0
        y      = 24
        width  = 12
        height = 6
        properties = {
          title  = "Classifier decisions (class → mode per table)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job created"
            | stats count() as calls, latest(class) as class, latest(mode) as mode by table
            | sort calls desc
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
          title  = "Mode mix over time"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job created"
            | stats count() by mode, bin(2m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },

      # === Row 6: Hot-path — CompactHot stitch + merge ===
      {
        type   = "log"
        x      = 0
        y      = 30
        width  = 12
        height = 6
        properties = {
          title  = "CompactHot: partitions stitched per table"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain: compact_hot done"
            | stats sum(partitions_stitched) as stitched, sum(partitions_failed) as failed, avg(elapsed_ms) as avg_ms by table
            | sort stitched desc
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
          title  = "CompactHot wall time over time (ms)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain: compact_hot done"
            | stats avg(elapsed_ms) as avg_ms, max(elapsed_ms) as max_ms by bin(5m)
          EOQ
          view = "timeSeries"
        }
      },

      # === Row 7: Cold-path + manifest consolidation + expire ===
      {
        type   = "log"
        x      = 0
        y      = 36
        width  = 8
        height = 6
        properties = {
          title  = "Cold triggers"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain: compact_cold done"
            | stats sum(partitions_triggered) as triggered, sum(partitions_compacted) as compacted by table
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 8
        y      = 36
        width  = 8
        height = 6
        properties = {
          title  = "Manifest consolidation (before → after)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /rewrite-manifests.*done/
            | stats avg(before_manifests) as avg_before, avg(after_manifests) as avg_after by table
            | sort avg_before desc
          EOQ
          view = "table"
        }
      },
      {
        type   = "log"
        x      = 16
        y      = 36
        width  = 8
        height = 6
        properties = {
          title  = "Expire (snapshots removed)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain: expire done"
            | stats sum(removed) as total_removed, avg(elapsed_ms) as avg_ms by table
          EOQ
          view = "table"
        }
      },

      # === Row 8: Cross-replica dedup + dry-run ===
      {
        type   = "log"
        x      = 0
        y      = 42
        width  = 12
        height = 6
        properties = {
          title  = "In-flight dedup events"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg like /dedup/
            | stats count() as dedup_hits by table, bin(2m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 42
        width  = 12
        height = 6
        properties = {
          title  = "Dry-run requests"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job created" and dry_run = 1
            | stats count() as dry_runs by table, bin(5m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },

      # === Row 9: Errors (always check last) ===
      {
        type   = "log"
        x      = 0
        y      = 48
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
