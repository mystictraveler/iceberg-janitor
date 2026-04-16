# CloudWatch dashboard for the iceberg-janitor system.
#
# Design principle: the top rows are ALWAYS populated (ECS metrics,
# HTTP request rate). You open the dashboard and immediately see
# "3 replicas, 12 req/s, 0 active jobs" or "3 replicas, 45 req/s,
# 2 active jobs running for 3m". The bottom rows are operational
# detail that populates during and after maintain cycles.
#
# Layout:
#   Row 1  — ECS service health (running/desired, CPU, memory)
#   Row 2  — HTTP request rate by path + active/completed jobs
#   Row 3  — File count sawtooth (before/after per maintain job)
#   Row 4  — Top 10 hot tables last hour
#   Row 5a — Maintain pipeline phase timings
#   Row 5b — Classifier + mode mix
#   Row 6  — Hot-path: CompactHot partitions stitched + wall time
#   Row 7  — Cold-path + manifest consolidation + expire
#   Row 8  — Cross-replica dedup + dry-run
#   Row 9  — MASTER CHECK correctness (pass/fail + per-invariant)     — NEW
#   Row 10 — Skipped rounds (schema-evolution guard, deferrals)       — NEW
#   Row 11 — Circuit breakers (trip rate by id + paused tables)       — NEW (phased)
#   Row 12 — V2 deletes (position/equality applied + refused)         — NEW (phased)
#   Row 13 — CAS contention (retries distribution)                    — NEW
#   Row 14 — Errors (always last)
#
# Observability-track phasing. Rows 9, 10, 13 read fields emitted by
# the compact-completed log line today (via
# cmd/janitor-server/jobs.go after the observability-track branch):
# `skipped`, `skipped_reason`, `master_overall`, `master_I1..I7`,
# `dvs_applied`, `attempts`. Rows 11 and 12 depend on later phase PRs
# that add CB-trip and V2-delete fields to the logs; until those
# ship, those panels render empty. That empty state is the deliberate
# signal that the corresponding instrumentation hasn't landed.
#
# Cost discipline. Every widget below is a CloudWatch Logs Insights
# query over the existing janitor log group; no agent deployment, no
# new metric-filter resources, no hot-path work on the server. Log
# cardinality is not amplified — we're reading fields the server
# was already going to emit (or has started emitting as of the
# observability-track branch). See OBSERVABILITY_SPEC.md for the
# no-hot-path-overhead rule this dashboard respects.

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
            ["AWS/NetworkELB", "HealthyHostCount", "TargetGroup", aws_lb_target_group.fargate.arn_suffix, "LoadBalancer", aws_lb.internal.arn_suffix, { stat = "Average" }],
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

      # === Row 4: Top 10 hot tables in the last hour ===
      #
      # At-a-glance: which tables are getting the most maintain
      # activity? Sorted by total maintain calls. Shows class,
      # mode, total wall time, and partitions stitched. If a table
      # is dominating the maintain budget, it'll be at the top.
      {
        type   = "log"
        x      = 0
        y      = 18
        width  = 24
        height = 6
        properties = {
          title  = "Top 10 hot tables (last hour)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "maintain job completed"
            | stats count() as maintain_calls, latest(mode) as mode, sum(total_ms) as total_ms, max(total_ms) as max_ms by table
            | sort maintain_calls desc
            | limit 10
          EOQ
          view = "table"
        }
      },

      # === Row 5: Maintain pipeline phase timings ===
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

      # === Row 9: Master-check correctness (pass/fail + per-invariant) ===
      #
      # The master check is the janitor's non-bypassable pre-commit
      # invariant set. A failure here means the janitor REFUSED to
      # commit something that would have been silently wrong — which
      # is a feature working as intended, but operators need it on a
      # dashboard to spot a sudden spike (could indicate an upstream
      # writer bug or a corrupt source file).
      #
      # Fields used (emitted by cmd/janitor-server/jobs.go on compact
      # job completed):
      #   master_overall   "pass" | "fail"
      #   master_I1        "pass" | "fail"  (row count)
      #   master_I2        schema identity
      #   master_I3/I4     per-column value/null counts
      #   master_I5        bounds presence
      #   master_I7        manifest references exist
      {
        type   = "log"
        x      = 0
        y      = 48
        width  = 12
        height = 6
        properties = {
          title  = "Master check overall pass/fail"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed"
            | stats count() as checks by master_overall, bin(5m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 48
        width  = 12
        height = 6
        properties = {
          title  = "Master check: failures by invariant (last 24h)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed" and master_overall = "fail"
            | stats count() as failures by master_I1, master_I2, master_I3, master_I4, master_I5, master_I7
          EOQ
          view = "table"
        }
      },

      # === Row 10: Skipped rounds (schema-evolution guard, deferrals) ===
      #
      # A compact that "ran" but Skipped=true did no work — e.g. the
      # schema-evolution guard saw source files spanning two schema
      # ids and refused to cross the boundary. Not an error, but a
      # signal: if the skip rate is chronic, the table's old-schema
      # tail isn't aging out and an operator may need to run expire.
      #
      # Fields: `skipped` (bool), `skipped_reason` ("mixed_schemas",
      # etc), from the compact-completed log line.
      {
        type   = "log"
        x      = 0
        y      = 54
        width  = 12
        height = 6
        properties = {
          title  = "Skipped compactions by reason"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed" and skipped = 1
            | stats count() as skips by skipped_reason, bin(10m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 54
        width  = 12
        height = 6
        properties = {
          title  = "Tables with mixed-schema backlog (last hour)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed" and skipped_reason = "mixed_schemas"
            | stats count() as mixed_rounds by table
            | sort mixed_rounds desc
            | limit 20
          EOQ
          view = "table"
        }
      },

      # === Row 11: Circuit breakers ===
      #
      # PHASED — these widgets read `cb_tripped` and `cb_reason`
      # fields that later observability-phase PRs must emit from
      # pkg/safety/circuitbreaker.go trip sites. Until that lands,
      # the panels render empty. That's the deliberate signal that
      # Phase 2 of the observability track has not yet shipped.
      #
      # Required log fields (to be emitted on trip):
      #   cb_tripped   "CB2" | "CB3" | "CB4" | "CB7" | "CB8" | "CB9" | "CB10" | "CB11"
      #   cb_reason    short human-readable reason string
      #   table        "ns.name"
      {
        type   = "log"
        x      = 0
        y      = 60
        width  = 12
        height = 6
        properties = {
          title  = "Circuit breaker trips by id (PHASED — needs Phase 2 emission)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter cb_tripped != ""
            | stats count() as trips by cb_tripped, bin(15m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 60
        width  = 12
        height = 6
        properties = {
          title  = "Currently-paused tables (PHASED)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter cb_tripped != ""
            | stats latest(cb_tripped) as last_trip, latest(cb_reason) as reason by table
            | sort @timestamp desc
            | limit 50
          EOQ
          view = "table"
        }
      },

      # === Row 12: V2 deletes applied / refused ===
      #
      # Fields — `dvs_applied` (int) is emitted today on the compact
      # completed line (from VerifyCompactionConsistency.I1RowCount.DVs).
      # `eq_delete_refused` + `eq_delete_file` are PHASED — emitted
      # by pkg/janitor/deletes.go's LoadEqualityDelete when it
      # returns *UnsupportedFeatureError. That phase add is small
      # (one logger call at the refusal site).
      {
        type   = "log"
        x      = 0
        y      = 66
        width  = 12
        height = 6
        properties = {
          title  = "Rows dropped by V2 deletes per compact (sum 5m)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed" and dvs_applied > 0
            | stats sum(dvs_applied) as rows_dropped by bin(5m)
          EOQ
          view    = "timeSeries"
          stacked = false
        }
      },
      {
        type   = "log"
        x      = 12
        y      = 66
        width  = 12
        height = 6
        properties = {
          title  = "Equality delete refusals (complex type) — PHASED"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter eq_delete_refused = 1
            | stats count() as refusals, latest(eq_delete_file) as file by table
          EOQ
          view = "table"
        }
      },

      # === Row 13: CAS contention — retry distribution ===
      #
      # `attempts` is already emitted on the compact-completed log
      # line. attempts=1 means no contention; attempts>1 means at
      # least one retry against a concurrent writer. Stacking by
      # attempt-count bucket gives operators a distribution of
      # writer-fight intensity per table over time.
      {
        type   = "log"
        x      = 0
        y      = 72
        width  = 24
        height = 6
        properties = {
          title  = "CAS attempts per compact (distribution — 1 = uncontended)"
          region = var.region
          query  = <<-EOQ
            SOURCE '${aws_cloudwatch_log_group.janitor.name}'
            | filter msg = "compact job completed"
            | stats count() as compacts by attempts, bin(10m)
          EOQ
          view    = "timeSeries"
          stacked = true
        }
      },

      # === Row 14: Errors (always check last) ===
      {
        type   = "log"
        x      = 0
        y      = 78
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
