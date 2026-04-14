"""CLI for Iceberg Janitor — analyze and maintain Iceberg tables."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click
import yaml
from rich.console import Console
from rich.table import Table as RichTable

from iceberg_janitor.analyzer.health import assess_table
from iceberg_janitor.catalog import get_catalog
from iceberg_janitor.policy.engine import evaluate
from iceberg_janitor.policy.models import PolicyConfig, TablePolicy

console = Console()


# ---------------------------------------------------------------------------
# Shared options
# ---------------------------------------------------------------------------

def _catalog_options(fn):
    """Decorate a Click command with the common catalog connection options."""
    fn = click.option("--catalog-uri", required=True, help="Iceberg REST catalog URI")(fn)
    fn = click.option("--warehouse", required=True, help="Warehouse location (e.g., s3://bucket/)")(fn)
    fn = click.option("--s3-endpoint", default=None, help="S3 endpoint URL (for MinIO)")(fn)
    return fn


def _build_catalog(catalog_uri: str, warehouse: str, s3_endpoint: str | None):
    """Construct a PyIceberg catalog from CLI options."""
    props: dict[str, str] = {"uri": catalog_uri, "warehouse": warehouse}
    if s3_endpoint:
        props["s3.endpoint"] = s3_endpoint
    return get_catalog(name="janitor", **props)


# ---------------------------------------------------------------------------
# Root group
# ---------------------------------------------------------------------------

@click.group()
@click.version_option()
def cli():
    """Iceberg Janitor — automated table maintenance for streaming workloads."""


# ---------------------------------------------------------------------------
# analyze
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("table_id")
@_catalog_options
@click.option("--small-file-threshold", default=8 * 1024 * 1024, help="Small file threshold in bytes")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table")
def analyze(table_id: str, catalog_uri: str, warehouse: str, s3_endpoint: str | None, small_file_threshold: int, output: str):
    """Analyze an Iceberg table's health via the catalog API."""
    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    report = assess_table(catalog, table_id, small_file_threshold=small_file_threshold)

    if output == "json":
        click.echo(json.dumps({
            "table_id": report.table_id,
            "assessed_at": report.assessed_at.isoformat(),
            "file_stats": {
                "file_count": report.file_stats.file_count,
                "total_mb": round(report.file_stats.total_mb, 2),
                "avg_file_size_mb": round(report.file_stats.avg_file_size_mb, 2),
                "small_file_count": report.file_stats.small_file_count,
                "small_file_ratio": round(report.file_stats.small_file_ratio, 3),
            },
            "snapshot_stats": {
                "snapshot_count": report.snapshot_stats.snapshot_count,
                "oldest_snapshot": str(report.snapshot_stats.oldest_snapshot_ts),
                "newest_snapshot": str(report.snapshot_stats.newest_snapshot_ts),
            },
            "is_healthy": report.is_healthy,
            "needs_compaction": report.needs_compaction,
            "needs_snapshot_expiry": report.needs_snapshot_expiry,
            "errors": report.errors,
        }, indent=2))
        return

    # Rich table output
    t = RichTable(title=f"Health Report: {table_id}")
    t.add_column("Metric", style="bold")
    t.add_column("Value")
    t.add_column("Status")

    def status_icon(ok: bool) -> str:
        return "[green]OK[/green]" if ok else "[red]NEEDS ATTENTION[/red]"

    t.add_row("Total Files", str(report.file_stats.file_count), "")
    t.add_row("Total Size", f"{report.file_stats.total_mb:.1f} MB", "")
    t.add_row("Avg File Size", f"{report.file_stats.avg_file_size_mb:.1f} MB", "")
    t.add_row(
        "Small Files",
        f"{report.file_stats.small_file_count} ({report.file_stats.small_file_ratio:.1%})",
        status_icon(not report.needs_compaction),
    )
    t.add_row(
        "Snapshots",
        str(report.snapshot_stats.snapshot_count),
        status_icon(not report.needs_snapshot_expiry),
    )

    if report.size_distribution:
        t.add_section()
        for bucket in report.size_distribution:
            t.add_row(f"  {bucket.bucket}", str(bucket.file_count), "")

    if report.hot_partitions:
        t.add_section()
        t.add_row("[bold]Hot Partitions[/bold]", "", "")
        for p in report.hot_partitions[:5]:
            t.add_row(f"  {p.partition}", f"{p.file_count} files", "")

    if report.errors:
        t.add_section()
        for err in report.errors:
            t.add_row("[red]Error[/red]", err, "")

    console.print(t)

    overall = "[green]HEALTHY[/green]" if report.is_healthy else "[red]NEEDS MAINTENANCE[/red]"
    console.print(f"\nOverall: {overall}")


# ---------------------------------------------------------------------------
# maintain (existing)
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("table_id")
@_catalog_options
@click.option("--policy-file", type=click.Path(exists=True), help="YAML policy config file")
@click.option("--dry-run", is_flag=True, help="Show what would be done without doing it")
def maintain(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str | None,
    policy_file: str | None,
    dry_run: bool,
):
    """Run maintenance on an Iceberg table based on its health and policy."""
    from iceberg_janitor.maintenance import compact_files, expire_snapshots, remove_orphans
    from iceberg_janitor.policy.models import ActionType

    # Load policy
    if policy_file:
        with open(policy_file) as f:
            policy_config = PolicyConfig.model_validate(yaml.safe_load(f))
        policy = policy_config.get_policy(table_id)
    else:
        policy = TablePolicy()

    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)

    console.print(f"[bold]Analyzing[/bold] {table_id}...")
    report = assess_table(catalog, table_id, small_file_threshold=policy.small_file_threshold_bytes)

    actions = evaluate(report, policy)

    if not actions:
        console.print("[green]No maintenance needed.[/green]")
        return

    console.print(f"\n[bold]Planned actions ({len(actions)}):[/bold]")
    for action in actions:
        console.print(f"  - {action.action_type.value}: {action.reason}")

    if dry_run:
        console.print("\n[yellow]Dry run — no changes made.[/yellow]")
        return

    for action in actions:
        console.print(f"\n[bold]Executing:[/bold] {action.action_type.value}")
        try:
            if action.action_type == ActionType.COMPACT_FILES:
                result = compact_files(
                    catalog, table_id,
                    target_file_size_bytes=action.params.get("target_file_size_bytes", 128 * 1024 * 1024),
                    dry_run=dry_run,
                )
            elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                result = expire_snapshots(
                    catalog, table_id,
                    retention_hours=action.params.get("retention_hours", 168),
                    min_snapshots_to_keep=action.params.get("min_snapshots_to_keep", 5),
                    dry_run=dry_run,
                )
            elif action.action_type == ActionType.REMOVE_ORPHANS:
                result = remove_orphans(
                    catalog, table_id,
                    retention_hours=action.params.get("retention_hours", 72),
                    dry_run=dry_run,
                )
            else:
                console.print(f"  [yellow]Skipped: {action.action_type.value} not yet implemented[/yellow]")
                continue

            console.print(f"  Result: {json.dumps(result, indent=2, default=str)}")
        except Exception as e:
            console.print(f"  [red]Failed: {e}[/red]")

    console.print("\n[green]Maintenance complete.[/green]")


# ---------------------------------------------------------------------------
# report (existing)
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("table_id")
@_catalog_options
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table")
def report(table_id: str, catalog_uri: str, warehouse: str, s3_endpoint: str | None, output: str):
    """Generate a detailed maintenance report for a table."""
    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    report_data = assess_table(catalog, table_id)
    actions = evaluate(report_data, TablePolicy())

    if output == "json":
        click.echo(json.dumps({
            "health": {
                "is_healthy": report_data.is_healthy,
                "needs_compaction": report_data.needs_compaction,
                "needs_snapshot_expiry": report_data.needs_snapshot_expiry,
            },
            "recommended_actions": [
                {
                    "action": a.action_type.value,
                    "priority": a.priority,
                    "reason": a.reason,
                }
                for a in actions
            ],
        }, indent=2))
        return

    console.print(f"\n[bold]Maintenance Report: {table_id}[/bold]\n")

    if report_data.is_healthy:
        console.print("[green]Table is healthy. No maintenance needed.[/green]")
    else:
        console.print("[yellow]Table needs maintenance:[/yellow]\n")
        for action in actions:
            console.print(f"  [{action.priority}] {action.action_type.value}")
            console.print(f"      {action.reason}\n")


# ---------------------------------------------------------------------------
# compact (manual override)
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("table_id")
@_catalog_options
@click.option("--dry-run", is_flag=True, help="Report what would be compacted without doing it")
@click.option("--partition", default=None, help="Only compact this partition (substring match)")
@click.option("--target-file-size-mb", default=128, type=int, help="Target file size in MB")
def compact(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str | None,
    dry_run: bool,
    partition: str | None,
    target_file_size_mb: int,
):
    """Force-compact a specific Iceberg table now."""
    from iceberg_janitor.maintenance.compaction import compact_files

    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    target_bytes = target_file_size_mb * 1024 * 1024

    if partition:
        console.print(
            f"[bold]Compacting[/bold] {table_id} "
            f"(partition filter: {partition!r}, dry_run={dry_run})"
        )
    else:
        console.print(f"[bold]Compacting[/bold] {table_id} (dry_run={dry_run})")

    try:
        result = compact_files(
            catalog,
            table_id,
            target_file_size_bytes=target_bytes,
            dry_run=dry_run,
        )
        console.print(json.dumps(result, indent=2, default=str))
    except Exception as exc:
        console.print(f"[red]Compaction failed: {exc}[/red]")
        sys.exit(1)


# ---------------------------------------------------------------------------
# expire-snapshots (manual override)
# ---------------------------------------------------------------------------

@cli.command("expire-snapshots")
@click.argument("table_id")
@_catalog_options
@click.option("--dry-run", is_flag=True, help="Report what would be expired without doing it")
@click.option("--retention-hours", default=168, type=int, help="Keep snapshots newer than this (hours)")
@click.option("--min-keep", default=5, type=int, help="Always keep at least this many snapshots")
def expire_snapshots_cmd(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str | None,
    dry_run: bool,
    retention_hours: int,
    min_keep: int,
):
    """Force-expire snapshots on a specific Iceberg table."""
    from iceberg_janitor.maintenance.snapshots import expire_snapshots

    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    console.print(
        f"[bold]Expiring snapshots[/bold] on {table_id} "
        f"(retention={retention_hours}h, min_keep={min_keep}, dry_run={dry_run})"
    )

    try:
        result = expire_snapshots(
            catalog,
            table_id,
            retention_hours=retention_hours,
            min_snapshots_to_keep=min_keep,
            dry_run=dry_run,
        )
        console.print(json.dumps(result, indent=2, default=str))
    except Exception as exc:
        console.print(f"[red]Snapshot expiration failed: {exc}[/red]")
        sys.exit(1)


# ---------------------------------------------------------------------------
# cleanup-orphans (manual override)
# ---------------------------------------------------------------------------

@cli.command("cleanup-orphans")
@click.argument("table_id")
@_catalog_options
@click.option("--dry-run", is_flag=True, help="Report orphans without deleting them")
@click.option("--retention-hours", default=72, type=int, help="Safety margin for orphan detection (hours)")
def cleanup_orphans_cmd(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str | None,
    dry_run: bool,
    retention_hours: int,
):
    """Force-clean orphan files for a specific Iceberg table."""
    from iceberg_janitor.maintenance.orphans import remove_orphans

    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    console.print(
        f"[bold]Cleaning orphans[/bold] on {table_id} "
        f"(retention={retention_hours}h, dry_run={dry_run})"
    )

    try:
        result = remove_orphans(
            catalog,
            table_id,
            retention_hours=retention_hours,
            dry_run=dry_run,
        )
        console.print(json.dumps(result, indent=2, default=str))
    except Exception as exc:
        console.print(f"[red]Orphan cleanup failed: {exc}[/red]")
        sys.exit(1)


# ---------------------------------------------------------------------------
# trigger-status (inspect trigger state)
# ---------------------------------------------------------------------------

@cli.command("trigger-status")
@click.argument("table_id")
@_catalog_options
@click.option("--policy-file", type=click.Path(exists=True), help="YAML policy config file")
@click.option("--partition", default=None, help="Show partition-level detail (substring match)")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table")
def trigger_status(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    s3_endpoint: str | None,
    policy_file: str | None,
    partition: str | None,
    output: str,
):
    """Show the current trigger state for a table.

    Connects to the catalog, inspects the table, builds an ephemeral
    scheduler state, evaluates triggers, and shows the result.
    """
    from iceberg_janitor.strategy.partition import PartitionAnalyzer
    from iceberg_janitor.strategy.scheduler import MaintenanceScheduler, TableState
    from iceberg_janitor.strategy.triggers import build_triggers_from_policy

    # Load policy
    if policy_file:
        with open(policy_file) as f:
            policy_config = PolicyConfig.model_validate(yaml.safe_load(f))
        policy = policy_config.get_policy(table_id)
    else:
        policy = TablePolicy()

    catalog = _build_catalog(catalog_uri, warehouse, s3_endpoint)
    table = catalog.load_table(table_id)

    # Build ephemeral state from live table
    plan_files = list(table.scan().plan_files())
    small_count = sum(
        1 for t in plan_files if t.file.file_size_in_bytes < policy.small_file_threshold_bytes
    )
    small_bytes = sum(
        t.file.file_size_in_bytes
        for t in plan_files
        if t.file.file_size_in_bytes < policy.small_file_threshold_bytes
    )
    current_snap = table.current_snapshot()

    state = TableState(
        table_id=table_id,
        small_file_count=small_count,
        total_file_count=len(plan_files),
        uncompacted_bytes=small_bytes,
        current_snapshot_id=current_snap.snapshot_id if current_snap else None,
        # commits_since_last_compaction is unknown for ephemeral eval;
        # use snapshot count as a rough proxy
        commits_since_last_compaction=len(table.metadata.snapshots),
    )

    trigger = build_triggers_from_policy(policy)
    result = trigger.evaluate(state)

    # Partition analysis (optional)
    part_analysis = None
    if partition is not None:
        analyzer = PartitionAnalyzer(
            small_file_threshold_bytes=policy.small_file_threshold_bytes,
        )
        part_analysis = analyzer.filter_partitions(catalog, table_id, partition)

    if output == "json":
        payload: dict = {
            "table_id": table_id,
            "trigger_mode": policy.trigger_mode,
            "trigger_fired": result.fired,
            "trigger_description": result.description,
            "state": {
                "total_files": state.total_file_count,
                "small_files": state.small_file_count,
                "uncompacted_bytes": state.uncompacted_bytes,
                "commits_proxy": state.commits_since_last_compaction,
            },
        }
        if part_analysis:
            payload["partition_analysis"] = {
                "total_partitions": part_analysis.total_partitions,
                "degraded_partitions": part_analysis.degraded_partitions,
                "recommendations": [
                    {
                        "partition": r.partition_key,
                        "action": r.action,
                        "reason": r.reason,
                    }
                    for r in part_analysis.recommendations
                ],
            }
        click.echo(json.dumps(payload, indent=2, default=str))
        return

    # Rich table output
    t = RichTable(title=f"Trigger Status: {table_id}")
    t.add_column("Field", style="bold")
    t.add_column("Value")

    t.add_row("Trigger mode", policy.trigger_mode)
    fired_str = "[red]FIRED[/red]" if result.fired else "[green]not fired[/green]"
    t.add_row("Trigger fired", fired_str)
    t.add_row("Description", result.description)
    t.add_section()
    t.add_row("Total files", str(state.total_file_count))
    t.add_row("Small files", str(state.small_file_count))
    t.add_row("Uncompacted data", f"{state.uncompacted_bytes / (1024*1024):.1f} MB")
    t.add_row("Commits (proxy)", str(state.commits_since_last_compaction))

    if part_analysis:
        t.add_section()
        t.add_row("[bold]Partition analysis[/bold]", "")
        t.add_row("Partitions matched", str(part_analysis.total_partitions))
        t.add_row("Degraded", str(part_analysis.degraded_partitions))
        for rec in part_analysis.recommendations[:10]:
            t.add_row(f"  {rec.partition_key}", f"{rec.action}: {rec.reason}")

    console.print(t)


# ---------------------------------------------------------------------------
# controller (long-running)
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("config_path", type=click.Path(exists=True))
def controller(config_path: str):
    """Start the long-running maintenance controller.

    The controller polls the catalog, evaluates triggers, and executes
    maintenance actions automatically.  It runs until interrupted.
    """
    from iceberg_janitor.runner.controller import controller_from_config

    console.print(f"[bold]Starting controller[/bold] with config: {config_path}")
    ctrl = controller_from_config(config_path)
    ctrl.start()


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
