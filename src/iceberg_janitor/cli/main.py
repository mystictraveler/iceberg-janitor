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


@click.group()
@click.version_option()
def cli():
    """Iceberg Janitor — automated table maintenance for streaming workloads."""


@cli.command()
@click.argument("table_path")
@click.option("--small-file-threshold", default=8 * 1024 * 1024, help="Small file threshold in bytes")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table")
def analyze(table_path: str, small_file_threshold: int, output: str):
    """Analyze an Iceberg table's health using DuckDB."""
    report = assess_table(table_path, small_file_threshold=small_file_threshold)

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
    t = RichTable(title=f"Health Report: {table_path}")
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


@cli.command()
@click.argument("table_id")
@click.option("--catalog-uri", required=True, help="Iceberg REST catalog URI")
@click.option("--warehouse", required=True, help="Warehouse location (e.g., s3://bucket/)")
@click.option("--policy-file", type=click.Path(exists=True), help="YAML policy config file")
@click.option("--dry-run", is_flag=True, help="Show what would be done without doing it")
@click.option("--s3-endpoint", help="S3 endpoint URL (for MinIO)")
def maintain(
    table_id: str,
    catalog_uri: str,
    warehouse: str,
    policy_file: str | None,
    dry_run: bool,
    s3_endpoint: str | None,
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

    # Build catalog
    catalog_props = {
        "uri": catalog_uri,
        "warehouse": warehouse,
    }
    if s3_endpoint:
        catalog_props["s3.endpoint"] = s3_endpoint

    catalog = get_catalog(name="janitor", **catalog_props)

    # Analyze
    # For maintenance, we need the table path for DuckDB analysis
    table = catalog.load_table(table_id)
    table_path = table.metadata.location

    console.print(f"[bold]Analyzing[/bold] {table_id}...")
    report = assess_table(table_path, small_file_threshold=policy.small_file_threshold_bytes)

    # Evaluate policy
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

    # Execute actions
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


@cli.command()
@click.argument("table_path")
@click.option("--output", "-o", type=click.Choice(["table", "json"]), default="table")
def report(table_path: str, output: str):
    """Generate a detailed maintenance report for a table."""
    report_data = assess_table(table_path)
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

    console.print(f"\n[bold]Maintenance Report: {table_path}[/bold]\n")

    if report_data.is_healthy:
        console.print("[green]Table is healthy. No maintenance needed.[/green]")
    else:
        console.print("[yellow]Table needs maintenance:[/yellow]\n")
        for action in actions:
            console.print(f"  [{action.priority}] {action.action_type.value}")
            console.print(f"      {action.reason}\n")


if __name__ == "__main__":
    cli()
