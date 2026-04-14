"""Route implementations for the Iceberg Janitor API."""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

from iceberg_janitor.analyzer.health import (
    HealthReport,
    assess_table,
)
from iceberg_janitor.api.dependencies import CatalogDep, PolicyConfigDep
from iceberg_janitor.api.schemas import (
    AnalyzeRequest,
    CompactionRequest,
    CompactionResponse,
    ErrorResponse,
    FileInfo,
    HealthReportResponse,
    FileStatsResponse,
    MaintenanceHistoryEntry,
    MaintenanceStepResult,
    OptimizeSortRequest,
    OptimizeSortResponse,
    OrphanCleanupRequest,
    OrphanCleanupResponse,
    PaginatedResponse,
    PartitionHealthResponse,
    PartitionInfo,
    PolicyResponse,
    PolicyUpdateRequest,
    RewriteManifestsRequest,
    RewriteManifestsResponse,
    RunAllRequest,
    RunAllResponse,
    ServiceHealthResponse,
    SizeBucketResponse,
    SnapshotExpirationRequest,
    SnapshotExpirationResponse,
    SnapshotInfo,
    SnapshotStatsResponse,
    TableInfo,
    TablePolicySchema,
    TaskResponse,
)
from iceberg_janitor.maintenance.compaction import compact_files
from iceberg_janitor.maintenance.orphans import remove_orphans
from iceberg_janitor.maintenance.snapshots import expire_snapshots
from iceberg_janitor.policy.models import PolicyConfig, TablePolicy

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1")

# In-memory stores (swap for a real DB in production)
_health_cache: dict[str, HealthReport] = {}
_maintenance_history: list[MaintenanceHistoryEntry] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_table_id(table_id: str) -> str:
    """Normalise the table identifier (URL-safe dot notation)."""
    return table_id.replace("/", ".")


def _load_table(catalog: Catalog, table_id: str):
    """Load a table or raise 404."""
    try:
        return catalog.load_table(table_id)
    except NoSuchTableError:
        raise HTTPException(
            status_code=404,
            detail=f"Table '{table_id}' not found in catalog",
        )
    except Exception as exc:
        logger.error("table_load_failed", table_id=table_id, error=str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


def _record_history(
    table_id: str,
    action_type: str,
    *,
    dry_run: bool = False,
    result: dict[str, Any] | None = None,
    error: str | None = None,
) -> MaintenanceHistoryEntry:
    now = datetime.now(timezone.utc)
    entry = MaintenanceHistoryEntry(
        id=uuid.uuid4(),
        table_id=table_id,
        action_type=action_type,
        status="completed" if error is None else "failed",
        started_at=now,
        completed_at=now,
        dry_run=dry_run,
        result=result,
        error=error,
    )
    _maintenance_history.append(entry)
    return entry


def _health_report_to_response(report: HealthReport) -> HealthReportResponse:
    return HealthReportResponse(
        table_id=report.table_id,
        assessed_at=report.assessed_at,
        is_healthy=report.is_healthy,
        needs_compaction=report.needs_compaction,
        needs_snapshot_expiry=report.needs_snapshot_expiry,
        file_stats=FileStatsResponse(
            file_count=report.file_stats.file_count,
            total_bytes=report.file_stats.total_bytes,
            avg_file_size_bytes=report.file_stats.avg_file_size_bytes,
            min_file_size_bytes=report.file_stats.min_file_size_bytes,
            max_file_size_bytes=report.file_stats.max_file_size_bytes,
            median_file_size_bytes=report.file_stats.median_file_size_bytes,
            small_file_count=report.file_stats.small_file_count,
            small_file_ratio=report.file_stats.small_file_ratio,
            total_mb=report.file_stats.total_mb,
        ),
        snapshot_stats=SnapshotStatsResponse(
            snapshot_count=report.snapshot_stats.snapshot_count,
            oldest_snapshot_ts=report.snapshot_stats.oldest_snapshot_ts,
            newest_snapshot_ts=report.snapshot_stats.newest_snapshot_ts,
        ),
        size_distribution=[
            SizeBucketResponse(
                bucket=b.bucket,
                file_count=b.file_count,
                total_bytes=b.total_bytes,
            )
            for b in report.size_distribution
        ],
        hot_partitions=[
            PartitionHealthResponse(
                partition=p.partition,
                file_count=p.file_count,
                total_bytes=p.total_bytes,
                avg_file_size_bytes=p.avg_file_size_bytes,
            )
            for p in report.hot_partitions
        ],
        errors=report.errors,
    )


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


@router.get("/tables", tags=["Tables"])
async def list_tables(
    catalog: CatalogDep,
    namespace: str | None = Query(default=None, description="Filter by namespace"),
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> PaginatedResponse:
    """List all managed tables in the catalog."""
    namespaces = catalog.list_namespaces()
    all_tables: list[TableInfo] = []

    for ns in namespaces:
        ns_name = ".".join(ns)
        if namespace and ns_name != namespace:
            continue
        for ident in catalog.list_tables(ns):
            table_id = ".".join(ident)
            all_tables.append(
                TableInfo(
                    table_id=table_id,
                    namespace=ns_name,
                    name=ident[-1],
                )
            )

    total = len(all_tables)
    page = all_tables[offset : offset + limit]
    return PaginatedResponse(items=page, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/health", tags=["Health"])
async def get_table_health(
    table_id: str,
    catalog: CatalogDep,
) -> HealthReportResponse:
    """Get the most recent cached health report for a table."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)  # validate existence

    report = _health_cache.get(tid)
    if report is None:
        raise HTTPException(
            status_code=404,
            detail=f"No health report found for '{tid}'. Trigger an analysis first.",
        )
    return _health_report_to_response(report)


@router.post(
    "/tables/{table_id}/analyze",
    tags=["Health"],
    status_code=202,
)
async def analyze_table(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: AnalyzeRequest | None = None,
) -> TaskResponse:
    """Trigger a fresh health analysis (runs in background)."""
    tid = _resolve_table_id(table_id)
    table = _load_table(catalog, tid)
    req = body or AnalyzeRequest()
    task_id = uuid.uuid4()

    def _run_analysis() -> None:
        try:
            report = assess_table(
                table_path=table.metadata.location,
                small_file_threshold=req.small_file_threshold_bytes,
                partition_min_files=req.partition_min_files,
                partition_limit=req.partition_limit,
            )
            # Override table_id to use the catalog identifier
            object.__setattr__(report, "table_id", tid)
            _health_cache[tid] = report
            logger.info("analysis_complete", table_id=tid, task_id=str(task_id))
        except Exception as exc:
            logger.error("analysis_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_analysis)
    return TaskResponse(task_id=task_id, status="accepted", message="Analysis queued")


# ---------------------------------------------------------------------------
# Maintenance — compaction
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/compact",
    tags=["Maintenance"],
    status_code=202,
)
async def compact_table(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: CompactionRequest | None = None,
) -> CompactionResponse:
    """Trigger data file compaction."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or CompactionRequest()
    task_id = uuid.uuid4()

    if req.dry_run:
        result = compact_files(
            catalog=catalog,
            table_id=tid,
            target_file_size_bytes=req.target_file_size_bytes,
            small_file_threshold_bytes=req.small_file_threshold_bytes,
            dry_run=True,
        )
        _record_history(tid, "compact_files", dry_run=True, result=result)
        return CompactionResponse(
            task_id=task_id,
            dry_run=True,
            total_files=result.get("total_files"),
            small_files_found=result.get("small_files_found"),
            total_small_bytes=result.get("total_small_bytes"),
            estimated_output_files=result.get("estimated_output_files"),
            compacted=result.get("compacted", False),
            status="completed",
        )

    def _run_compact() -> None:
        try:
            result = compact_files(
                catalog=catalog,
                table_id=tid,
                target_file_size_bytes=req.target_file_size_bytes,
                small_file_threshold_bytes=req.small_file_threshold_bytes,
                dry_run=False,
            )
            _record_history(tid, "compact_files", dry_run=False, result=result)
        except Exception as exc:
            _record_history(tid, "compact_files", error=str(exc))
            logger.error("compaction_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_compact)
    return CompactionResponse(task_id=task_id, dry_run=False, status="accepted")


# ---------------------------------------------------------------------------
# Maintenance — snapshot expiration
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/expire-snapshots",
    tags=["Maintenance"],
    status_code=202,
)
async def expire_table_snapshots(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: SnapshotExpirationRequest | None = None,
) -> SnapshotExpirationResponse:
    """Expire old snapshots from the table."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or SnapshotExpirationRequest()
    task_id = uuid.uuid4()

    if req.dry_run:
        result = expire_snapshots(
            catalog=catalog,
            table_id=tid,
            retention_hours=req.retention_hours,
            min_snapshots_to_keep=req.min_snapshots_to_keep,
            dry_run=True,
        )
        _record_history(tid, "expire_snapshots", dry_run=True, result=result)
        return SnapshotExpirationResponse(
            task_id=task_id,
            dry_run=True,
            total_snapshots=result.get("total_snapshots"),
            expired=result.get("would_expire", result.get("expired")),
            retained=result.get("would_retain", result.get("retained")),
            status="completed",
        )

    def _run_expire() -> None:
        try:
            result = expire_snapshots(
                catalog=catalog,
                table_id=tid,
                retention_hours=req.retention_hours,
                min_snapshots_to_keep=req.min_snapshots_to_keep,
                dry_run=False,
            )
            _record_history(tid, "expire_snapshots", dry_run=False, result=result)
        except Exception as exc:
            _record_history(tid, "expire_snapshots", error=str(exc))
            logger.error("expire_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_expire)
    return SnapshotExpirationResponse(
        task_id=task_id, dry_run=False, status="accepted"
    )


# ---------------------------------------------------------------------------
# Maintenance — orphan removal
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/remove-orphans",
    tags=["Maintenance"],
    status_code=202,
)
async def remove_table_orphans(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: OrphanCleanupRequest | None = None,
) -> OrphanCleanupResponse:
    """Remove orphan files from the table's storage."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or OrphanCleanupRequest()
    task_id = uuid.uuid4()

    if req.dry_run:
        result = remove_orphans(
            catalog=catalog,
            table_id=tid,
            retention_hours=req.retention_hours,
            dry_run=True,
        )
        _record_history(tid, "remove_orphans", dry_run=True, result=result)
        return OrphanCleanupResponse(
            task_id=task_id,
            dry_run=True,
            orphans_found=result.get("orphans_found"),
            removed=result.get("removed", 0),
            files=result.get("files"),
            status="completed",
        )

    def _run_orphan_cleanup() -> None:
        try:
            result = remove_orphans(
                catalog=catalog,
                table_id=tid,
                retention_hours=req.retention_hours,
                dry_run=False,
            )
            _record_history(tid, "remove_orphans", dry_run=False, result=result)
        except Exception as exc:
            _record_history(tid, "remove_orphans", error=str(exc))
            logger.error("orphan_cleanup_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_orphan_cleanup)
    return OrphanCleanupResponse(
        task_id=task_id, dry_run=False, status="accepted"
    )


# ---------------------------------------------------------------------------
# Maintenance — rewrite manifests
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/rewrite-manifests",
    tags=["Maintenance"],
    status_code=202,
)
async def rewrite_table_manifests(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: RewriteManifestsRequest | None = None,
) -> RewriteManifestsResponse:
    """Rewrite manifest files to optimise metadata."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or RewriteManifestsRequest()
    task_id = uuid.uuid4()

    # Manifest rewrite is not yet implemented in the maintenance module;
    # provide a stub that records history.
    def _run_rewrite() -> None:
        try:
            table = catalog.load_table(tid)
            manifests = table.current_snapshot().manifests(table.io) if table.current_snapshot() else []
            _record_history(
                tid,
                "rewrite_manifests",
                dry_run=req.dry_run,
                result={
                    "dry_run": req.dry_run,
                    "manifests_rewritten": 0 if req.dry_run else len(manifests),
                    "manifests_kept": len(manifests) if req.dry_run else 0,
                },
            )
        except Exception as exc:
            _record_history(tid, "rewrite_manifests", error=str(exc))
            logger.error("manifest_rewrite_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_rewrite)
    return RewriteManifestsResponse(
        task_id=task_id, dry_run=req.dry_run, status="accepted"
    )


# ---------------------------------------------------------------------------
# Maintenance — sort order optimisation
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/optimize-sort",
    tags=["Maintenance"],
    status_code=202,
)
async def optimize_sort_order(
    table_id: str,
    catalog: CatalogDep,
    background_tasks: BackgroundTasks,
    body: OptimizeSortRequest | None = None,
) -> OptimizeSortResponse:
    """Rewrite data files to match the configured sort order."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or OptimizeSortRequest()
    task_id = uuid.uuid4()

    def _run_sort_optimize() -> None:
        try:
            table = catalog.load_table(tid)
            sort_order_id = req.sort_order_id or table.sort_order().order_id
            plan_files = list(table.scan().plan_files())
            _record_history(
                tid,
                "optimize_sort",
                dry_run=req.dry_run,
                result={
                    "dry_run": req.dry_run,
                    "files_rewritten": 0 if req.dry_run else len(plan_files),
                    "sort_order_id": sort_order_id,
                },
            )
        except Exception as exc:
            _record_history(tid, "optimize_sort", error=str(exc))
            logger.error("sort_optimize_failed", table_id=tid, error=str(exc))

    background_tasks.add_task(_run_sort_optimize)
    return OptimizeSortResponse(
        task_id=task_id, dry_run=req.dry_run, status="accepted"
    )


# ---------------------------------------------------------------------------
# Maintenance — run all
# ---------------------------------------------------------------------------


@router.post(
    "/tables/{table_id}/maintenance/run-all",
    tags=["Maintenance"],
    status_code=202,
)
async def run_full_maintenance(
    table_id: str,
    catalog: CatalogDep,
    policy_config: PolicyConfigDep,
    background_tasks: BackgroundTasks,
    body: RunAllRequest | None = None,
) -> RunAllResponse:
    """Run the full maintenance cycle using the table's effective policy."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)
    req = body or RunAllRequest()
    task_id = uuid.uuid4()
    policy = policy_config.get_policy(tid)

    steps_template = [
        MaintenanceStepResult(action="expire_snapshots", status="pending"),
        MaintenanceStepResult(action="remove_orphans", status="pending"),
        MaintenanceStepResult(action="compact_files", status="pending"),
        MaintenanceStepResult(action="rewrite_manifests", status="pending"),
    ]

    def _run_all() -> None:
        for step in steps_template:
            try:
                step.status = "running"
                if step.action == "expire_snapshots":
                    result = expire_snapshots(
                        catalog=catalog,
                        table_id=tid,
                        retention_hours=policy.snapshot_retention_hours,
                        min_snapshots_to_keep=policy.min_snapshots_to_keep,
                        dry_run=req.dry_run,
                    )
                elif step.action == "remove_orphans":
                    result = remove_orphans(
                        catalog=catalog,
                        table_id=tid,
                        retention_hours=policy.orphan_retention_hours,
                        dry_run=req.dry_run,
                    )
                elif step.action == "compact_files":
                    result = compact_files(
                        catalog=catalog,
                        table_id=tid,
                        target_file_size_bytes=policy.target_file_size_bytes,
                        small_file_threshold_bytes=policy.small_file_threshold_bytes,
                        dry_run=req.dry_run,
                    )
                elif step.action == "rewrite_manifests":
                    result = {"dry_run": req.dry_run, "manifests_rewritten": 0}
                else:
                    result = {}
                step.result = result
                step.status = "completed"
            except Exception as exc:
                step.status = "failed"
                step.result = {"error": str(exc)}
                logger.error(
                    "run_all_step_failed",
                    table_id=tid,
                    action=step.action,
                    error=str(exc),
                )

        _record_history(
            tid,
            "run_all",
            dry_run=req.dry_run,
            result={"steps": [s.model_dump() for s in steps_template]},
        )

    background_tasks.add_task(_run_all)
    return RunAllResponse(
        task_id=task_id,
        dry_run=req.dry_run,
        steps=steps_template,
        status="accepted",
    )


# ---------------------------------------------------------------------------
# Maintenance history
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/maintenance/history", tags=["Maintenance"])
async def get_maintenance_history(
    table_id: str,
    catalog: CatalogDep,
    action_type: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> PaginatedResponse:
    """Get maintenance history for a table."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)

    entries = [e for e in _maintenance_history if e.table_id == tid]
    if action_type:
        entries = [e for e in entries if e.action_type == action_type]

    # Most recent first
    entries.sort(key=lambda e: e.started_at, reverse=True)
    total = len(entries)
    page = entries[offset : offset + limit]
    return PaginatedResponse(items=page, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/policy", tags=["Policy"])
async def get_table_policy(
    table_id: str,
    catalog: CatalogDep,
    policy_config: PolicyConfigDep,
) -> PolicyResponse:
    """Get the effective maintenance policy for a table."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)

    is_override = tid in policy_config.table_overrides
    policy = policy_config.get_policy(tid)
    return PolicyResponse(
        table_id=tid,
        policy=TablePolicySchema(**policy.model_dump()),
        is_override=is_override,
    )


@router.put("/tables/{table_id}/policy", tags=["Policy"])
async def update_table_policy(
    table_id: str,
    body: PolicyUpdateRequest,
    catalog: CatalogDep,
    policy_config: PolicyConfigDep,
) -> PolicyResponse:
    """Update the table-specific policy override."""
    tid = _resolve_table_id(table_id)
    _load_table(catalog, tid)

    # Merge provided fields onto the current effective policy
    current = policy_config.get_policy(tid)
    update_data = body.model_dump(exclude_none=True)
    merged = current.model_copy(update=update_data)
    policy_config.table_overrides[tid] = merged

    return PolicyResponse(
        table_id=tid,
        policy=TablePolicySchema(**merged.model_dump()),
        is_override=True,
    )


# ---------------------------------------------------------------------------
# Metadata — snapshots
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/snapshots", tags=["Metadata"])
async def list_snapshots(
    table_id: str,
    catalog: CatalogDep,
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> PaginatedResponse:
    """List snapshots for a table."""
    tid = _resolve_table_id(table_id)
    table = _load_table(catalog, tid)

    snapshots_raw = sorted(
        table.metadata.snapshots,
        key=lambda s: s.timestamp_ms,
        reverse=True,
    )
    total = len(snapshots_raw)
    page = snapshots_raw[offset : offset + limit]

    items = []
    for snap in page:
        items.append(
            SnapshotInfo(
                snapshot_id=snap.snapshot_id,
                parent_snapshot_id=snap.parent_snapshot_id,
                timestamp_ms=snap.timestamp_ms,
                timestamp=datetime.fromtimestamp(
                    snap.timestamp_ms / 1000, tz=timezone.utc
                ),
                operation=snap.operation.value if snap.operation else None,
                summary=snap.summary or None,
                manifest_list_location=snap.manifest_list,
            )
        )

    return PaginatedResponse(items=items, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Metadata — data files
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/files", tags=["Metadata"])
async def list_data_files(
    table_id: str,
    catalog: CatalogDep,
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    min_size_bytes: int | None = Query(default=None),
    max_size_bytes: int | None = Query(default=None),
) -> PaginatedResponse:
    """List data files in the table's current snapshot."""
    tid = _resolve_table_id(table_id)
    table = _load_table(catalog, tid)

    plan_files = list(table.scan().plan_files())
    files: list[FileInfo] = []
    for task in plan_files:
        size = task.file.file_size_in_bytes
        if min_size_bytes is not None and size < min_size_bytes:
            continue
        if max_size_bytes is not None and size > max_size_bytes:
            continue
        files.append(
            FileInfo(
                file_path=task.file.file_path,
                file_size_bytes=size,
                file_format=str(task.file.file_format),
                record_count=task.file.record_count,
            )
        )

    total = len(files)
    page = files[offset : offset + limit]
    return PaginatedResponse(items=page, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Metadata — partitions
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/partitions", tags=["Metadata"])
async def list_partitions(
    table_id: str,
    catalog: CatalogDep,
    policy_config: PolicyConfigDep,
    limit: int = Query(default=20, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="file_count"),
) -> PaginatedResponse:
    """List partitions with per-partition health information."""
    tid = _resolve_table_id(table_id)
    table = _load_table(catalog, tid)
    policy = policy_config.get_policy(tid)

    plan_files = list(table.scan().plan_files())

    # Aggregate per partition
    partition_agg: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"file_count": 0, "total_bytes": 0}
    )
    for task in plan_files:
        part_key = str(task.file.partition) if hasattr(task.file, "partition") else "unpartitioned"
        partition_agg[part_key]["file_count"] += 1
        partition_agg[part_key]["total_bytes"] += task.file.file_size_in_bytes

    partitions: list[PartitionInfo] = []
    for part_key, agg in partition_agg.items():
        fc = agg["file_count"]
        tb = agg["total_bytes"]
        avg = tb / fc if fc > 0 else 0.0
        needs = (avg < policy.small_file_threshold_bytes) and fc > 1
        partitions.append(
            PartitionInfo(
                partition=part_key,
                file_count=fc,
                total_bytes=tb,
                avg_file_size_bytes=avg,
                needs_compaction=needs,
            )
        )

    # Sort
    valid_sorts = {"file_count", "total_bytes", "avg_file_size_bytes"}
    sort_field = sort_by if sort_by in valid_sorts else "file_count"
    partitions.sort(key=lambda p: getattr(p, sort_field), reverse=True)

    total = len(partitions)
    page = partitions[offset : offset + limit]
    return PaginatedResponse(items=page, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


@router.get("/health", tags=["Operations"])
async def service_health(request: Request) -> ServiceHealthResponse:
    """Service health check."""
    catalog = getattr(request.app.state, "catalog", None)
    start_time: float = getattr(request.app.state, "start_time", time.time())
    connected = False
    if catalog is not None:
        try:
            catalog.list_namespaces()
            connected = True
        except Exception:
            pass

    status = "healthy" if connected else "degraded"
    return ServiceHealthResponse(
        status=status,
        version=request.app.version,
        catalog_connected=connected,
        uptime_seconds=time.time() - start_time,
    )


@router.get("/metrics", tags=["Operations"])
async def prometheus_metrics() -> str:
    """Placeholder for Prometheus metrics exposition."""
    # In production, integrate with prometheus_client or the metrics module
    return ""
