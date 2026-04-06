"""Long-running controller — watches tables and executes maintenance based on strategy.

The controller is designed to run as a persistent process (e.g. a Kubernetes
Deployment).  It periodically polls the catalog for table changes, evaluates
triggers via the :class:`MaintenanceScheduler`, and dispatches maintenance
actions.  Prometheus metrics are exposed for observability.
"""

from __future__ import annotations

import signal
import threading
import time
from datetime import datetime, time as dt_time, timezone
from pathlib import Path
from typing import Any

import structlog
import yaml

from pyiceberg.catalog import Catalog

from iceberg_janitor.analyzer.health import assess_table
from iceberg_janitor.catalog import get_catalog
from iceberg_janitor.maintenance.compaction import compact_files
from iceberg_janitor.maintenance.orphans import remove_orphans
from iceberg_janitor.maintenance.snapshots import expire_snapshots
from iceberg_janitor.metrics.prometheus import (
    compaction_runs,
    maintenance_duration_seconds,
    orphan_removals,
    orphans_removed_count,
    snapshot_expirations,
    snapshots_expired_count,
    update_health_metrics,
)
from iceberg_janitor.policy.engine import evaluate
from iceberg_janitor.policy.models import ActionType, PolicyConfig
from iceberg_janitor.strategy.partition import PartitionAnalyzer
from iceberg_janitor.strategy.scheduler import MaintenanceScheduler, TableState

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Prometheus metrics specific to the controller
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Gauge

    trigger_evaluations_total = Counter(
        "janitor_trigger_evaluations_total",
        "Total trigger evaluations performed",
        ["table_id", "result"],
    )
    controller_active_tables = Gauge(
        "janitor_controller_active_tables",
        "Number of tables actively monitored by the controller",
    )
    controller_poll_duration = Gauge(
        "janitor_controller_poll_duration_seconds",
        "Duration of the last catalog poll cycle",
    )
except Exception:  # pragma: no cover — prometheus_client is optional at import time
    trigger_evaluations_total = None  # type: ignore[assignment]
    controller_active_tables = None  # type: ignore[assignment]
    controller_poll_duration = None  # type: ignore[assignment]


class MaintenanceController:
    """Long-running controller that watches for table changes and runs maintenance.

    Parameters
    ----------
    catalog:
        A connected PyIceberg catalog.
    policy_config:
        The resolved :class:`PolicyConfig`.
    poll_interval_seconds:
        How frequently to poll the catalog for changes (default 60 s).
    table_ids:
        Explicit list of tables to monitor.  If empty, tables are
        auto-discovered from the catalog on each poll.
    max_concurrent_compactions:
        Global rate limit for simultaneous compactions.
    maintenance_window_start / maintenance_window_end:
        Optional UTC time boundaries for maintenance (HH:MM strings).
    """

    def __init__(
        self,
        catalog: Catalog,
        policy_config: PolicyConfig,
        poll_interval_seconds: int = 60,
        table_ids: list[str] | None = None,
        max_concurrent_compactions: int = 2,
        maintenance_window_start: str | None = None,
        maintenance_window_end: str | None = None,
    ) -> None:
        self.catalog = catalog
        self.policy_config = policy_config
        self.poll_interval_seconds = poll_interval_seconds
        self.explicit_table_ids = table_ids or []
        self._stop_event = threading.Event()

        window_start = _parse_time(maintenance_window_start)
        window_end = _parse_time(maintenance_window_end)

        self.scheduler = MaintenanceScheduler(
            max_concurrent=max_concurrent_compactions,
            maintenance_window_start=window_start,
            maintenance_window_end=window_end,
        )
        self.partition_analyzer = PartitionAnalyzer()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the controller loop (blocking).  Handles SIGINT/SIGTERM."""
        log = logger.bind(poll_interval=self.poll_interval_seconds)
        log.info("controller_starting")

        # Graceful shutdown on signals
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

        self._initial_registration()

        while not self._stop_event.is_set():
            try:
                self._poll_cycle()
            except Exception:
                logger.exception("poll_cycle_error")
            self._stop_event.wait(timeout=self.poll_interval_seconds)

        log.info("controller_stopped")

    def stop(self) -> None:
        """Request a graceful shutdown."""
        logger.info("controller_stop_requested")
        self._stop_event.set()

    def _handle_signal(self, signum: int, frame: Any) -> None:
        logger.info("signal_received", signal=signum)
        self.stop()

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def _initial_registration(self) -> None:
        """Register all tables with the scheduler on startup."""
        table_ids = self._resolve_table_ids()
        for tid in table_ids:
            policy = self.policy_config.get_policy(tid)
            if not policy.enabled:
                continue
            if policy.trigger_mode == "manual":
                # Manual-only tables are not registered for auto-evaluation
                continue
            self.scheduler.register_table(tid, policy)

        if controller_active_tables is not None:
            controller_active_tables.set(len(self.scheduler.table_ids()))

        logger.info(
            "initial_registration_complete",
            registered=len(self.scheduler.table_ids()),
        )

    def _resolve_table_ids(self) -> list[str]:
        """Return explicit table list or auto-discover from catalog."""
        if self.explicit_table_ids:
            return list(self.explicit_table_ids)

        discovered: list[str] = []
        for namespace in self.catalog.list_namespaces():
            ns_name = ".".join(namespace)
            for table_ident in self.catalog.list_tables(namespace):
                discovered.append(f"{ns_name}.{table_ident[-1]}")
        return discovered

    # ------------------------------------------------------------------
    # Poll cycle
    # ------------------------------------------------------------------

    def _poll_cycle(self) -> None:
        """Execute one poll: detect changes, evaluate triggers, run maintenance."""
        poll_start = time.monotonic()
        log = logger.bind(cycle="poll")

        # 1. Detect changes via snapshot IDs
        table_ids = self._resolve_table_ids()
        for tid in table_ids:
            try:
                table = self.catalog.load_table(tid)
                current_snap = table.current_snapshot()
                if current_snap is not None:
                    self.scheduler.record_new_commit(tid, current_snap.snapshot_id)

                # Refresh file stats into state
                self._refresh_table_stats(tid, table)
            except Exception:
                log.warning("table_poll_failed", table_id=tid, exc_info=True)

        # 2. Evaluate triggers
        actions = self.scheduler.evaluate_all()
        log.info("trigger_evaluation_complete", actions=len(actions))

        for sa in actions:
            if trigger_evaluations_total is not None:
                trigger_evaluations_total.labels(
                    table_id=sa.table_id,
                    result="fired",
                ).inc()

        # 3. Execute maintenance for fired tables (rate-limited)
        for sa in actions:
            if not self.scheduler.acquire_compaction_slot():
                log.info("compaction_rate_limited", table_id=sa.table_id)
                break

            try:
                self._execute_maintenance(sa.table_id)
                self.scheduler.record_compaction_done(sa.table_id)
            except Exception:
                log.error("maintenance_failed", table_id=sa.table_id, exc_info=True)
            finally:
                self.scheduler.release_compaction_slot()

        poll_duration = time.monotonic() - poll_start
        if controller_poll_duration is not None:
            controller_poll_duration.set(poll_duration)

    def _refresh_table_stats(self, table_id: str, table: Any) -> None:
        """Update scheduler state with current table file statistics."""
        try:
            plan_files = list(table.scan().plan_files())
            policy = self.policy_config.get_policy(table_id)

            small_count = sum(
                1
                for t in plan_files
                if t.file.file_size_in_bytes < policy.small_file_threshold_bytes
            )
            total_bytes = sum(t.file.file_size_in_bytes for t in plan_files)
            small_bytes = sum(
                t.file.file_size_in_bytes
                for t in plan_files
                if t.file.file_size_in_bytes < policy.small_file_threshold_bytes
            )

            health_score = 1.0
            if len(plan_files) > 0:
                ratio = small_count / len(plan_files)
                health_score = max(0.0, 1.0 - ratio)

            self.scheduler.update_state(
                table_id,
                small_file_count=small_count,
                total_file_count=len(plan_files),
                uncompacted_bytes=small_bytes,
                last_health_score=health_score,
            )
        except Exception:
            logger.warning("stats_refresh_failed", table_id=table_id, exc_info=True)

    def _execute_maintenance(self, table_id: str) -> None:
        """Run full maintenance (policy-evaluated) for a single table."""
        log = logger.bind(table_id=table_id)
        log.info("executing_maintenance")

        policy = self.policy_config.get_policy(table_id)
        table = self.catalog.load_table(table_id)
        table_path = table.metadata.location

        # Analyze
        report = assess_table(
            table_path,
            small_file_threshold=policy.small_file_threshold_bytes,
        )
        update_health_metrics(table_id, report)

        # Evaluate policy
        actions = evaluate(report, policy)

        for action in actions:
            start = time.time()
            try:
                if action.action_type == ActionType.COMPACT_FILES:
                    result = compact_files(self.catalog, table_id, **action.params)
                    compaction_runs.labels(table_id=table_id, status="success").inc()
                elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                    result = expire_snapshots(self.catalog, table_id, **action.params)
                    snapshot_expirations.labels(table_id=table_id, status="success").inc()
                    snapshots_expired_count.labels(table_id=table_id).inc(
                        result.get("expired", 0)
                    )
                elif action.action_type == ActionType.REMOVE_ORPHANS:
                    result = remove_orphans(self.catalog, table_id, **action.params)
                    orphan_removals.labels(table_id=table_id, status="success").inc()
                    orphans_removed_count.labels(table_id=table_id).inc(
                        result.get("removed", 0)
                    )
                else:
                    continue

                duration = time.time() - start
                maintenance_duration_seconds.labels(
                    table_id=table_id, action_type=action.action_type.value
                ).observe(duration)
                log.info(
                    "action_complete",
                    action=action.action_type.value,
                    duration=round(duration, 2),
                )
            except Exception as exc:
                log.error(
                    "action_failed",
                    action=action.action_type.value,
                    error=str(exc),
                )
                if action.action_type == ActionType.COMPACT_FILES:
                    compaction_runs.labels(table_id=table_id, status="error").inc()
                elif action.action_type == ActionType.EXPIRE_SNAPSHOTS:
                    snapshot_expirations.labels(table_id=table_id, status="error").inc()


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def controller_from_config(config_path: str) -> MaintenanceController:
    """Build a :class:`MaintenanceController` from a YAML config file.

    This is the recommended entrypoint for production use.
    """
    with open(config_path) as fh:
        raw = yaml.safe_load(fh)

    catalog_cfg = raw.get("catalog", {})
    catalog = get_catalog(
        name=catalog_cfg.get("name", "default"),
        uri=catalog_cfg.get("uri"),
        warehouse=catalog_cfg.get("warehouse"),
        catalog_type=catalog_cfg.get("type", "rest"),
        **{
            k: v
            for k, v in catalog_cfg.items()
            if k not in ("name", "uri", "warehouse", "type")
        },
    )

    policy_config = PolicyConfig.model_validate(raw.get("policy", {}))
    table_ids: list[str] = raw.get("tables", [])

    strategy_cfg = raw.get("strategy", {})

    return MaintenanceController(
        catalog=catalog,
        policy_config=policy_config,
        poll_interval_seconds=strategy_cfg.get("poll_interval_seconds", 60),
        table_ids=table_ids or None,
        max_concurrent_compactions=strategy_cfg.get("max_concurrent_compactions", 2),
        maintenance_window_start=strategy_cfg.get("maintenance_window_start"),
        maintenance_window_end=strategy_cfg.get("maintenance_window_end"),
    )


def _parse_time(value: str | None) -> dt_time | None:
    """Parse an ``HH:MM`` string into a :class:`datetime.time`, or ``None``."""
    if value is None:
        return None
    parts = value.strip().split(":")
    return dt_time(int(parts[0]), int(parts[1]))
