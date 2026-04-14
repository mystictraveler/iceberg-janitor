"""Access frequency tracking for adaptive maintenance scheduling.

Tracks per-table query access patterns to classify tables as hot, warm,
or cold.  The heat classification drives compaction priority: hot tables
get compacted more aggressively because query performance matters most.

Thread-safe — all mutations go through a lock so the tracker can be
shared across the async API and the background controller loop.
"""

from __future__ import annotations

import json
import math
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import structlog

logger = structlog.get_logger()

AccessClassification = Literal["hot", "warm", "cold"]

# Default rolling-window sizes in seconds
_1H = 3600
_6H = 6 * 3600
_24H = 24 * 3600

# Default thresholds (queries per hour)
_DEFAULT_HOT_QPH = 100
_DEFAULT_WARM_QPH = 10


@dataclass(frozen=True)
class AccessEvent:
    """A single recorded access to an Iceberg table."""

    table_id: str
    timestamp: float = field(default_factory=time.time)
    query_latency_ms: float | None = None
    client_id: str | None = None
    source: str = "api"  # "api" | "prometheus" | "duckdb"


@dataclass
class _TableAccessState:
    """Internal mutable state for one table's access history."""

    # Append-only event log (bounded by max_events)
    events: deque[AccessEvent] = field(default_factory=lambda: deque(maxlen=100_000))

    # Rolling counters — maintained lazily on read
    unique_clients: set[str] = field(default_factory=set)

    # Latency tracking (exponential moving average)
    latency_ema_ms: float = 0.0
    _latency_alpha: float = 0.1

    # Feedback adjustment — multiplier applied by the feedback loop
    priority_adjustment: float = 1.0


class AccessTracker:
    """Per-table access frequency tracker with decay and persistence.

    Parameters
    ----------
    hot_threshold_qph:
        Queries-per-hour above which a table is classified as *hot*.
    warm_threshold_qph:
        Queries-per-hour above which a table is *warm* (below = *cold*).
    persistence_path:
        Optional filesystem path for JSON persistence so state survives
        restarts.  Set to ``None`` to disable persistence.
    decay_half_life_seconds:
        Half-life for the exponential decay applied to access counts.
        After this many seconds, an old event contributes half as much
        to the heat score as a brand-new one.
    """

    def __init__(
        self,
        hot_threshold_qph: int = _DEFAULT_HOT_QPH,
        warm_threshold_qph: int = _DEFAULT_WARM_QPH,
        persistence_path: str | Path | None = None,
        decay_half_life_seconds: float = _6H,
    ) -> None:
        self.hot_threshold_qph = hot_threshold_qph
        self.warm_threshold_qph = warm_threshold_qph
        self.persistence_path = Path(persistence_path) if persistence_path else None
        self.decay_half_life_seconds = decay_half_life_seconds

        self._lock = threading.Lock()
        self._tables: dict[str, _TableAccessState] = defaultdict(_TableAccessState)

        # Pre-compute the decay constant: lambda = ln(2) / half_life
        self._decay_lambda = math.log(2) / max(self.decay_half_life_seconds, 1.0)

        if self.persistence_path and self.persistence_path.exists():
            self._load_state()

    # ------------------------------------------------------------------
    # Public — recording events
    # ------------------------------------------------------------------

    def record_event(self, event: AccessEvent) -> None:
        """Record a single access event (thread-safe)."""
        with self._lock:
            state = self._tables[event.table_id]
            state.events.append(event)

            if event.client_id:
                state.unique_clients.add(event.client_id)

            # Update latency EMA
            if event.query_latency_ms is not None and event.query_latency_ms > 0:
                if state.latency_ema_ms == 0.0:
                    state.latency_ema_ms = event.query_latency_ms
                else:
                    alpha = state._latency_alpha
                    state.latency_ema_ms = (
                        alpha * event.query_latency_ms
                        + (1 - alpha) * state.latency_ema_ms
                    )

    def record_events(self, events: list[AccessEvent]) -> None:
        """Record a batch of access events."""
        for ev in events:
            self.record_event(ev)

    def ingest_from_prometheus(
        self,
        table_id: str,
        query_count: int,
        avg_latency_ms: float | None = None,
    ) -> None:
        """Ingest aggregated metrics from a Prometheus scrape.

        Creates synthetic events so the rolling-window logic works uniformly.
        """
        now = time.time()
        events = [
            AccessEvent(
                table_id=table_id,
                timestamp=now,
                query_latency_ms=avg_latency_ms,
                source="prometheus",
            )
            for _ in range(query_count)
        ]
        self.record_events(events)

    def ingest_from_duckdb_log(
        self,
        table_id: str,
        query_count: int,
        avg_latency_ms: float | None = None,
        client_id: str | None = None,
    ) -> None:
        """Ingest aggregated metrics from DuckDB query logs."""
        now = time.time()
        events = [
            AccessEvent(
                table_id=table_id,
                timestamp=now,
                query_latency_ms=avg_latency_ms,
                client_id=client_id,
                source="duckdb",
            )
            for _ in range(query_count)
        ]
        self.record_events(events)

    # ------------------------------------------------------------------
    # Public — querying
    # ------------------------------------------------------------------

    def get_heat_score(self, table_id: str) -> float:
        """Return a normalised 0.0-1.0 heat score for *table_id*.

        The score blends three signals:
        1. **Frequency** — decayed query count over the last 24 h,
           normalised against the hot threshold.
        2. **Recency** — how recently the table was accessed (exponential
           decay from the most recent event).
        3. **Latency** — high average latency suggests the table is
           suffering from small-file overhead and would benefit from
           compaction, so it boosts the score.

        Weights: frequency 60 %, recency 25 %, latency 15 %.
        """
        with self._lock:
            state = self._tables.get(table_id)
            if state is None or len(state.events) == 0:
                return 0.0
            return self._compute_heat_score(state)

    def get_classification(self, table_id: str) -> AccessClassification:
        """Classify a table as ``"hot"``, ``"warm"``, or ``"cold"``."""
        with self._lock:
            state = self._tables.get(table_id)
            if state is None or len(state.events) == 0:
                return "cold"
            qph = self._decayed_queries_per_hour(state, window_seconds=_1H)

        if qph >= self.hot_threshold_qph:
            return "hot"
        if qph >= self.warm_threshold_qph:
            return "warm"
        return "cold"

    def get_query_counts(self, table_id: str) -> dict[str, float]:
        """Return decayed query counts for the 1 h, 6 h, and 24 h windows."""
        with self._lock:
            state = self._tables.get(table_id)
            if state is None:
                return {"1h": 0.0, "6h": 0.0, "24h": 0.0}
            return {
                "1h": round(self._decayed_query_count(state, _1H), 2),
                "6h": round(self._decayed_query_count(state, _6H), 2),
                "24h": round(self._decayed_query_count(state, _24H), 2),
            }

    def get_access_stats(self, table_id: str) -> dict[str, Any]:
        """Return a JSON-serialisable summary of access stats for *table_id*."""
        with self._lock:
            state = self._tables.get(table_id)
            if state is None:
                return {
                    "table_id": table_id,
                    "classification": "cold",
                    "heat_score": 0.0,
                    "query_counts": {"1h": 0.0, "6h": 0.0, "24h": 0.0},
                    "avg_latency_ms": 0.0,
                    "unique_clients": 0,
                    "last_access": None,
                    "total_events": 0,
                }

            last_ts = state.events[-1].timestamp if state.events else None
            return {
                "table_id": table_id,
                "classification": self._classify_unlocked(state),
                "heat_score": round(self._compute_heat_score(state), 4),
                "query_counts": {
                    "1h": round(self._decayed_query_count(state, _1H), 2),
                    "6h": round(self._decayed_query_count(state, _6H), 2),
                    "24h": round(self._decayed_query_count(state, _24H), 2),
                },
                "avg_latency_ms": round(state.latency_ema_ms, 2),
                "unique_clients": len(state.unique_clients),
                "last_access": (
                    datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
                    if last_ts
                    else None
                ),
                "total_events": len(state.events),
            }

    def get_all_rankings(self) -> list[dict[str, Any]]:
        """Return all tracked tables ranked by heat score (descending)."""
        with self._lock:
            rankings = []
            for table_id, state in self._tables.items():
                if len(state.events) == 0:
                    continue
                rankings.append(
                    {
                        "table_id": table_id,
                        "heat_score": round(self._compute_heat_score(state), 4),
                        "classification": self._classify_unlocked(state),
                        "queries_1h": round(
                            self._decayed_query_count(state, _1H), 2
                        ),
                        "avg_latency_ms": round(state.latency_ema_ms, 2),
                        "priority_adjustment": state.priority_adjustment,
                    }
                )
        rankings.sort(key=lambda r: r["heat_score"], reverse=True)
        return rankings

    def get_priority_adjustment(self, table_id: str) -> float:
        """Return the feedback-loop priority adjustment for *table_id*."""
        with self._lock:
            state = self._tables.get(table_id)
            if state is None:
                return 1.0
            return state.priority_adjustment

    def set_priority_adjustment(self, table_id: str, value: float) -> None:
        """Set the feedback-loop priority adjustment for *table_id*."""
        with self._lock:
            self._tables[table_id].priority_adjustment = max(0.0, value)

    def table_ids(self) -> list[str]:
        """Return all tracked table IDs."""
        with self._lock:
            return list(self._tables.keys())

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def persist(self) -> None:
        """Write the current state to the configured persistence path."""
        if self.persistence_path is None:
            return

        with self._lock:
            data = self._serialise_state()

        self.persistence_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.persistence_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2))
        tmp.replace(self.persistence_path)
        logger.debug("access_tracker_persisted", path=str(self.persistence_path))

    def _load_state(self) -> None:
        """Load persisted state from disk (called at init, no lock needed)."""
        if self.persistence_path is None or not self.persistence_path.exists():
            return
        try:
            raw = json.loads(self.persistence_path.read_text())
            for table_id, tdata in raw.get("tables", {}).items():
                state = _TableAccessState()
                for ev_dict in tdata.get("events", []):
                    state.events.append(AccessEvent(**ev_dict))
                state.latency_ema_ms = tdata.get("latency_ema_ms", 0.0)
                state.priority_adjustment = tdata.get("priority_adjustment", 1.0)
                state.unique_clients = set(tdata.get("unique_clients", []))
                self._tables[table_id] = state
            logger.info(
                "access_tracker_loaded",
                path=str(self.persistence_path),
                tables=len(self._tables),
            )
        except Exception as exc:
            logger.warning("access_tracker_load_failed", error=str(exc))

    def _serialise_state(self) -> dict[str, Any]:
        """Convert in-memory state to a JSON-compatible dict."""
        tables: dict[str, Any] = {}
        now = time.time()
        for table_id, state in self._tables.items():
            # Only persist events from the last 24 h to keep the file small
            recent = [
                asdict(ev)
                for ev in state.events
                if (now - ev.timestamp) < _24H
            ]
            tables[table_id] = {
                "events": recent,
                "latency_ema_ms": state.latency_ema_ms,
                "priority_adjustment": state.priority_adjustment,
                "unique_clients": list(state.unique_clients),
            }
        return {"persisted_at": datetime.now(timezone.utc).isoformat(), "tables": tables}

    # ------------------------------------------------------------------
    # Internal helpers (must be called with lock held)
    # ------------------------------------------------------------------

    def _decayed_query_count(
        self, state: _TableAccessState, window_seconds: float
    ) -> float:
        """Sum of exponentially-decayed event weights within *window_seconds*."""
        now = time.time()
        cutoff = now - window_seconds
        total = 0.0
        for ev in reversed(state.events):
            if ev.timestamp < cutoff:
                break
            age = now - ev.timestamp
            weight = math.exp(-self._decay_lambda * age)
            total += weight
        return total

    def _decayed_queries_per_hour(
        self, state: _TableAccessState, window_seconds: float
    ) -> float:
        """Decayed queries per hour within *window_seconds*."""
        count = self._decayed_query_count(state, window_seconds)
        hours = window_seconds / 3600
        return count / max(hours, 1 / 3600)

    def _compute_heat_score(self, state: _TableAccessState) -> float:
        """Compute normalised 0.0-1.0 heat score (lock must be held)."""
        now = time.time()

        # --- Frequency component (60 %) ---
        qph_24h = self._decayed_queries_per_hour(state, _24H)
        # Normalise: at hot_threshold_qph we want ~0.8, beyond that saturates at 1.0
        freq_raw = qph_24h / max(self.hot_threshold_qph, 1)
        freq_score = min(freq_raw, 1.0)

        # --- Recency component (25 %) ---
        if state.events:
            last_ts = state.events[-1].timestamp
            seconds_ago = now - last_ts
            # Score drops to 0.5 after one half-life
            recency_score = math.exp(-self._decay_lambda * seconds_ago)
        else:
            recency_score = 0.0

        # --- Latency component (15 %) ---
        # High latency => table may benefit from compaction => boost score.
        # Normalise: 5000 ms maps to ~1.0
        latency_score = min(state.latency_ema_ms / 5000.0, 1.0) if state.latency_ema_ms > 0 else 0.0

        combined = 0.60 * freq_score + 0.25 * recency_score + 0.15 * latency_score

        # Apply feedback-loop adjustment
        combined *= state.priority_adjustment

        return max(0.0, min(1.0, combined))

    def _classify_unlocked(self, state: _TableAccessState) -> AccessClassification:
        """Classify without acquiring the lock (caller holds it)."""
        qph = self._decayed_queries_per_hour(state, _1H)
        if qph >= self.hot_threshold_qph:
            return "hot"
        if qph >= self.warm_threshold_qph:
            return "warm"
        return "cold"
