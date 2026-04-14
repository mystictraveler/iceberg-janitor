"""API routes for access tracking and compaction feedback.

These endpoints allow external query engines (Trino, Spark, DuckDB) to push
access events into the janitor so it can prioritise maintenance for hot tables.
"""

from __future__ import annotations

import time

import structlog
from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from iceberg_janitor.strategy.access_tracker import AccessEvent, AccessTracker
from iceberg_janitor.strategy.feedback_loop import FeedbackLoop

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1", tags=["Access Tracking"])


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------


class AccessEventRequest(BaseModel):
    """Body for recording a single access event."""

    table_id: str = Field(..., description="Fully-qualified table identifier (e.g. 'db.schema.table')")
    query_latency_ms: float | None = Field(
        default=None, description="Query latency in milliseconds"
    )
    client_id: str | None = Field(
        default=None, description="Identifier of the querying client or user"
    )
    source: str = Field(
        default="api",
        description="Source of the event: 'api', 'prometheus', 'duckdb'",
    )
    timestamp: float | None = Field(
        default=None,
        description="Unix epoch timestamp. Defaults to server time if omitted.",
    )


class AccessEventBatchRequest(BaseModel):
    """Body for recording multiple access events at once."""

    events: list[AccessEventRequest] = Field(..., min_length=1, max_length=10_000)


class AccessStatsResponse(BaseModel):
    """Access statistics for a single table."""

    table_id: str
    classification: str
    heat_score: float
    query_counts: dict[str, float]
    avg_latency_ms: float
    unique_clients: int
    last_access: str | None
    total_events: int


class TableRankingEntry(BaseModel):
    """A single entry in the table-ranking list."""

    table_id: str
    heat_score: float
    classification: str
    queries_1h: float
    avg_latency_ms: float
    priority_adjustment: float


class FeedbackHistoryEntry(BaseModel):
    """A single effectiveness record."""

    table_id: str
    recorded_at: float
    effectiveness_score: float
    details: dict[str, float]
    before: dict[str, float | int]
    after: dict[str, float | int]


# ---------------------------------------------------------------------------
# Dependency helpers
# ---------------------------------------------------------------------------


def _get_access_tracker(request: Request) -> AccessTracker:
    """Retrieve the AccessTracker from app state, or raise 503."""
    tracker: AccessTracker | None = getattr(request.app.state, "access_tracker", None)
    if tracker is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Access tracking is not enabled. Set adaptive_policy_enabled=true "
                "in the policy configuration."
            ),
        )
    return tracker


def _get_feedback_loop(request: Request) -> FeedbackLoop:
    """Retrieve the FeedbackLoop from app state, or raise 503."""
    loop: FeedbackLoop | None = getattr(request.app.state, "feedback_loop", None)
    if loop is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Feedback loop is not enabled. Set feedback_loop_enabled=true "
                "in the policy configuration."
            ),
        )
    return loop


# ---------------------------------------------------------------------------
# Routes — access events
# ---------------------------------------------------------------------------


@router.post("/access/event", status_code=201)
async def record_access_event(
    body: AccessEventRequest,
    request: Request,
) -> dict[str, str]:
    """Record a single table access event.

    Call this from query engine hooks or proxies to inform the janitor
    about table access patterns.
    """
    tracker = _get_access_tracker(request)
    event = AccessEvent(
        table_id=body.table_id,
        timestamp=body.timestamp or time.time(),
        query_latency_ms=body.query_latency_ms,
        client_id=body.client_id,
        source=body.source,
    )
    tracker.record_event(event)
    return {"status": "recorded", "table_id": body.table_id}


@router.post("/access/events", status_code=201)
async def record_access_events_batch(
    body: AccessEventBatchRequest,
    request: Request,
) -> dict[str, str | int]:
    """Record a batch of access events in one call."""
    tracker = _get_access_tracker(request)
    events = [
        AccessEvent(
            table_id=ev.table_id,
            timestamp=ev.timestamp or time.time(),
            query_latency_ms=ev.query_latency_ms,
            client_id=ev.client_id,
            source=ev.source,
        )
        for ev in body.events
    ]
    tracker.record_events(events)
    return {"status": "recorded", "count": len(events)}


# ---------------------------------------------------------------------------
# Routes — access stats
# ---------------------------------------------------------------------------


@router.get("/tables/{table_id}/access", response_model=AccessStatsResponse)
async def get_table_access_stats(
    table_id: str,
    request: Request,
) -> AccessStatsResponse:
    """Get access frequency statistics for a single table."""
    tracker = _get_access_tracker(request)
    stats = tracker.get_access_stats(table_id)
    return AccessStatsResponse(**stats)


@router.get("/tables/ranking", response_model=list[TableRankingEntry])
async def get_table_rankings(
    request: Request,
    limit: int = Query(default=50, ge=1, le=1000),
    classification: str | None = Query(
        default=None,
        description="Filter by classification: 'hot', 'warm', or 'cold'",
    ),
) -> list[TableRankingEntry]:
    """Get all tracked tables ranked by heat score (hottest first)."""
    tracker = _get_access_tracker(request)
    rankings = tracker.get_all_rankings()

    if classification:
        rankings = [r for r in rankings if r["classification"] == classification]

    rankings = rankings[:limit]
    return [TableRankingEntry(**r) for r in rankings]


# ---------------------------------------------------------------------------
# Routes — feedback / effectiveness
# ---------------------------------------------------------------------------


@router.get("/feedback/{table_id}", response_model=list[FeedbackHistoryEntry])
async def get_feedback_history(
    table_id: str,
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
) -> list[FeedbackHistoryEntry]:
    """Get compaction effectiveness history for a table."""
    loop = _get_feedback_loop(request)
    history = loop.get_history(table_id)
    # Most recent first
    history.sort(key=lambda r: r["recorded_at"], reverse=True)
    history = history[:limit]
    return [FeedbackHistoryEntry(**entry) for entry in history]


@router.get("/feedback/summary", tags=["Access Tracking"])
async def get_feedback_summary(request: Request) -> dict:
    """Get aggregate feedback loop metrics for dashboarding."""
    loop = _get_feedback_loop(request)
    return loop.get_metrics_summary()
