"""CloudEvents handler for Knative-based event-driven maintenance."""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from iceberg_janitor.runner.executor import execute_full_cycle, execute_table_maintenance

logger = structlog.get_logger()

router = APIRouter(tags=["CloudEvents"])


def _parse_cloudevent(request: Request, body: bytes) -> dict[str, Any]:
    """Parse a CloudEvent from either structured or binary content mode.

    Structured mode: the entire JSON body *is* the CloudEvent envelope.
    Binary mode: CloudEvents attributes are carried as ``ce-`` HTTP headers and
    the body is the ``data`` payload.
    """
    headers = request.headers

    # Binary content mode – attributes live in headers prefixed with ``ce-``
    if "ce-type" in headers:
        import json

        ce: dict[str, Any] = {
            "type": headers.get("ce-type", ""),
            "source": headers.get("ce-source", ""),
            "id": headers.get("ce-id", ""),
            "specversion": headers.get("ce-specversion", "1.0"),
        }
        if "ce-subject" in headers:
            ce["subject"] = headers["ce-subject"]
        try:
            ce["data"] = json.loads(body) if body else {}
        except (json.JSONDecodeError, ValueError):
            ce["data"] = {}
        return ce

    # Structured content mode – the body is the full CloudEvent JSON envelope
    import json

    try:
        ce = json.loads(body)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"Invalid CloudEvent JSON: {exc}") from exc
    return ce


@router.post("/ce")
async def handle_cloudevent(request: Request) -> JSONResponse:
    """Receive a CloudEvent and dispatch maintenance accordingly.

    Supported ``ce-type`` values:

    * ``dev.knative.sources.ping`` – triggers a full maintenance cycle for all
      tables (or those listed in the application config).
    * ``dev.knative.kafka.event`` – triggers single-table maintenance; the
      ``table_id`` is extracted from the event payload.

    Returns 200 on success to satisfy Knative delivery-retry semantics.
    """
    body = await request.body()

    try:
        ce = _parse_cloudevent(request, body)
    except ValueError as exc:
        logger.warning("cloudevent_parse_failed", error=str(exc))
        return JSONResponse(status_code=400, content={"error": str(exc)})

    ce_type = ce.get("type", "")
    ce_id = ce.get("id", "")
    log = logger.bind(ce_type=ce_type, ce_id=ce_id)
    log.info("cloudevent_received")

    catalog = request.app.state.catalog
    policy_config = request.app.state.policy_config

    if catalog is None:
        log.error("catalog_not_available")
        return JSONResponse(
            status_code=503,
            content={"error": "Catalog not available"},
        )

    if ce_type == "dev.knative.sources.ping":
        # Full cycle – optionally scoped to configured table list
        raw_config: dict[str, Any] = getattr(request.app.state, "raw_config", {})
        table_ids: list[str] | None = raw_config.get("tables") or None
        results = execute_full_cycle(catalog, policy_config, table_ids=table_ids)
        log.info("full_cycle_complete", tables_processed=len(results))
        return JSONResponse(status_code=200, content={"status": "ok", "results": results})

    if ce_type == "dev.knative.kafka.event":
        data = ce.get("data", {})
        table_id = data.get("table_id")
        if not table_id:
            log.warning("missing_table_id_in_payload")
            return JSONResponse(
                status_code=400,
                content={"error": "Payload must include 'table_id'"},
            )
        result = execute_table_maintenance(catalog, table_id, policy_config)
        log.info("table_maintenance_complete", table_id=table_id)
        return JSONResponse(status_code=200, content={"status": "ok", "result": result})

    log.warning("unknown_cloudevent_type", ce_type=ce_type)
    return JSONResponse(
        status_code=400,
        content={"error": f"Unsupported CloudEvent type: {ce_type}"},
    )
