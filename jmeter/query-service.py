"""Lightweight DuckDB query service for Iceberg benchmarking.

Accepts SQL queries over HTTP and executes them via DuckDB with the
iceberg extension loaded. Designed to be the target for JMeter load tests
that measure query performance before and after table compaction.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger("query-service")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DUCKDB_S3_ENDPOINT: str = os.getenv("DUCKDB_S3_ENDPOINT", "minio:9000")
DUCKDB_S3_ACCESS_KEY: str = os.getenv("AWS_ACCESS_KEY_ID", "admin")
DUCKDB_S3_SECRET_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
DUCKDB_S3_REGION: str = os.getenv("AWS_REGION", "us-east-1")
DUCKDB_S3_USE_SSL: bool = os.getenv("DUCKDB_S3_USE_SSL", "false").lower() == "true"
DUCKDB_S3_URL_STYLE: str = os.getenv("DUCKDB_S3_URL_STYLE", "path")
DUCKDB_MEMORY_LIMIT: str = os.getenv("DUCKDB_MEMORY_LIMIT", "512MB")
DUCKDB_THREADS: str = os.getenv("DUCKDB_THREADS", "4")

# Maximum rows returned per query to prevent OOM on large result sets.
MAX_RESULT_ROWS: int = int(os.getenv("MAX_RESULT_ROWS", "10000"))

# SQL keywords that are forbidden for safety (case-insensitive check).
BLOCKED_KEYWORDS: set[str] = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "CREATE",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "COPY",
    "EXPORT",
    "IMPORT",
    "ATTACH",
}


# ---------------------------------------------------------------------------
# DuckDB connection pool (single writer, safe for read-only workloads)
# ---------------------------------------------------------------------------

_conn: duckdb.DuckDBPyConnection | None = None


def get_connection() -> duckdb.DuckDBPyConnection:
    global _conn  # noqa: PLW0603
    if _conn is None:
        _conn = _init_connection()
    return _conn


def _init_connection() -> duckdb.DuckDBPyConnection:
    """Create and configure a DuckDB in-memory connection with iceberg support."""
    conn = duckdb.connect(":memory:")

    # Performance settings
    conn.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    conn.execute(f"SET threads = {DUCKDB_THREADS}")

    # Load iceberg + httpfs extensions
    conn.execute("INSTALL iceberg")
    conn.execute("LOAD iceberg")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    # S3 configuration for MinIO / S3-compatible storage
    conn.execute(f"SET s3_endpoint = '{DUCKDB_S3_ENDPOINT}'")
    conn.execute(f"SET s3_access_key_id = '{DUCKDB_S3_ACCESS_KEY}'")
    conn.execute(f"SET s3_secret_access_key = '{DUCKDB_S3_SECRET_KEY}'")
    conn.execute(f"SET s3_region = '{DUCKDB_S3_REGION}'")
    conn.execute(f"SET s3_url_style = '{DUCKDB_S3_URL_STYLE}'")
    conn.execute(f"SET s3_use_ssl = {str(DUCKDB_S3_USE_SSL).lower()}")

    logger.info(
        "DuckDB connection initialized (iceberg + httpfs loaded, s3_endpoint=%s)",
        DUCKDB_S3_ENDPOINT,
    )
    return conn


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Initialize DuckDB connection on startup, close on shutdown."""
    logger.info("Starting query service - initializing DuckDB...")
    get_connection()
    logger.info("DuckDB ready.")
    yield
    if _conn is not None:
        _conn.close()
        logger.info("DuckDB connection closed.")


app = FastAPI(
    title="Iceberg Query Service",
    description="DuckDB query endpoint for Iceberg table benchmarking",
    version="0.1.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class QueryRequest(BaseModel):
    sql: str


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    elapsed_ms: float


class HealthResponse(BaseModel):
    status: str
    duckdb_version: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Health check - verifies DuckDB is responsive."""
    conn = get_connection()
    version = conn.execute("SELECT version()").fetchone()[0]
    return HealthResponse(status="ok", duckdb_version=version)


@app.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest) -> QueryResponse:
    """Execute a read-only SQL query via DuckDB and return results as JSON."""
    sql = request.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Empty SQL query")

    # Safety: block write operations
    first_token = sql.split()[0].upper() if sql.split() else ""
    if first_token in BLOCKED_KEYWORDS:
        raise HTTPException(
            status_code=403,
            detail=f"Write operations are not allowed: {first_token}",
        )

    conn = get_connection()

    start = time.perf_counter()
    try:
        result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchmany(MAX_RESULT_ROWS)
        # Convert to plain lists for JSON serialization
        rows = [list(row) for row in rows]
    except duckdb.Error as exc:
        raise HTTPException(status_code=422, detail=f"DuckDB error: {exc}") from exc
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    logger.info("Query executed in %.1fms (%d rows): %s", elapsed_ms, len(rows), sql[:120])

    return QueryResponse(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        elapsed_ms=round(elapsed_ms, 2),
    )


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("QUERY_SERVICE_PORT", "8070"))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
