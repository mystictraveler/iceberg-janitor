"""FastAPI application factory for the Iceberg Janitor API."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pyiceberg.catalog import load_catalog

from iceberg_janitor.api.routes import router
from iceberg_janitor.api.routes_access import router as access_router
from iceberg_janitor.api.routes_cloudevents import router as cloudevents_router
from iceberg_janitor.api.schemas import ErrorResponse
from iceberg_janitor.policy.models import PolicyConfig

logger = structlog.get_logger()

APP_TITLE = "Iceberg Janitor"
APP_VERSION = "1.0.0"
APP_DESCRIPTION = (
    "REST API for Apache Iceberg table maintenance automation. "
    "Provides health assessment, compaction, snapshot expiration, "
    "orphan file removal, manifest rewriting, sort order optimisation, "
    "and policy-driven scheduling."
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialise shared resources on startup and tear them down on shutdown."""
    app.state.start_time = time.time()

    # Initialise the Iceberg catalog from environment variables.
    catalog_name = os.environ.get("ICEBERG_CATALOG_NAME", "default")
    try:
        catalog = load_catalog(catalog_name)
        app.state.catalog = catalog
        logger.info("catalog_connected", catalog=catalog_name)
    except Exception as exc:
        logger.warning(
            "catalog_init_failed",
            catalog=catalog_name,
            error=str(exc),
        )
        app.state.catalog = None

    # Load policy configuration (could come from a file or DB in production)
    policy_config = PolicyConfig()
    app.state.policy_config = policy_config

    # Store the raw config dict for the CloudEvents handler (e.g. table list)
    config_path = os.environ.get("JANITOR_CONFIG_PATH")
    raw_config: dict = {}
    if config_path:
        import yaml

        try:
            with open(config_path) as f:
                raw_config = yaml.safe_load(f) or {}
        except Exception as exc:
            logger.warning("raw_config_load_failed", path=config_path, error=str(exc))
    app.state.raw_config = raw_config

    logger.info("policy_config_loaded")

    # Initialise adaptive policy components when enabled
    adaptive_cfg = policy_config.adaptive
    if adaptive_cfg.adaptive_policy_enabled:
        from iceberg_janitor.strategy.access_tracker import AccessTracker
        from iceberg_janitor.strategy.adaptive_policy import AdaptivePolicyEngine

        tracker = AccessTracker(
            hot_threshold_qph=adaptive_cfg.hot_threshold_queries_per_hour,
            warm_threshold_qph=adaptive_cfg.warm_threshold_queries_per_hour,
            persistence_path=adaptive_cfg.access_state_persistence_path,
            decay_half_life_seconds=adaptive_cfg.decay_half_life_hours * 3600,
        )
        app.state.access_tracker = tracker

        feedback_loop = None
        if adaptive_cfg.feedback_loop_enabled:
            from iceberg_janitor.strategy.feedback_loop import FeedbackLoop

            feedback_loop = FeedbackLoop(
                access_tracker=tracker,
                lookback_hours=adaptive_cfg.effectiveness_lookback_hours,
            )
            app.state.feedback_loop = feedback_loop

        engine = AdaptivePolicyEngine(
            base_config=policy_config,
            access_tracker=tracker,
            hot_multiplier=adaptive_cfg.hot_multiplier,
            warm_multiplier=adaptive_cfg.warm_multiplier,
            cold_multiplier=adaptive_cfg.cold_multiplier,
            feedback_loop=feedback_loop,
        )
        app.state.adaptive_policy_engine = engine
        logger.info(
            "adaptive_policy_enabled",
            hot_threshold=adaptive_cfg.hot_threshold_queries_per_hour,
            feedback_loop=adaptive_cfg.feedback_loop_enabled,
        )

    yield

    # Persist access tracker state on shutdown
    if hasattr(app.state, "access_tracker"):
        try:
            app.state.access_tracker.persist()
            logger.info("access_tracker_state_persisted")
        except Exception as exc:
            logger.warning("access_tracker_persist_failed", error=str(exc))

    # Shutdown
    logger.info("shutting_down")


def create_app() -> FastAPI:
    """Build and configure the FastAPI application."""

    app = FastAPI(
        title=APP_TITLE,
        version=APP_VERSION,
        description=APP_DESCRIPTION,
        lifespan=lifespan,
        openapi_url="/api/v1/openapi.json",
        docs_url="/api/v1/docs",
        redoc_url="/api/v1/redoc",
    )

    # -----------------------------------------------------------------------
    # Middleware
    # -----------------------------------------------------------------------

    allowed_origins = os.environ.get("CORS_ALLOWED_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------------
    # Exception handlers
    # -----------------------------------------------------------------------

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=ErrorResponse(
                error="bad_request", detail=str(exc)
            ).model_dump(),
        )

    @app.exception_handler(RuntimeError)
    async def runtime_error_handler(
        request: Request, exc: RuntimeError
    ) -> JSONResponse:
        logger.error("runtime_error", error=str(exc))
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="internal_error", detail=str(exc)
            ).model_dump(),
        )

    @app.exception_handler(Exception)
    async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.error("unhandled_exception", error=str(exc), exc_type=type(exc).__name__)
        return JSONResponse(
            status_code=500,
            content=ErrorResponse(
                error="internal_error",
                detail="An unexpected error occurred",
            ).model_dump(),
        )

    # -----------------------------------------------------------------------
    # Routers
    # -----------------------------------------------------------------------

    app.include_router(router)
    app.include_router(access_router)
    app.include_router(cloudevents_router)

    return app


# Allow ``uvicorn iceberg_janitor.api.app:app`` to work out of the box.
app = create_app()
