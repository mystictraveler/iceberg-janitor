"""FastAPI dependency injection for catalog and policy configuration."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Request
from pyiceberg.catalog import Catalog

from iceberg_janitor.policy.models import PolicyConfig


def get_catalog(request: Request) -> Catalog:
    """Retrieve the PyIceberg catalog from application state.

    The catalog is initialised during the app lifespan and stored on
    ``request.app.state.catalog``.
    """
    catalog: Catalog | None = getattr(request.app.state, "catalog", None)
    if catalog is None:
        raise RuntimeError(
            "Catalog not initialised. Ensure the application lifespan has started."
        )
    return catalog


def get_policy_config(request: Request) -> PolicyConfig:
    """Retrieve the policy configuration from application state.

    Falls back to a default ``PolicyConfig`` if none has been set explicitly.
    """
    config: PolicyConfig | None = getattr(request.app.state, "policy_config", None)
    if config is None:
        config = PolicyConfig()
        request.app.state.policy_config = config
    return config


# Annotated types for cleaner route signatures
CatalogDep = Annotated[Catalog, Depends(get_catalog)]
PolicyConfigDep = Annotated[PolicyConfig, Depends(get_policy_config)]
