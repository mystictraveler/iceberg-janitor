"""Shared PyIceberg catalog connection factory."""

from __future__ import annotations

from functools import lru_cache
from typing import Any

from pyiceberg.catalog import Catalog, load_catalog


@lru_cache(maxsize=4)
def get_catalog(
    name: str = "default",
    uri: str | None = None,
    warehouse: str | None = None,
    catalog_type: str = "rest",
    **properties: Any,
) -> Catalog:
    """Create or retrieve a cached PyIceberg catalog connection.

    Args:
        name: Catalog name identifier.
        uri: Catalog URI (e.g., http://rest-catalog:8181).
        warehouse: Warehouse location (e.g., s3://warehouse/).
        catalog_type: Catalog type (rest, glue, hive, sql).
        **properties: Additional catalog properties (S3 endpoint, credentials, etc.).
    """
    config: dict[str, Any] = {"type": catalog_type}
    if uri:
        config["uri"] = uri
    if warehouse:
        config["warehouse"] = warehouse
    config.update(properties)
    return load_catalog(name, **config)


def get_catalog_from_config(config: dict[str, Any]) -> Catalog:
    """Create a catalog from a configuration dictionary.

    Expected keys: name, uri, warehouse, type, and any S3/credential properties.
    """
    name = config.pop("name", "default")
    return load_catalog(name, **config)
