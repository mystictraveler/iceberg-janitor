"""CronJob entrypoint: single-shot maintenance cycle for K8s CronJob execution."""

from __future__ import annotations

import structlog
import yaml

from iceberg_janitor.catalog import get_catalog
from iceberg_janitor.policy.models import PolicyConfig
from iceberg_janitor.runner.executor import execute_full_cycle

logger = structlog.get_logger()


def run_maintenance_cycle(config_path: str) -> dict:
    """Execute a full maintenance cycle: analyze all tables, evaluate policies, run actions.

    This is the main entrypoint for the K8s CronJob. It:
    1. Loads policy config from YAML
    2. Connects to the catalog
    3. Delegates to :func:`executor.execute_full_cycle` for the
       analyze -> evaluate -> execute loop
    4. Emits Prometheus metrics

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        Summary dict with per-table results.
    """
    log = logger.bind(config_path=config_path)
    log.info("maintenance_cycle_starting")

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    # Extract catalog config and policy config
    catalog_config = raw_config.get("catalog", {})
    policy_config = PolicyConfig.model_validate(raw_config.get("policy", {}))
    table_ids: list[str] = raw_config.get("tables", []) or None

    # Connect to catalog
    catalog = get_catalog(
        name=catalog_config.get("name", "default"),
        uri=catalog_config.get("uri"),
        warehouse=catalog_config.get("warehouse"),
        catalog_type=catalog_config.get("type", "rest"),
        **{k: v for k, v in catalog_config.items() if k not in ("name", "uri", "warehouse", "type")},
    )

    return execute_full_cycle(catalog, policy_config, table_ids=table_ids)
