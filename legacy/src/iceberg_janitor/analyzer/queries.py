"""DEPRECATED — DuckDB queries module.

This module previously contained DuckDB SQL queries that accessed Iceberg
metadata by scanning S3 directly via the DuckDB iceberg extension.

The analyzer now uses PyIceberg's catalog API exclusively
(see health.py), which works with any catalog backend:
REST Catalog, AWS Glue, Hive Metastore, etc.

DuckDB remains available as an optional query engine for *user-facing*
analytics against Iceberg tables (via the JMeter query service), but is
no longer a dependency for table health assessment.
"""
