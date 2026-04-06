"""TPC-DS Benchmark Test Suite — before/after compaction performance comparison.

This test suite:
1. Creates all 24 TPC-DS tables as Iceberg tables via the REST catalog
2. Batch-loads 17 dimension tables
3. Streams 7 fact tables in micro-batches (creating the small-file problem)
4. Runs 10 TPC-DS-derived queries via DuckDB and records timings (BEFORE)
5. Runs compaction on all fact tables
6. Reruns the same queries and records timings (AFTER)
7. Prints a comparison report

Prerequisites:
    - MinIO running at localhost:9000 (port-forwarded from K8s)
    - REST catalog running at localhost:8181 (port-forwarded from K8s)
    - pip install iceberg-janitor[query]  (includes duckdb)

Run:
    pytest tests/test_tpcds_benchmark.py -v -s --tb=short
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime

import duckdb
import pyarrow as pa
import pytest

# Add scripts/ to path for tpcds modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scripts.tpcds_schema import ALL_TABLES, DIMENSION_TABLES, FACT_TABLES
from scripts.tpcds_datagen import DIMENSION_GENERATORS, FACT_BATCH_GENERATORS

# ─── Configuration ──────────────────────────────────────────────────

CATALOG_URI = os.environ.get("CATALOG_URI", "http://localhost:8181")
WAREHOUSE = os.environ.get("WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")
NAMESPACE = "tpcds"

# Streaming config: number of micro-batches per fact table
NUM_BATCHES = int(os.environ.get("NUM_BATCHES", "50"))

# ─── TPC-DS Queries (adapted for DuckDB + Iceberg) ─────────────────

BENCHMARK_QUERIES = {
    # Q1: Top customers by store returns (store_returns + customer + store + date_dim)
    "q1_top_return_customers": """
        SELECT c.c_customer_id, c.c_first_name, c.c_last_name,
               sum(sr.sr_return_amt) as total_returns
        FROM {ns}.store_returns sr
        JOIN {ns}.customer c ON sr.sr_customer_sk = c.c_customer_sk
        JOIN {ns}.store s ON sr.sr_store_sk = s.s_store_sk
        JOIN {ns}.date_dim d ON sr.sr_returned_date_sk = d.d_date_sk
        WHERE d.d_year = 2022
        GROUP BY c.c_customer_id, c.c_first_name, c.c_last_name
        ORDER BY total_returns DESC
        LIMIT 100
    """,

    # Q3: Brand revenue by year (store_sales + item + date_dim)
    "q3_brand_revenue": """
        SELECT d.d_year, i.i_brand, i.i_brand_id,
               sum(ss.ss_ext_sales_price) as revenue
        FROM {ns}.store_sales ss
        JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE i.i_category = 'Electronics'
        GROUP BY d.d_year, i.i_brand, i.i_brand_id
        ORDER BY d.d_year, revenue DESC
        LIMIT 100
    """,

    # Q7: Promotion impact (store_sales + item + customer_demographics + promotion + date_dim)
    "q7_promo_impact": """
        SELECT i.i_item_id,
               avg(ss.ss_quantity) as avg_qty,
               avg(ss.ss_list_price) as avg_price,
               avg(ss.ss_coupon_amt) as avg_coupon,
               avg(ss.ss_sales_price) as avg_sales
        FROM {ns}.store_sales ss
        JOIN {ns}.customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
        JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
        JOIN {ns}.promotion p ON ss.ss_promo_sk = p.p_promo_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE cd.cd_gender = 'F' AND cd.cd_marital_status = 'S' AND d.d_year = 2023
        GROUP BY i.i_item_id
        ORDER BY i.i_item_id
        LIMIT 100
    """,

    # Q13: Store sales by demographics (store_sales + store + customer_demographics +
    #       customer_address + household_demographics + date_dim)
    "q13_demo_store_sales": """
        SELECT avg(ss.ss_quantity) as avg_qty,
               avg(ss.ss_ext_sales_price) as avg_ext_price,
               avg(ss.ss_ext_wholesale_cost) as avg_wholesale,
               sum(ss.ss_ext_wholesale_cost) as total_wholesale
        FROM {ns}.store_sales ss
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        JOIN {ns}.customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
        JOIN {ns}.customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE cd.cd_marital_status = 'M'
          AND d.d_year = 2022
          AND ca.ca_state IN ('CA', 'NY', 'TX')
    """,

    # Q19: Revenue by brand-manager-zip (store_sales + item + customer + customer_address +
    #       store + date_dim)
    "q19_brand_manager_zip": """
        SELECT i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact,
               sum(ss.ss_ext_sales_price) as revenue
        FROM {ns}.store_sales ss
        JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
        JOIN {ns}.customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN {ns}.customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE d.d_moy = 11 AND d.d_year = 2023
          AND ca.ca_state != s.s_state
        GROUP BY i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact
        ORDER BY revenue DESC
        LIMIT 100
    """,

    # Q25: Store returns joined with catalog sales (store_sales + store_returns +
    #       catalog_sales + date_dim + store + item)
    "q25_cross_channel_returns": """
        SELECT i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name,
               sum(ss.ss_net_profit) as store_profit,
               sum(sr.sr_net_loss) as return_loss
        FROM {ns}.store_sales ss
        JOIN {ns}.store_returns sr ON ss.ss_customer_sk = sr.sr_customer_sk
            AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number
        JOIN {ns}.catalog_sales cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk
            AND sr.sr_item_sk = cs.cs_item_sk
        JOIN {ns}.date_dim d1 ON ss.ss_sold_date_sk = d1.d_date_sk
        JOIN {ns}.date_dim d2 ON sr.sr_returned_date_sk = d2.d_date_sk
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
        WHERE d1.d_year = 2022
        GROUP BY i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name
        ORDER BY i.i_item_id, store_profit
        LIMIT 100
    """,

    # Q43: Weekly store sales (store_sales + store + date_dim)
    "q43_weekly_store_sales": """
        SELECT s.s_store_name, s.s_store_id,
               sum(CASE WHEN d.d_dow = 0 THEN ss.ss_sales_price ELSE 0 END) as sun_sales,
               sum(CASE WHEN d.d_dow = 1 THEN ss.ss_sales_price ELSE 0 END) as mon_sales,
               sum(CASE WHEN d.d_dow = 2 THEN ss.ss_sales_price ELSE 0 END) as tue_sales,
               sum(CASE WHEN d.d_dow = 3 THEN ss.ss_sales_price ELSE 0 END) as wed_sales,
               sum(CASE WHEN d.d_dow = 4 THEN ss.ss_sales_price ELSE 0 END) as thu_sales,
               sum(CASE WHEN d.d_dow = 5 THEN ss.ss_sales_price ELSE 0 END) as fri_sales,
               sum(CASE WHEN d.d_dow = 6 THEN ss.ss_sales_price ELSE 0 END) as sat_sales
        FROM {ns}.store_sales ss
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE d.d_year = 2023
        GROUP BY s.s_store_name, s.s_store_id
        ORDER BY s.s_store_name
        LIMIT 100
    """,

    # Q46: Customer spend by city (store_sales + customer + customer_address +
    #       date_dim + household_demographics + store)
    "q46_customer_spend_city": """
        SELECT c.c_last_name, c.c_first_name, ca.ca_city,
               sum(ss.ss_net_paid) as total_spend
        FROM {ns}.store_sales ss
        JOIN {ns}.customer c ON ss.ss_customer_sk = c.c_customer_sk
        JOIN {ns}.customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
        JOIN {ns}.household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        WHERE d.d_year IN (2022, 2023)
          AND hd.hd_dep_count >= 2
        GROUP BY c.c_last_name, c.c_first_name, ca.ca_city
        ORDER BY total_spend DESC
        LIMIT 100
    """,

    # Q55: Brand revenue by month (store_sales + item + date_dim)
    "q55_brand_revenue_monthly": """
        SELECT i.i_brand_id, i.i_brand,
               sum(ss.ss_ext_sales_price) as total_revenue
        FROM {ns}.store_sales ss
        JOIN {ns}.item i ON ss.ss_item_sk = i.i_item_sk
        JOIN {ns}.date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
        WHERE d.d_moy = 6 AND d.d_year = 2023
          AND i.i_manager_id = 15
        GROUP BY i.i_brand_id, i.i_brand
        ORDER BY total_revenue DESC
        LIMIT 100
    """,

    # Q96: Order count by time/shift (store_sales + time_dim + household_demographics + store)
    "q96_order_by_shift": """
        SELECT count(*) as order_count
        FROM {ns}.store_sales ss
        JOIN {ns}.time_dim t ON ss.ss_sold_time_sk = t.t_time_sk
        JOIN {ns}.household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
        JOIN {ns}.store s ON ss.ss_store_sk = s.s_store_sk
        WHERE t.t_hour = 14
          AND hd.hd_dep_count = 3
    """,
}


# ─── Fixtures ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def catalog():
    """Create a PyIceberg catalog connection."""
    from pyiceberg.catalog import load_catalog
    return load_catalog(
        "benchmark",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_ACCESS_KEY,
            "s3.secret-access-key": S3_SECRET_KEY,
        },
    )


@pytest.fixture(scope="session")
def duckdb_conn():
    """Create a DuckDB connection with Iceberg + S3 configured."""
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT.replace('http://', '')}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


@pytest.fixture(scope="session")
def tpcds_tables(catalog):
    """Create all TPC-DS tables and load data. Returns table locations."""
    # Create namespace
    try:
        catalog.create_namespace(NAMESPACE)
    except Exception:
        pass  # already exists

    locations = {}

    # Create and load dimension tables (batch)
    print(f"\n{'='*60}")
    print("LOADING TPC-DS DIMENSION TABLES")
    print(f"{'='*60}")
    for name, schema in DIMENSION_TABLES.items():
        table_id = f"{NAMESPACE}.{name}"
        try:
            table = catalog.create_table(table_id, schema=schema)
        except Exception:
            table = catalog.load_table(table_id)

        if name in DIMENSION_GENERATORS:
            data = DIMENSION_GENERATORS[name]()
            table.overwrite(data)
            print(f"  {name}: {len(data)} rows (batch)")

        locations[name] = table.metadata.location

    # Create and stream fact tables (micro-batches)
    print(f"\n{'='*60}")
    print(f"STREAMING TPC-DS FACT TABLES ({NUM_BATCHES} micro-batches each)")
    print(f"{'='*60}")
    for name, schema in FACT_TABLES.items():
        table_id = f"{NAMESPACE}.{name}"
        try:
            table = catalog.create_table(table_id, schema=schema)
        except Exception:
            table = catalog.load_table(table_id)

        gen = FACT_BATCH_GENERATORS.get(name)
        if gen:
            total_rows = 0
            for i in range(NUM_BATCHES):
                batch = gen()
                table.append(batch)
                total_rows += len(batch)
            print(f"  {name}: {total_rows} rows across {NUM_BATCHES} micro-batches")

        locations[name] = table.metadata.location

    print(f"\nAll {len(ALL_TABLES)} TPC-DS tables created and loaded.")
    return locations


# ─── Helper ─────────────────────────────────────────────────────────

def _run_query(conn: duckdb.DuckDBPyConnection, name: str, sql: str, ns: str) -> tuple[str, float, int]:
    """Run a query and return (name, elapsed_ms, row_count)."""
    formatted = sql.format(ns=ns)
    start = time.perf_counter()
    try:
        result = conn.execute(formatted).fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        return name, elapsed, len(result)
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        print(f"    WARN: {name} failed: {e}")
        return name, elapsed, -1


def _register_iceberg_tables(conn: duckdb.DuckDBPyConnection, locations: dict[str, str]):
    """Register all Iceberg tables as DuckDB views for SQL access."""
    for name, loc in locations.items():
        try:
            conn.execute(f"""
                CREATE OR REPLACE VIEW {NAMESPACE}.{name} AS
                SELECT * FROM iceberg_scan('{loc}', allow_moved_paths = true)
            """)
        except Exception as e:
            print(f"  WARN: Could not register {name}: {e}")


def _run_benchmark(conn: duckdb.DuckDBPyConnection, label: str) -> dict[str, tuple[float, int]]:
    """Run all benchmark queries and return results."""
    print(f"\n{'='*60}")
    print(f"BENCHMARK: {label}")
    print(f"{'='*60}")

    results = {}
    for name, sql in BENCHMARK_QUERIES.items():
        timings = []
        rows = 0
        # Run 3 times, take median
        for run in range(3):
            qname, elapsed, rcount = _run_query(conn, name, sql, NAMESPACE)
            timings.append(elapsed)
            rows = rcount
        med = sorted(timings)[1]  # median of 3
        results[name] = (med, rows)
        status = f"{rows} rows" if rows >= 0 else "FAILED"
        print(f"  {name:40s} {med:8.1f} ms  ({status})")

    total = sum(t for t, _ in results.values())
    print(f"  {'TOTAL':40s} {total:8.1f} ms")
    return results


def _print_comparison(before: dict, after: dict):
    """Print a side-by-side comparison report."""
    print(f"\n{'='*70}")
    print("PERFORMANCE COMPARISON: BEFORE vs AFTER COMPACTION")
    print(f"{'='*70}")
    print(f"  {'Query':<40s} {'Before':>10s} {'After':>10s} {'Change':>10s}")
    print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*10}")

    total_before = 0
    total_after = 0
    for name in BENCHMARK_QUERIES:
        b_ms, _ = before.get(name, (0, 0))
        a_ms, _ = after.get(name, (0, 0))
        total_before += b_ms
        total_after += a_ms

        if b_ms > 0:
            pct = ((a_ms - b_ms) / b_ms) * 100
            arrow = "faster" if pct < 0 else "slower"
            print(f"  {name:<40s} {b_ms:>8.1f}ms {a_ms:>8.1f}ms {pct:>+7.1f}% {arrow}")
        else:
            print(f"  {name:<40s} {b_ms:>8.1f}ms {a_ms:>8.1f}ms {'N/A':>10s}")

    if total_before > 0:
        overall_pct = ((total_after - total_before) / total_before) * 100
        print(f"  {'-'*40} {'-'*10} {'-'*10} {'-'*10}")
        print(f"  {'TOTAL':<40s} {total_before:>8.1f}ms {total_after:>8.1f}ms {overall_pct:>+7.1f}%")
        print(f"\n  Overall: {'FASTER' if overall_pct < 0 else 'SLOWER'} by {abs(overall_pct):.1f}%")


# ─── Tests ──────────────────────────────────────────────────────────

class TestTPCDSBenchmark:
    """Full TPC-DS benchmark suite testing before/after compaction performance."""

    def test_01_setup_tables(self, tpcds_tables):
        """Verify all 24 TPC-DS tables were created."""
        assert len(tpcds_tables) == len(ALL_TABLES)
        for name in ALL_TABLES:
            assert name in tpcds_tables, f"Missing table: {name}"

    def test_02_verify_streaming_created_small_files(self, catalog, tpcds_tables):
        """Verify that streaming micro-batches created many small files."""
        print(f"\n{'='*60}")
        print("TABLE HEALTH BEFORE COMPACTION")
        print(f"{'='*60}")

        for name in FACT_TABLES:
            table = catalog.load_table(f"{NAMESPACE}.{name}")
            files = list(table.scan().plan_files())
            sizes = [f.file.file_size_in_bytes for f in files]
            total_mb = sum(sizes) / (1024 * 1024)
            avg_kb = (sum(sizes) / len(sizes) / 1024) if sizes else 0
            snapshots = len(table.metadata.snapshots)

            print(f"  {name:25s}  files={len(files):4d}  "
                  f"avg={avg_kb:.0f}KB  total={total_mb:.1f}MB  "
                  f"snapshots={snapshots}")

            # Fact tables should have many files from micro-batches
            assert len(files) >= NUM_BATCHES, (
                f"{name} should have >= {NUM_BATCHES} files, got {len(files)}"
            )

    def test_03_benchmark_before_compaction(self, duckdb_conn, tpcds_tables):
        """Run all TPC-DS queries BEFORE compaction and record timings."""
        # Create schema in DuckDB
        try:
            duckdb_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {NAMESPACE}")
        except Exception:
            pass

        _register_iceberg_tables(duckdb_conn, tpcds_tables)

        results = _run_benchmark(duckdb_conn, "BEFORE COMPACTION")

        # Store results for comparison (using pytest cache or class attr)
        TestTPCDSBenchmark._before_results = results

        # At least some queries should succeed
        successful = sum(1 for _, (_, rows) in results.items() if rows >= 0)
        assert successful >= 5, f"Only {successful} queries succeeded"

    def test_04_run_compaction(self, catalog):
        """Compact all fact tables to merge small files."""
        print(f"\n{'='*60}")
        print("RUNNING COMPACTION ON FACT TABLES")
        print(f"{'='*60}")

        for name in FACT_TABLES:
            table_id = f"{NAMESPACE}.{name}"
            table = catalog.load_table(table_id)

            # Count files before
            files_before = len(list(table.scan().plan_files()))

            # Read all data and overwrite (compact)
            start = time.perf_counter()
            df = table.scan().to_arrow()
            table.overwrite(df)
            elapsed = time.perf_counter() - start

            # Count files after
            table = catalog.load_table(table_id)  # reload metadata
            files_after = len(list(table.scan().plan_files()))

            print(f"  {name:25s}  {files_before:4d} -> {files_after:4d} files  "
                  f"({elapsed:.1f}s)  rows={len(df)}")

    def test_05_verify_compaction_reduced_files(self, catalog):
        """Verify compaction reduced file count."""
        print(f"\n{'='*60}")
        print("TABLE HEALTH AFTER COMPACTION")
        print(f"{'='*60}")

        for name in FACT_TABLES:
            table = catalog.load_table(f"{NAMESPACE}.{name}")
            files = list(table.scan().plan_files())
            sizes = [f.file.file_size_in_bytes for f in files]
            total_mb = sum(sizes) / (1024 * 1024) if sizes else 0
            avg_kb = (sum(sizes) / len(sizes) / 1024) if sizes else 0

            print(f"  {name:25s}  files={len(files):4d}  "
                  f"avg={avg_kb:.0f}KB  total={total_mb:.1f}MB")

            # After compaction, should have far fewer files
            assert len(files) < NUM_BATCHES, (
                f"{name} should have < {NUM_BATCHES} files after compaction, got {len(files)}"
            )

    def test_06_benchmark_after_compaction(self, duckdb_conn, tpcds_tables):
        """Run all TPC-DS queries AFTER compaction and compare with before."""
        # Re-register tables (metadata changed after compaction)
        _register_iceberg_tables(duckdb_conn, tpcds_tables)

        after_results = _run_benchmark(duckdb_conn, "AFTER COMPACTION")

        before_results = getattr(TestTPCDSBenchmark, "_before_results", None)
        if before_results:
            _print_comparison(before_results, after_results)

        successful = sum(1 for _, (_, rows) in after_results.items() if rows >= 0)
        assert successful >= 5, f"Only {successful} queries succeeded"
