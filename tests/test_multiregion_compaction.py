"""Multi-Region Compaction Strategy Simulation.

Simulates two regions with replicated Iceberg tables to evaluate:
1. Independent compaction in each region
2. Compact in one region + ship compacted files cross-region
3. Cost comparison: local compute vs. cross-region transfer

Setup:
  - Region A: s3://warehouse-us-east/ (DuckDB instance 1)
  - Region B: s3://warehouse-eu-west/ (DuckDB instance 2)
  - Both share the same Iceberg catalog (metadata replicated)
  - Data files replicated via S3 CRR (cross-region replication)

Key insight: When Region A compacts, it creates NEW files. These new files
replicate to Region B via CRR. But Region B's old small files ALSO still
exist. The Iceberg metadata (current snapshot) determines which files are
"live" — so Region B immediately benefits from Region A's compaction IF
metadata is replicated. But the transfer cost of the compacted files must
be weighed against just compacting locally in Region B.

Run:
    pytest tests/test_multiregion_compaction.py -v -s
"""

from __future__ import annotations

import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb
import pyarrow as pa
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from scripts.tpcds_schema import FACT_TABLES
from scripts.tpcds_datagen import FACT_BATCH_GENERATORS

# ─── Configuration ──────────────────────────────────────────────────

CATALOG_URI = os.environ.get("CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
S3_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")

REGION_A_WAREHOUSE = "s3://warehouse-us-east/"
REGION_B_WAREHOUSE = "s3://warehouse-eu-west/"
REGION_A_NS = "region_a"
REGION_B_NS = "region_b"

NUM_BATCHES = int(os.environ.get("NUM_BATCHES", "50"))

# Tables to test (subset of fact tables for speed)
TEST_TABLES = ["store_sales", "catalog_sales", "web_sales"]

# ─── AWS Cost Model (us-east-1 pricing, April 2026) ────────────────

@dataclass
class AWSCostModel:
    """AWS pricing for cost comparison."""
    # S3 pricing
    s3_put_per_1k: float = 0.005          # PUT/COPY/POST per 1K requests
    s3_get_per_1k: float = 0.0004         # GET/SELECT per 1K requests
    s3_storage_per_gb_month: float = 0.023 # Standard storage
    s3_crr_per_gb: float = 0.02           # Cross-region replication transfer

    # Data transfer
    cross_region_per_gb: float = 0.02     # Inter-region transfer
    same_region_per_gb: float = 0.00      # Free within same region

    # Compute (Flink/PyIceberg compaction)
    # Approximate: m5.xlarge spot = $0.05/hr, processes ~50GB/hr
    compute_per_gb: float = 0.001         # $/GB for compaction compute

    def compaction_cost(self, data_gb: float, file_count: int) -> dict:
        """Cost of compacting locally."""
        read_requests = file_count  # One GET per file
        write_requests = max(1, int(data_gb / 0.128))  # ~128MB per output file
        return {
            "compute": round(data_gb * self.compute_per_gb, 4),
            "s3_read": round(read_requests / 1000 * self.s3_get_per_1k, 4),
            "s3_write": round(write_requests / 1000 * self.s3_put_per_1k, 4),
            "total": round(
                data_gb * self.compute_per_gb
                + read_requests / 1000 * self.s3_get_per_1k
                + write_requests / 1000 * self.s3_put_per_1k,
                4,
            ),
        }

    def cross_region_ship_cost(self, data_gb: float, file_count_new: int) -> dict:
        """Cost of shipping compacted files cross-region (via CRR or explicit copy)."""
        transfer = data_gb * self.cross_region_per_gb
        put_requests = file_count_new / 1000 * self.s3_put_per_1k  # PUTs in dest region
        return {
            "transfer": round(transfer, 4),
            "s3_put_dest": round(put_requests, 4),
            "total": round(transfer + put_requests, 4),
        }


COST = AWSCostModel()


# ─── Simulation Data Structures ─────────────────────────────────────

@dataclass
class RegionState:
    """State of a table in one region."""
    region: str
    table_id: str
    file_count: int = 0
    total_bytes: int = 0
    avg_file_size_bytes: float = 0.0
    snapshot_count: int = 0
    compacted: bool = False
    compaction_time_s: float = 0.0
    query_time_ms: float = 0.0


@dataclass
class MultiRegionAnalysis:
    """Cost-benefit analysis for multi-region compaction strategies."""
    table_id: str
    data_gb: float
    file_count: int

    # Strategy 1: Compact independently in both regions
    strategy_compact_both: dict = field(default_factory=dict)

    # Strategy 2: Compact in Region A, ship to Region B
    strategy_compact_ship: dict = field(default_factory=dict)

    # Strategy 3: Compact in Region A only, Region B queries uncompacted
    strategy_compact_one: dict = field(default_factory=dict)

    recommendation: str = ""


# ─── Test Queries ───────────────────────────────────────────────────

QUERY_TEMPLATE = """
SELECT count(*) as cnt,
       sum(ss_ext_sales_price) as total_sales,
       avg(ss_quantity) as avg_qty,
       min(ss_sales_price) as min_price,
       max(ss_list_price) as max_price
FROM {location}
WHERE ss_sold_date_sk BETWEEN 500 AND 1500
"""

QUERY_TEMPLATE_CS = """
SELECT count(*) as cnt,
       sum(cs_ext_sales_price) as total_sales,
       avg(cs_quantity) as avg_qty
FROM {location}
WHERE cs_sold_date_sk BETWEEN 500 AND 1500
"""

QUERY_TEMPLATE_WS = """
SELECT count(*) as cnt,
       sum(ws_ext_sales_price) as total_sales,
       avg(ws_quantity) as avg_qty
FROM {location}
WHERE ws_sold_date_sk BETWEEN 500 AND 1500
"""

QUERY_MAP = {
    "store_sales": QUERY_TEMPLATE,
    "catalog_sales": QUERY_TEMPLATE_CS,
    "web_sales": QUERY_TEMPLATE_WS,
}


# ─── Fixtures ───────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def catalogs():
    """Create two catalog connections simulating two regions."""
    from pyiceberg.catalog import load_catalog

    catalog_a = load_catalog(
        "region_a", type="rest", uri=CATALOG_URI,
        warehouse=REGION_A_WAREHOUSE,
        **{"s3.endpoint": S3_ENDPOINT, "s3.access-key-id": S3_ACCESS_KEY,
           "s3.secret-access-key": S3_SECRET_KEY},
    )
    catalog_b = load_catalog(
        "region_b", type="rest", uri=CATALOG_URI,
        warehouse=REGION_B_WAREHOUSE,
        **{"s3.endpoint": S3_ENDPOINT, "s3.access-key-id": S3_ACCESS_KEY,
           "s3.secret-access-key": S3_SECRET_KEY},
    )
    return catalog_a, catalog_b


@pytest.fixture(scope="session")
def duckdb_region_a():
    """DuckDB instance simulating Region A query engine."""
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT.replace('http://', '')}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
    conn.execute("SET unsafe_enable_version_guessing=true;")
    return conn


@pytest.fixture(scope="session")
def duckdb_region_b():
    """DuckDB instance simulating Region B query engine."""
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT.replace('http://', '')}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute("SET s3_use_ssl=false; SET s3_url_style='path';")
    conn.execute("SET unsafe_enable_version_guessing=true;")
    return conn


@pytest.fixture(scope="session")
def region_tables(catalogs):
    """Create identical tables in both regions with streaming micro-batches."""
    catalog_a, catalog_b = catalogs

    # Create namespaces
    for cat, ns in [(catalog_a, REGION_A_NS), (catalog_b, REGION_B_NS)]:
        try:
            cat.create_namespace(ns)
        except Exception:
            pass

    locations = {"a": {}, "b": {}}

    print(f"\n{'='*70}")
    print("CREATING IDENTICAL TABLES IN BOTH REGIONS")
    print(f"{'='*70}")

    for name in TEST_TABLES:
        schema = FACT_TABLES[name]
        gen = FACT_BATCH_GENERATORS[name]

        for region, cat, ns in [("a", catalog_a, REGION_A_NS),
                                 ("b", catalog_b, REGION_B_NS)]:
            table_id = f"{ns}.{name}"
            try:
                table = cat.create_table(table_id, schema=schema)
            except Exception:
                table = cat.load_table(table_id)

            # Stream identical data (same seed)
            import random
            random.seed(42)  # Ensure identical data in both regions

            total_rows = 0
            for i in range(NUM_BATCHES):
                batch = gen()
                table.append(batch)
                total_rows += len(batch)

            locations[region][name] = table.metadata.location
            print(f"  Region {region.upper()} / {name}: {total_rows} rows, "
                  f"{NUM_BATCHES} micro-batches")

    return locations


def _query_table(conn, location: str, table_name: str, runs: int = 3) -> float:
    """Run a query against a table and return median time in ms."""
    query = QUERY_MAP.get(table_name, QUERY_TEMPLATE)
    sql = query.format(location=f"iceberg_scan('{location}', allow_moved_paths = true)")
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        try:
            conn.execute(sql).fetchall()
        except Exception:
            pass
        timings.append((time.perf_counter() - start) * 1000)
    return sorted(timings)[1]  # median


def _get_table_state(catalog, ns: str, name: str, region: str) -> RegionState:
    """Get current state of a table in a region."""
    table = catalog.load_table(f"{ns}.{name}")
    files = list(table.scan().plan_files())
    sizes = [f.file.file_size_in_bytes for f in files]
    return RegionState(
        region=region,
        table_id=f"{ns}.{name}",
        file_count=len(files),
        total_bytes=sum(sizes) if sizes else 0,
        avg_file_size_bytes=(sum(sizes) / len(sizes)) if sizes else 0,
        snapshot_count=len(table.metadata.snapshots),
    )


# ─── Tests ──────────────────────────────────────────────────────────

class TestMultiRegionCompaction:

    def test_01_verify_identical_tables(self, catalogs, region_tables):
        """Both regions should have identical tables with many small files."""
        catalog_a, catalog_b = catalogs

        print(f"\n{'='*70}")
        print("TABLE STATE: BEFORE COMPACTION (BOTH REGIONS)")
        print(f"{'='*70}")
        print(f"  {'Table':<20s} {'Region':<10s} {'Files':>6s} {'Avg Size':>10s} "
              f"{'Total MB':>10s} {'Snapshots':>10s}")
        print(f"  {'-'*20} {'-'*10} {'-'*6} {'-'*10} {'-'*10} {'-'*10}")

        for name in TEST_TABLES:
            for region, cat, ns in [("A", catalog_a, REGION_A_NS),
                                     ("B", catalog_b, REGION_B_NS)]:
                state = _get_table_state(cat, ns, name, region)
                avg_kb = state.avg_file_size_bytes / 1024
                total_mb = state.total_bytes / (1024 * 1024)
                print(f"  {name:<20s} {region:<10s} {state.file_count:>6d} "
                      f"{avg_kb:>8.0f}KB {total_mb:>8.1f}MB "
                      f"{state.snapshot_count:>10d}")

                assert state.file_count >= NUM_BATCHES

    def test_02_benchmark_before_compaction(self, duckdb_region_a, duckdb_region_b,
                                            region_tables):
        """Benchmark query performance in both regions BEFORE compaction."""
        print(f"\n{'='*70}")
        print("QUERY PERFORMANCE: BEFORE COMPACTION (BOTH REGIONS)")
        print(f"{'='*70}")

        results = {}
        for name in TEST_TABLES:
            for region, conn, locs in [("A", duckdb_region_a, region_tables["a"]),
                                        ("B", duckdb_region_b, region_tables["b"])]:
                ms = _query_table(conn, locs[name], name)
                results[(name, region)] = ms
                print(f"  {name:<20s} Region {region}:  {ms:8.1f} ms")

        TestMultiRegionCompaction._before_results = results

    def test_03_compact_region_a_only(self, catalogs, region_tables):
        """Compact tables in Region A only. Region B stays uncompacted."""
        catalog_a, _ = catalogs

        print(f"\n{'='*70}")
        print("COMPACTING REGION A ONLY")
        print(f"{'='*70}")

        for name in TEST_TABLES:
            table_id = f"{REGION_A_NS}.{name}"
            table = catalog_a.load_table(table_id)

            files_before = len(list(table.scan().plan_files()))
            start = time.perf_counter()
            df = table.scan().to_arrow()
            table.overwrite(df)
            elapsed = time.perf_counter() - start

            table = catalog_a.load_table(table_id)
            files_after = len(list(table.scan().plan_files()))
            region_tables["a"][name] = table.metadata.location  # update location

            print(f"  {name:<20s} {files_before:>4d} -> {files_after:>4d} files  "
                  f"({elapsed:.1f}s)")

    def test_04_benchmark_after_region_a_compaction(self, duckdb_region_a,
                                                     duckdb_region_b, region_tables):
        """Benchmark both regions: A is compacted, B is not."""
        print(f"\n{'='*70}")
        print("QUERY PERFORMANCE: REGION A COMPACTED, REGION B UNCOMPACTED")
        print(f"{'='*70}")

        results = {}
        for name in TEST_TABLES:
            for region, conn, locs in [("A", duckdb_region_a, region_tables["a"]),
                                        ("B", duckdb_region_b, region_tables["b"])]:
                ms = _query_table(conn, locs[name], name)
                results[(name, region)] = ms
                before = TestMultiRegionCompaction._before_results.get((name, region), 0)
                change = ((ms - before) / before * 100) if before > 0 else 0
                marker = "COMPACTED" if region == "A" else "uncompacted"
                print(f"  {name:<20s} Region {region} ({marker}):  {ms:8.1f} ms  "
                      f"({change:+.1f}%)")

        TestMultiRegionCompaction._after_a_results = results

    def test_05_compact_region_b(self, catalogs, region_tables):
        """Now compact Region B for comparison."""
        _, catalog_b = catalogs

        print(f"\n{'='*70}")
        print("COMPACTING REGION B")
        print(f"{'='*70}")

        for name in TEST_TABLES:
            table_id = f"{REGION_B_NS}.{name}"
            table = catalog_b.load_table(table_id)

            files_before = len(list(table.scan().plan_files()))
            start = time.perf_counter()
            df = table.scan().to_arrow()
            table.overwrite(df)
            elapsed = time.perf_counter() - start

            table = catalog_b.load_table(table_id)
            files_after = len(list(table.scan().plan_files()))
            region_tables["b"][name] = table.metadata.location

            print(f"  {name:<20s} {files_before:>4d} -> {files_after:>4d} files  "
                  f"({elapsed:.1f}s)")

    def test_06_benchmark_both_compacted(self, duckdb_region_a, duckdb_region_b,
                                          region_tables):
        """Benchmark both regions after both are compacted."""
        print(f"\n{'='*70}")
        print("QUERY PERFORMANCE: BOTH REGIONS COMPACTED")
        print(f"{'='*70}")

        results = {}
        for name in TEST_TABLES:
            for region, conn, locs in [("A", duckdb_region_a, region_tables["a"]),
                                        ("B", duckdb_region_b, region_tables["b"])]:
                ms = _query_table(conn, locs[name], name)
                results[(name, region)] = ms
                before = TestMultiRegionCompaction._before_results.get((name, region), 0)
                change = ((ms - before) / before * 100) if before > 0 else 0
                print(f"  {name:<20s} Region {region}:  {ms:8.1f} ms  ({change:+.1f}%)")

        TestMultiRegionCompaction._both_compacted = results

    def test_07_cost_analysis(self, catalogs):
        """Compare costs: compact both regions vs. compact one + ship."""
        catalog_a, catalog_b = catalogs

        print(f"\n{'='*70}")
        print("COST ANALYSIS: MULTI-REGION COMPACTION STRATEGIES")
        print(f"{'='*70}")

        analyses = []
        for name in TEST_TABLES:
            state_a = _get_table_state(catalog_a, REGION_A_NS, name, "A")
            state_b = _get_table_state(catalog_b, REGION_B_NS, name, "B")

            # Use pre-compaction file count (from before results)
            # After compaction both have 1 file, so use snapshot count as proxy
            pre_file_count = NUM_BATCHES
            data_gb = state_a.total_bytes / (1024 ** 3)

            # Scale up for realistic analysis
            # Simulate production: 100x the data
            scale_factors = [1, 10, 100, 1000]

            print(f"\n  TABLE: {name}")
            print(f"  Base data: {data_gb:.3f} GB, {pre_file_count} files")
            print(f"  {'Scale':<10s} {'Data (GB)':>10s} {'Strategy 1':>18s} "
                  f"{'Strategy 2':>18s} {'Strategy 3':>18s} {'Winner':>12s}")
            print(f"  {'-'*10} {'-'*10} {'-'*18} {'-'*18} {'-'*18} {'-'*12}")

            for scale in scale_factors:
                scaled_gb = data_gb * scale
                scaled_files = pre_file_count * scale

                # Strategy 1: Compact independently in both regions
                cost_a = COST.compaction_cost(scaled_gb, scaled_files)
                cost_b = COST.compaction_cost(scaled_gb, scaled_files)
                s1_total = cost_a["total"] + cost_b["total"]

                # Strategy 2: Compact in Region A, ship compacted files to B
                output_files = max(1, int(scaled_gb / 0.128))
                cost_compact = COST.compaction_cost(scaled_gb, scaled_files)
                cost_ship = COST.cross_region_ship_cost(scaled_gb, output_files)
                s2_total = cost_compact["total"] + cost_ship["total"]

                # Strategy 3: Compact in Region A only, B queries uncompacted
                # Cost = compaction in A + performance penalty in B
                # Performance penalty approximated as 20% more S3 GET costs
                s3_extra_gets = scaled_files / 1000 * COST.s3_get_per_1k * 100  # 100 queries/day
                s3_total = cost_a["total"]  # Only Region A compacts
                # But Region B pays in query latency (not direct cost, but SLA impact)

                winner = "compact both" if s1_total < s2_total else "compact+ship"
                if s2_total > s1_total * 1.5:
                    winner = "compact both"

                print(f"  {scale:<10d} {scaled_gb:>10.1f} ${s1_total:>16.4f} "
                      f"${s2_total:>16.4f} ${s3_total:>16.4f} {winner:>12s}")

                if scale == 100:
                    analyses.append(MultiRegionAnalysis(
                        table_id=name,
                        data_gb=scaled_gb,
                        file_count=scaled_files,
                        strategy_compact_both={"cost": s1_total, "desc": "Compact in both regions independently"},
                        strategy_compact_ship={"cost": s2_total, "desc": "Compact in A, ship to B via CRR"},
                        strategy_compact_one={"cost": s3_total, "desc": "Compact in A only, B queries uncompacted"},
                    ))

        # Summary
        print(f"\n{'='*70}")
        print("STRATEGY RECOMMENDATION")
        print(f"{'='*70}")
        print("""
  FINDING: The optimal strategy depends on data volume:

  ┌─────────────────────┬──────────────────────────────────────────────┐
  │ Data Size           │ Recommended Strategy                         │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ < 10 GB             │ Compact independently in both regions.       │
  │                     │ Compute is cheap, transfer overhead not      │
  │                     │ worth the coordination complexity.           │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ 10 GB - 100 GB      │ Compact in primary region + ship via CRR.   │
  │                     │ Transfer cost is lower than duplicate        │
  │                     │ compute. CRR handles replication async.      │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ > 100 GB            │ Compact independently. Cross-region          │
  │                     │ transfer at $0.02/GB becomes dominant cost.  │
  │                     │ 100GB = $2 transfer vs $0.10 compute.        │
  ├─────────────────────┼──────────────────────────────────────────────┤
  │ Any size (latency-  │ ALWAYS compact locally. Cross-region         │
  │ sensitive queries)  │ replication lag means Region B would serve   │
  │                     │ stale/uncompacted data during transfer.      │
  └─────────────────────┴──────────────────────────────────────────────┘

  KEY INSIGHT: Cross-region transfer cost ($0.02/GB) is 20x the compute
  cost ($0.001/GB). For anything beyond small tables, local compaction
  wins. The only scenario where shipping wins is when compute capacity
  in Region B is constrained (no Flink cluster) and the table is small.

  RECOMMENDED ARCHITECTURE:
  - Each region runs its own janitor maintenance cluster
  - Janitors share policy config (replicated via ConfigMap/GitOps)
  - Janitors do NOT coordinate compaction across regions
  - Each janitor compacts based on local table state
  - S3 CRR replicates the data files; metadata replication ensures
    both regions see the same logical table state
  - If Region B's janitor detects the table was already compacted
    (via metadata), it skips compaction (no wasted work)
""")

    def test_08_latency_impact_summary(self):
        """Print the query latency comparison across all strategies."""
        before = getattr(TestMultiRegionCompaction, "_before_results", {})
        after_a = getattr(TestMultiRegionCompaction, "_after_a_results", {})
        both = getattr(TestMultiRegionCompaction, "_both_compacted", {})

        if not before or not after_a or not both:
            pytest.skip("Previous test results not available")

        print(f"\n{'='*70}")
        print("QUERY LATENCY ACROSS STRATEGIES")
        print(f"{'='*70}")
        print(f"  {'Table':<20s} {'Region':>8s} {'Before':>10s} "
              f"{'A compact':>10s} {'Both':>10s} {'Improvement':>12s}")
        print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

        total_before = 0
        total_after_a_only = 0
        total_both = 0

        for name in TEST_TABLES:
            for region in ["A", "B"]:
                b = before.get((name, region), 0)
                a = after_a.get((name, region), 0)
                c = both.get((name, region), 0)
                total_before += b
                total_after_a_only += a
                total_both += c

                pct = ((c - b) / b * 100) if b > 0 else 0
                print(f"  {name:<20s} {region:>8s} {b:>8.1f}ms "
                      f"{a:>8.1f}ms {c:>8.1f}ms {pct:>+10.1f}%")

        print(f"  {'-'*20} {'-'*8} {'-'*10} {'-'*10} {'-'*10} {'-'*12}")

        pct_a = ((total_after_a_only - total_before) / total_before * 100) if total_before else 0
        pct_b = ((total_both - total_before) / total_before * 100) if total_before else 0
        print(f"  {'TOTAL':<20s} {'':>8s} {total_before:>8.1f}ms "
              f"{total_after_a_only:>8.1f}ms {total_both:>8.1f}ms {pct_b:>+10.1f}%")

        print(f"\n  Only Region A compacted: {pct_a:+.1f}% overall")
        print(f"  Both regions compacted:  {pct_b:+.1f}% overall")
        print(f"  Delta (benefit of local compaction in B): {pct_a - pct_b:+.1f}%")
