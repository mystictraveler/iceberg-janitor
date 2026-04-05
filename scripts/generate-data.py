#!/usr/bin/env python3
"""Generate synthetic streaming data to create the small-file problem in Iceberg.

Simulates high-frequency micro-batch writes that create many small files —
exactly the scenario iceberg-janitor is built to fix.
"""

import os
import random
import string
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
from pyiceberg.catalog import load_catalog

CATALOG_URI = os.environ.get("CATALOG_URI", "http://localhost:8181")
WAREHOUSE = os.environ.get("WAREHOUSE", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
NUM_BATCHES = int(os.environ.get("NUM_BATCHES", "200"))
ROWS_PER_BATCH = int(os.environ.get("ROWS_PER_BATCH", "100"))
NAMESPACE = os.environ.get("NAMESPACE", "default")
TABLE_NAME = os.environ.get("TABLE_NAME", "events")

EVENT_TYPES = ["click", "pageview", "purchase", "signup", "logout", "search", "add_to_cart"]


def random_payload(size: int = 200) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=size))


def generate_batch(rows: int) -> pa.Table:
    return pa.table({
        "event_id": [str(uuid.uuid4()) for _ in range(rows)],
        "event_type": [random.choice(EVENT_TYPES) for _ in range(rows)],
        "user_id": [f"user_{random.randint(1, 10000)}" for _ in range(rows)],
        "payload": [random_payload() for _ in range(rows)],
        "event_time": [datetime.now(timezone.utc) for _ in range(rows)],
    })


def main():
    print(f"Connecting to catalog at {CATALOG_URI}")

    catalog = load_catalog(
        "default",
        type="rest",
        uri=CATALOG_URI,
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
            "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "password"),
        },
    )

    # Create namespace
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"Created namespace: {NAMESPACE}")
    except Exception:
        print(f"Namespace {NAMESPACE} already exists")

    # Create table if not exists
    table_id = f"{NAMESPACE}.{TABLE_NAME}"
    schema = pa.schema([
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("user_id", pa.string()),
        ("payload", pa.string()),
        ("event_time", pa.timestamp("us", tz="UTC")),
    ])

    try:
        table = catalog.create_table(table_id, schema=schema)
        print(f"Created table: {table_id}")
    except Exception:
        table = catalog.load_table(table_id)
        print(f"Table {table_id} already exists")

    # Write many small batches to simulate streaming micro-batches
    print(f"\nWriting {NUM_BATCHES} batches of {ROWS_PER_BATCH} rows each...")
    print("This intentionally creates many small files to demonstrate the problem.\n")

    for i in range(NUM_BATCHES):
        batch = generate_batch(ROWS_PER_BATCH)
        table.append(batch)

        if (i + 1) % 10 == 0:
            print(f"  Batch {i + 1}/{NUM_BATCHES} — "
                  f"{(i + 1) * ROWS_PER_BATCH} total rows written")

        # Small delay to spread out timestamps
        time.sleep(0.05)

    total_rows = NUM_BATCHES * ROWS_PER_BATCH
    print(f"\nDone! Wrote {total_rows} rows across {NUM_BATCHES} commits.")
    print(f"This created ~{NUM_BATCHES} small data files and {NUM_BATCHES} snapshots.")
    print(f"\nRun 'janitor analyze' to see the damage:")
    print(f"  janitor analyze {table.metadata.location}")


if __name__ == "__main__":
    main()
