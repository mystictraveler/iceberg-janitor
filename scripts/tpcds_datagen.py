"""TPC-DS synthetic data generator for Iceberg tables.

Generates realistic data for all 24 TPC-DS tables:
- Dimension tables: loaded as a single batch
- Fact tables: loaded as streaming micro-batches (creating the small-file problem)
"""

from __future__ import annotations

import random
import string
from datetime import date, timedelta

import pyarrow as pa

from scripts.tpcds_schema import DIMENSION_TABLES, FACT_TABLES

# ─── Reproducible randomness ────────────────────────────────────────
_RNG = random.Random(42)

# ─── Reference data pools ───────────────────────────────────────────
STATES = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI",
          "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI", "CO", "MN"]
CITIES = ["Springfield", "Franklin", "Clinton", "Madison", "Georgetown",
          "Salem", "Fairview", "Bristol", "Oxford", "Manchester"]
COUNTRIES = ["United States"]
GENDERS = ["M", "F"]
MARITAL = ["S", "M", "D", "W", "U"]
EDUCATION = ["Primary", "Secondary", "College", "2 yr Degree", "4 yr Degree",
             "Advanced Degree", "Unknown"]
CREDIT = ["Low Risk", "Medium Risk", "High Risk", "Unknown"]
COLORS = ["red", "blue", "green", "yellow", "white", "black", "brown",
          "purple", "orange", "pink"]
SIZES = ["small", "medium", "large", "extra large", "petite", "economy", "N/A"]
CATEGORIES = ["Electronics", "Books", "Music", "Home", "Sports", "Shoes",
              "Children", "Women", "Men", "Jewelry"]
BRANDS = [f"Brand #{i}" for i in range(1, 51)]
SHIP_MODES = ["REGULAR", "EXPRESS", "OVERNIGHT", "TWO DAY", "LIBRARY"]
CARRIERS = ["UPS", "FEDEX", "DHL", "USPS", "AIRBORNE", "GREAT EASTERN"]
HOURS = ["8AM-4PM", "4PM-12AM", "12AM-8AM"]

BASE_DATE = date(2020, 1, 1)
NUM_DATES = 2190  # ~6 years
NUM_TIMES = 86400
NUM_ITEMS = 5000
NUM_CUSTOMERS = 20000
NUM_ADDRESSES = 15000
NUM_STORES = 50
NUM_WAREHOUSES = 10
NUM_PROMOTIONS = 200
NUM_CALL_CENTERS = 10
NUM_CATALOG_PAGES = 500
NUM_WEB_SITES = 20
NUM_WEB_PAGES = 200


def _rand_str(n: int) -> str:
    return "".join(_RNG.choices(string.ascii_uppercase + string.digits, k=n))


def _rand_date(start: date = BASE_DATE, days: int = NUM_DATES) -> date:
    return start + timedelta(days=_RNG.randint(0, days - 1))


# ─── Dimension generators ───────────────────────────────────────────

def gen_date_dim(n: int = NUM_DATES) -> pa.Table:
    rows = []
    for i in range(n):
        d = BASE_DATE + timedelta(days=i)
        rows.append({
            "d_date_sk": i + 1, "d_date_id": f"AAAAAA{i:06d}",
            "d_date": d, "d_month_seq": i // 30, "d_week_seq": i // 7,
            "d_quarter_seq": i // 90, "d_year": d.year, "d_dow": d.weekday(),
            "d_moy": d.month, "d_dom": d.day, "d_qoy": (d.month - 1) // 3 + 1,
            "d_fy_year": d.year, "d_fy_quarter_seq": i // 90,
            "d_fy_week_seq": i // 7, "d_day_name": d.strftime("%A"),
            "d_quarter_name": f"{d.year}Q{(d.month-1)//3+1}",
            "d_holiday": _RNG.choice(["Y", "N"]),
            "d_weekend": "Y" if d.weekday() >= 5 else "N",
            "d_following_holiday": _RNG.choice(["Y", "N"]),
            "d_first_dom": 1, "d_last_dom": 28 + _RNG.randint(0, 3),
            "d_same_day_ly": max(1, i - 365), "d_same_day_lq": max(1, i - 90),
            "d_current_day": "N", "d_current_week": "N",
            "d_current_month": "N", "d_current_quarter": "N",
            "d_current_year": "N",
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["date_dim"])


def gen_time_dim(n: int = 86400) -> pa.Table:
    rows = []
    for i in range(0, n, 60):  # one row per minute = 1440 rows
        h, m = divmod(i // 60, 60)
        h = h % 24
        rows.append({
            "t_time_sk": len(rows) + 1, "t_time_id": f"TTTT{len(rows):06d}",
            "t_time": i, "t_hour": h, "t_minute": m, "t_second": 0,
            "t_am_pm": "AM" if h < 12 else "PM",
            "t_shift": "first" if h < 8 else ("second" if h < 16 else "third"),
            "t_sub_shift": "night" if h < 6 else "day",
            "t_meal_time": "breakfast" if 6 <= h < 9 else ("lunch" if 11 <= h < 14 else "dinner" if 17 <= h < 20 else ""),
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["time_dim"])


def gen_item(n: int = NUM_ITEMS) -> pa.Table:
    rows = []
    for i in range(n):
        cat = _RNG.choice(CATEGORIES)
        rows.append({
            "i_item_sk": i + 1, "i_item_id": f"ITEM{i:08d}",
            "i_rec_start_date": _rand_date(), "i_rec_end_date": _rand_date(),
            "i_item_desc": f"Description for item {i}",
            "i_current_price": round(_RNG.uniform(1, 500), 2),
            "i_wholesale_cost": round(_RNG.uniform(0.5, 250), 2),
            "i_brand_id": _RNG.randint(1, 50), "i_brand": _RNG.choice(BRANDS),
            "i_class_id": _RNG.randint(1, 15), "i_class": f"Class {_RNG.randint(1,15)}",
            "i_category_id": CATEGORIES.index(cat) + 1, "i_category": cat,
            "i_manufact_id": _RNG.randint(1, 100), "i_manufact": f"Manufact {_RNG.randint(1,100)}",
            "i_size": _RNG.choice(SIZES), "i_formulation": _rand_str(10),
            "i_color": _RNG.choice(COLORS), "i_units": _RNG.choice(["Each", "Oz", "Lb", "Gram", "Dram"]),
            "i_container": _RNG.choice(["Unknown", "Wrap", "Box", "Bag"]),
            "i_manager_id": _RNG.randint(1, 50), "i_product_name": f"product_{i}",
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["item"])


def gen_customer(n: int = NUM_CUSTOMERS) -> pa.Table:
    rows = []
    for i in range(n):
        rows.append({
            "c_customer_sk": i + 1, "c_customer_id": f"CUST{i:08d}",
            "c_current_cdemo_sk": _RNG.randint(1, 1920),
            "c_current_hdemo_sk": _RNG.randint(1, 7200),
            "c_current_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "c_first_shipto_date_sk": _RNG.randint(1, NUM_DATES),
            "c_first_sales_date_sk": _RNG.randint(1, NUM_DATES),
            "c_salutation": _RNG.choice(["Mr.", "Mrs.", "Ms.", "Dr.", "Sir"]),
            "c_first_name": f"First{i}", "c_last_name": f"Last{i}",
            "c_preferred_cust_flag": _RNG.choice(["Y", "N"]),
            "c_birth_day": _RNG.randint(1, 28), "c_birth_month": _RNG.randint(1, 12),
            "c_birth_year": _RNG.randint(1940, 2005), "c_birth_country": "US",
            "c_login": f"user{i}", "c_email_address": f"user{i}@example.com",
            "c_last_review_date_sk": _RNG.randint(1, NUM_DATES),
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["customer"])


def gen_customer_address(n: int = NUM_ADDRESSES) -> pa.Table:
    rows = []
    for i in range(n):
        rows.append({
            "ca_address_sk": i + 1, "ca_address_id": f"ADDR{i:08d}",
            "ca_street_number": str(_RNG.randint(1, 9999)),
            "ca_street_name": f"{_RNG.choice(['Oak','Elm','Main','1st','2nd'])} St",
            "ca_street_type": _RNG.choice(["St", "Ave", "Blvd", "Dr", "Ln"]),
            "ca_suite_number": f"Suite {_RNG.randint(1,500)}",
            "ca_city": _RNG.choice(CITIES), "ca_county": f"{_RNG.choice(CITIES)} County",
            "ca_state": _RNG.choice(STATES), "ca_zip": f"{_RNG.randint(10000,99999)}",
            "ca_country": "United States", "ca_gmt_offset": _RNG.choice([-5.0, -6.0, -7.0, -8.0]),
            "ca_location_type": _RNG.choice(["single family", "apartment", "condo"]),
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["customer_address"])


def gen_customer_demographics() -> pa.Table:
    rows = []
    sk = 1
    for g in GENDERS:
        for m in MARITAL:
            for e in EDUCATION:
                for cr in CREDIT:
                    rows.append({
                        "cd_demo_sk": sk, "cd_gender": g, "cd_marital_status": m,
                        "cd_education_status": e, "cd_purchase_estimate": _RNG.randint(500, 10000),
                        "cd_credit_rating": cr, "cd_dep_count": _RNG.randint(0, 6),
                        "cd_dep_employed_count": _RNG.randint(0, 6),
                        "cd_dep_college_count": _RNG.randint(0, 6),
                    })
                    sk += 1
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["customer_demographics"])


def gen_household_demographics() -> pa.Table:
    rows = []
    for i in range(7200):
        rows.append({
            "hd_demo_sk": i + 1, "hd_income_band_sk": _RNG.randint(1, 20),
            "hd_buy_potential": _RNG.choice(["Unknown", "0-500", "501-1000", "1001-5000", "5001-10000", ">10000"]),
            "hd_dep_count": _RNG.randint(0, 9), "hd_vehicle_count": _RNG.randint(0, 4),
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["household_demographics"])


def gen_income_band() -> pa.Table:
    rows = [{"ib_income_band_sk": i + 1, "ib_lower_bound": i * 10000, "ib_upper_bound": (i + 1) * 10000} for i in range(20)]
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["income_band"])


def gen_store(n: int = NUM_STORES) -> pa.Table:
    rows = []
    for i in range(n):
        rows.append({
            "s_store_sk": i + 1, "s_store_id": f"STORE{i:06d}",
            "s_rec_start_date": _rand_date(), "s_rec_end_date": _rand_date(),
            "s_closed_date_sk": 0, "s_store_name": f"Store {i}",
            "s_number_employees": _RNG.randint(50, 500),
            "s_floor_space": _RNG.randint(5000, 100000), "s_hours": _RNG.choice(HOURS),
            "s_manager": f"Manager {i}", "s_market_id": _RNG.randint(1, 10),
            "s_geography_class": "medium", "s_market_desc": f"Market desc {i}",
            "s_market_manager": f"MktMgr {i}", "s_division_id": _RNG.randint(1, 5),
            "s_division_name": f"Div {_RNG.randint(1,5)}",
            "s_company_id": _RNG.randint(1, 3), "s_company_name": f"Company {_RNG.randint(1,3)}",
            "s_street_number": str(_RNG.randint(1, 999)), "s_street_name": f"Store St {i}",
            "s_street_type": "Ave", "s_suite_number": "", "s_city": _RNG.choice(CITIES),
            "s_county": f"{_RNG.choice(CITIES)} County", "s_state": _RNG.choice(STATES),
            "s_zip": f"{_RNG.randint(10000,99999)}", "s_country": "United States",
            "s_gmt_offset": -5.0, "s_tax_percentage": round(_RNG.uniform(0, 0.12), 2),
        })
    return pa.Table.from_pylist(rows, schema=DIMENSION_TABLES["store"])


def gen_small_dim(name: str, schema: pa.schema, n: int) -> pa.Table:
    """Generic generator for small dimension tables."""
    rows = []
    field_names = [f.name for f in schema]
    for i in range(n):
        row = {}
        for f in schema:
            if f.name.endswith("_sk"):
                row[f.name] = i + 1
            elif f.name.endswith("_id"):
                row[f.name] = f"{name.upper()[:4]}{i:06d}"
            elif f.type == pa.date32():
                row[f.name] = _rand_date()
            elif f.type == pa.int32():
                row[f.name] = _RNG.randint(1, 100)
            elif f.type == pa.int64():
                row[f.name] = _RNG.randint(1, 10000)
            elif f.type == pa.float64():
                row[f.name] = round(_RNG.uniform(-10, 100), 2)
            else:
                row[f.name] = f"{name}_{f.name}_{i}"
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


# ─── Fact table generators (micro-batch) ────────────────────────────

def gen_store_sales_batch(batch_size: int = 500) -> pa.Table:
    rows = []
    for _ in range(batch_size):
        qty = _RNG.randint(1, 100)
        price = round(_RNG.uniform(1, 500), 2)
        wholesale = round(price * _RNG.uniform(0.3, 0.7), 2)
        discount = round(price * _RNG.uniform(0, 0.3), 2)
        tax = round(price * qty * _RNG.uniform(0.05, 0.12), 2)
        rows.append({
            "ss_sold_date_sk": _RNG.randint(1, NUM_DATES),
            "ss_sold_time_sk": _RNG.randint(1, 1440),
            "ss_item_sk": _RNG.randint(1, NUM_ITEMS),
            "ss_customer_sk": _RNG.randint(1, NUM_CUSTOMERS),
            "ss_cdemo_sk": _RNG.randint(1, 1920),
            "ss_hdemo_sk": _RNG.randint(1, 7200),
            "ss_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "ss_store_sk": _RNG.randint(1, NUM_STORES),
            "ss_promo_sk": _RNG.randint(1, NUM_PROMOTIONS),
            "ss_ticket_number": _RNG.randint(1, 1_000_000),
            "ss_quantity": qty, "ss_wholesale_cost": wholesale,
            "ss_list_price": price, "ss_sales_price": round(price - discount, 2),
            "ss_ext_discount_amt": round(discount * qty, 2),
            "ss_ext_sales_price": round((price - discount) * qty, 2),
            "ss_ext_wholesale_cost": round(wholesale * qty, 2),
            "ss_ext_list_price": round(price * qty, 2),
            "ss_ext_tax": tax, "ss_coupon_amt": round(_RNG.uniform(0, 50), 2),
            "ss_net_paid": round((price - discount) * qty, 2),
            "ss_net_paid_inc_tax": round((price - discount) * qty + tax, 2),
            "ss_net_profit": round((price - discount - wholesale) * qty, 2),
        })
    return pa.Table.from_pylist(rows, schema=FACT_TABLES["store_sales"])


def gen_catalog_sales_batch(batch_size: int = 300) -> pa.Table:
    rows = []
    for _ in range(batch_size):
        qty = _RNG.randint(1, 50)
        price = round(_RNG.uniform(5, 1000), 2)
        wholesale = round(price * _RNG.uniform(0.3, 0.6), 2)
        discount = round(price * _RNG.uniform(0, 0.25), 2)
        tax = round(price * qty * 0.08, 2)
        ship = round(_RNG.uniform(3, 25), 2)
        rows.append({
            "cs_sold_date_sk": _RNG.randint(1, NUM_DATES),
            "cs_sold_time_sk": _RNG.randint(1, 1440),
            "cs_ship_date_sk": _RNG.randint(1, NUM_DATES),
            "cs_bill_customer_sk": _RNG.randint(1, NUM_CUSTOMERS),
            "cs_bill_cdemo_sk": _RNG.randint(1, 1920),
            "cs_bill_hdemo_sk": _RNG.randint(1, 7200),
            "cs_bill_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "cs_ship_customer_sk": _RNG.randint(1, NUM_CUSTOMERS),
            "cs_ship_cdemo_sk": _RNG.randint(1, 1920),
            "cs_ship_hdemo_sk": _RNG.randint(1, 7200),
            "cs_ship_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "cs_call_center_sk": _RNG.randint(1, NUM_CALL_CENTERS),
            "cs_catalog_page_sk": _RNG.randint(1, NUM_CATALOG_PAGES),
            "cs_ship_mode_sk": _RNG.randint(1, 5),
            "cs_warehouse_sk": _RNG.randint(1, NUM_WAREHOUSES),
            "cs_item_sk": _RNG.randint(1, NUM_ITEMS),
            "cs_promo_sk": _RNG.randint(1, NUM_PROMOTIONS),
            "cs_order_number": _RNG.randint(1, 1_000_000),
            "cs_quantity": qty, "cs_wholesale_cost": wholesale,
            "cs_list_price": price, "cs_sales_price": round(price - discount, 2),
            "cs_ext_discount_amt": round(discount * qty, 2),
            "cs_ext_sales_price": round((price - discount) * qty, 2),
            "cs_ext_wholesale_cost": round(wholesale * qty, 2),
            "cs_ext_list_price": round(price * qty, 2),
            "cs_ext_tax": tax, "cs_coupon_amt": round(_RNG.uniform(0, 30), 2),
            "cs_ext_ship_cost": round(ship * qty, 2),
            "cs_net_paid": round((price - discount) * qty, 2),
            "cs_net_paid_inc_tax": round((price - discount) * qty + tax, 2),
            "cs_net_paid_inc_ship": round((price - discount) * qty + ship, 2),
            "cs_net_paid_inc_ship_tax": round((price - discount) * qty + ship + tax, 2),
            "cs_net_profit": round((price - discount - wholesale) * qty, 2),
        })
    return pa.Table.from_pylist(rows, schema=FACT_TABLES["catalog_sales"])


def gen_web_sales_batch(batch_size: int = 200) -> pa.Table:
    rows = []
    for _ in range(batch_size):
        qty = _RNG.randint(1, 30)
        price = round(_RNG.uniform(5, 800), 2)
        wholesale = round(price * _RNG.uniform(0.3, 0.6), 2)
        discount = round(price * _RNG.uniform(0, 0.2), 2)
        tax = round(price * qty * 0.07, 2)
        ship = round(_RNG.uniform(2, 20), 2)
        rows.append({
            "ws_sold_date_sk": _RNG.randint(1, NUM_DATES),
            "ws_sold_time_sk": _RNG.randint(1, 1440),
            "ws_ship_date_sk": _RNG.randint(1, NUM_DATES),
            "ws_item_sk": _RNG.randint(1, NUM_ITEMS),
            "ws_bill_customer_sk": _RNG.randint(1, NUM_CUSTOMERS),
            "ws_bill_cdemo_sk": _RNG.randint(1, 1920),
            "ws_bill_hdemo_sk": _RNG.randint(1, 7200),
            "ws_bill_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "ws_ship_customer_sk": _RNG.randint(1, NUM_CUSTOMERS),
            "ws_ship_cdemo_sk": _RNG.randint(1, 1920),
            "ws_ship_hdemo_sk": _RNG.randint(1, 7200),
            "ws_ship_addr_sk": _RNG.randint(1, NUM_ADDRESSES),
            "ws_web_page_sk": _RNG.randint(1, NUM_WEB_PAGES),
            "ws_web_site_sk": _RNG.randint(1, NUM_WEB_SITES),
            "ws_ship_mode_sk": _RNG.randint(1, 5),
            "ws_warehouse_sk": _RNG.randint(1, NUM_WAREHOUSES),
            "ws_promo_sk": _RNG.randint(1, NUM_PROMOTIONS),
            "ws_order_number": _RNG.randint(1, 1_000_000),
            "ws_quantity": qty, "ws_wholesale_cost": wholesale,
            "ws_list_price": price, "ws_sales_price": round(price - discount, 2),
            "ws_ext_discount_amt": round(discount * qty, 2),
            "ws_ext_sales_price": round((price - discount) * qty, 2),
            "ws_ext_wholesale_cost": round(wholesale * qty, 2),
            "ws_ext_list_price": round(price * qty, 2),
            "ws_ext_tax": tax, "ws_coupon_amt": round(_RNG.uniform(0, 20), 2),
            "ws_ext_ship_cost": round(ship * qty, 2),
            "ws_net_paid": round((price - discount) * qty, 2),
            "ws_net_paid_inc_tax": round((price - discount) * qty + tax, 2),
            "ws_net_paid_inc_ship": round((price - discount) * qty + ship, 2),
            "ws_net_paid_inc_ship_tax": round((price - discount) * qty + ship + tax, 2),
            "ws_net_profit": round((price - discount - wholesale) * qty, 2),
        })
    return pa.Table.from_pylist(rows, schema=FACT_TABLES["web_sales"])


def gen_returns_batch(table_name: str, batch_size: int = 100) -> pa.Table:
    """Generate return rows for store_returns, catalog_returns, or web_returns."""
    schema = FACT_TABLES[table_name]
    rows = []
    for _ in range(batch_size):
        qty = _RNG.randint(1, 10)
        amt = round(_RNG.uniform(5, 500), 2)
        tax = round(amt * 0.08, 2)
        row = {}
        for f in schema:
            if f.name.endswith("_sk"):
                if "date" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_DATES)
                elif "time" in f.name:
                    row[f.name] = _RNG.randint(1, 1440)
                elif "customer" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_CUSTOMERS)
                elif "cdemo" in f.name:
                    row[f.name] = _RNG.randint(1, 1920)
                elif "hdemo" in f.name:
                    row[f.name] = _RNG.randint(1, 7200)
                elif "addr" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_ADDRESSES)
                elif "store" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_STORES)
                elif "item" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_ITEMS)
                elif "reason" in f.name:
                    row[f.name] = _RNG.randint(1, 35)
                elif "call_center" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_CALL_CENTERS)
                elif "catalog_page" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_CATALOG_PAGES)
                elif "ship_mode" in f.name:
                    row[f.name] = _RNG.randint(1, 5)
                elif "warehouse" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_WAREHOUSES)
                elif "web_page" in f.name:
                    row[f.name] = _RNG.randint(1, NUM_WEB_PAGES)
                else:
                    row[f.name] = _RNG.randint(1, 10000)
            elif "quantity" in f.name:
                row[f.name] = qty
            elif "number" in f.name:
                row[f.name] = _RNG.randint(1, 1_000_000)
            elif f.type == pa.float64():
                row[f.name] = round(_RNG.uniform(1, 500), 2)
            elif f.type == pa.int32():
                row[f.name] = _RNG.randint(1, 100)
            else:
                row[f.name] = _RNG.randint(1, 10000)
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=schema)


def gen_inventory_batch(batch_size: int = 500) -> pa.Table:
    rows = []
    for _ in range(batch_size):
        rows.append({
            "inv_date_sk": _RNG.randint(1, NUM_DATES),
            "inv_item_sk": _RNG.randint(1, NUM_ITEMS),
            "inv_warehouse_sk": _RNG.randint(1, NUM_WAREHOUSES),
            "inv_quantity_on_hand": _RNG.randint(0, 1000),
        })
    return pa.Table.from_pylist(rows, schema=FACT_TABLES["inventory"])


# ─── Generators registry ────────────────────────────────────────────

DIMENSION_GENERATORS = {
    "date_dim": gen_date_dim,
    "time_dim": gen_time_dim,
    "item": gen_item,
    "customer": gen_customer,
    "customer_address": gen_customer_address,
    "customer_demographics": gen_customer_demographics,
    "household_demographics": gen_household_demographics,
    "income_band": gen_income_band,
    "store": lambda: gen_store(),
    "promotion": lambda: gen_small_dim("promotion", DIMENSION_TABLES["promotion"], NUM_PROMOTIONS),
    "warehouse": lambda: gen_small_dim("warehouse", DIMENSION_TABLES["warehouse"], NUM_WAREHOUSES),
    "ship_mode": lambda: gen_small_dim("ship_mode", DIMENSION_TABLES["ship_mode"], 5),
    "reason": lambda: gen_small_dim("reason", DIMENSION_TABLES["reason"], 35),
    "call_center": lambda: gen_small_dim("call_center", DIMENSION_TABLES["call_center"], NUM_CALL_CENTERS),
    "catalog_page": lambda: gen_small_dim("catalog_page", DIMENSION_TABLES["catalog_page"], NUM_CATALOG_PAGES),
    "web_site": lambda: gen_small_dim("web_site", DIMENSION_TABLES["web_site"], NUM_WEB_SITES),
    "web_page": lambda: gen_small_dim("web_page", DIMENSION_TABLES["web_page"], NUM_WEB_PAGES),
}

FACT_BATCH_GENERATORS = {
    "store_sales": gen_store_sales_batch,
    "catalog_sales": gen_catalog_sales_batch,
    "web_sales": gen_web_sales_batch,
    "store_returns": lambda batch_size=100: gen_returns_batch("store_returns", batch_size),
    "catalog_returns": lambda batch_size=80: gen_returns_batch("catalog_returns", batch_size),
    "web_returns": lambda batch_size=60: gen_returns_batch("web_returns", batch_size),
    "inventory": gen_inventory_batch,
}
