"""
include/nammamart/config.py
---------------------------
Paths, reference lists and small helpers shared by both NammaMart pipelines.

Everything that a DAG run can override (data folder, warehouse file, "as of" time)
is resolved in ONE place (`resolve_runtime`) so the ETL and ELT DAGs behave the same way.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pendulum

OWNER = "sudarshan"          # suffix for DAG ids, warehouse file and quarantine folder
TZ = "Asia/Kolkata"          # NammaMart works in IST; Airflow itself stores UTC

INCLUDE_DIR = Path(__file__).resolve().parents[1]
SQL_DIR = INCLUDE_DIR / "sql"

DEFAULT_DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
DEFAULT_WAREHOUSE = INCLUDE_DIR / f"warehouse_{OWNER}.duckdb"
QUARANTINE_ROOT = INCLUDE_DIR / "quarantine" / OWNER
WORK_ROOT = INCLUDE_DIR / "work" / OWNER

ETL_DAG_ID = f"nammamart_etl_{OWNER}"
ELT_DAG_ID = f"nammamart_elt_{OWNER}"

# --------------------------------------------------------------------------- source files
# The five files the daily DAGs read, plus the glob for next-day increment files.
SOURCE_FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
}
INCREMENT_GLOB = "orders_*_increment.csv"

# Schema contract: if a header does not match, extract fails loudly instead of loading wrong data.
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price",
               "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status",
                 "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date",
                  "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit",
                 "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}
EXPECTED_COLUMNS["increment"] = EXPECTED_COLUMNS["orders"]

# --------------------------------------------------------------------------- reference lists
ORDER_STATUSES = ("DELIVERED", "CANCELLED", "RETURNED")
PAYMENT_STATUSES = ("SUCCESS", "FAILED", "REFUNDED")
PAYMENT_MODES = ("UPI", "Card", "Cash on Delivery", "Wallet")
# Known spellings of the four valid modes. Anything else (e.g. "Cashh") is NOT guessed - it is rejected.
PAYMENT_MODE_ALIASES = {
    "upi": "UPI",
    "card": "Card",
    "cod": "Cash on Delivery",
    "cash on delivery": "Cash on Delivery",
    "wallet": "Wallet",
}
CHANNELS = ("App", "Web")
LOYALTY_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
ZONES = ("North", "South", "East", "West")

# --------------------------------------------------------------------------- business thresholds
AMOUNT_TOLERANCE = 0.011            # 1 paisa rounding tolerance when comparing money
MAX_DELIVERY_MINUTES = 120          # above this a delivery time is impossible -> Critical
TYPICAL_DELIVERY_RANGE = (8, 35)    # outside this (but <= 120) -> Warning
SLA_MINUTES = 15                    # "10-minute promise" KPI uses a 15 minute cut-off
FRESHNESS_HOURS = 24                # latest order older than this -> Warning
VOLUME_DROP_THRESHOLD = 0.90        # incoming orders volume down by more than 90% -> Warning
PRICE_DEVIATION_FACTOR = 3.0        # unit_price more than 3x above / below the catalogue MRP is not believable -> Critical
UNKNOWN = "UNKNOWN"                 # placeholder for a missing descriptive attribute in a dimension


# --------------------------------------------------------------------------- runtime helpers
def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are illegal in Windows file names (bind mounts)."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def now_ist() -> pendulum.DateTime:
    return pendulum.now(TZ)


def parse_as_of(value: str | None) -> str:
    """'As of' = the moment the run treats as 'now' (freshness / future-date checks). Returns 'YYYY-MM-DD HH:MM:SS' IST."""
    if value:
        return pendulum.parse(str(value), tz=TZ).format("YYYY-MM-DD HH:mm:ss")
    return now_ist().format("YYYY-MM-DD HH:mm:ss")


def resolve_runtime(params: dict | None, run_id: str) -> dict:
    """
    Turn DAG params (all optional) into the concrete runtime settings used by every task.
    Env vars NAMMAMART_DATA_DIR / NAMMAMART_WAREHOUSE / NAMMAMART_QUARANTINE_DIR / NAMMAMART_WORK_DIR are honoured
    for local tests and the experiment harness (an explicit DAG param wins over an env var).
    """
    params = params or {}
    data_dir = params.get("data_dir") or os.environ.get("NAMMAMART_DATA_DIR") or str(DEFAULT_DATA_DIR)
    warehouse = params.get("warehouse_path") or os.environ.get("NAMMAMART_WAREHOUSE") or str(DEFAULT_WAREHOUSE)
    return {
        "run_id": run_id,
        "safe_run_id": safe_run_id(run_id),
        "data_dir": str(data_dir),
        "warehouse": str(warehouse),
        "as_of": parse_as_of(params.get("as_of")),
        "load_increments": bool(params.get("load_increments", True)),
        "quarantine_dir": str(params.get("quarantine_dir") or os.environ.get("NAMMAMART_QUARANTINE_DIR") or QUARANTINE_ROOT),
        "work_dir": str(params.get("work_dir") or os.environ.get("NAMMAMART_WORK_DIR") or WORK_ROOT),
    }
