"""
include/nammamart/etl/transform.py
----------------------------------
ETL step 4 - TRANSFORM. Works only on rows that passed validation.

  * standardise  trim, fix casing, map 'upi' / 'UPI ' -> 'UPI', missing category/zone -> 'UNKNOWN'
  * deduplicate  customers and products (validation already rejected the extra copies; this asserts it)
  * mask PII     name, email and phone are masked here - the star schema never holds the raw values
  * join         orders x customers x products x stores (also proves every foreign key resolves)
  * derive       order_date, net_revenue, gross_margin, within_15_min
"""

from __future__ import annotations

import pandas as pd

from nammamart.config import LOYALTY_TIERS, UNKNOWN, ZONES
from nammamart.etl.extract import work_path
from nammamart.masking import mask_email, mask_name, mask_phone
from nammamart.standardise import capitalize, parse_date, parse_num, parse_ts, std_payment_mode, strip


def read_scored(rt: dict, name: str) -> pd.DataFrame:
    df = pd.read_csv(work_path(rt) / f"scored_{name}.csv", dtype=str, keep_default_na=False)
    df["is_duplicate_copy"] = df["is_duplicate_copy"] == "True"
    return df


def _or_none(series: pd.Series, mask: pd.Series) -> pd.Series:
    return series.where(mask, None)


def build_dim_customer(scored: pd.DataFrame, as_of: str) -> pd.DataFrame:
    d = scored[scored["dq_critical"] == ""]
    signup = parse_date(d["signup_date"])
    tier = capitalize(d["loyalty_tier"])
    pincode = strip(d["pincode"])
    active = strip(d["is_active"]).str.upper()
    city = strip(d["city"])
    out = pd.DataFrame({
        "customer_id": strip(d["customer_id"]),
        "customer_name_masked": d["full_name"].map(mask_name),
        "email_masked": d["email"].map(mask_email),
        "phone_masked": d["phone"].map(mask_phone),
        "city": city.where(city != "", UNKNOWN),
        "area": strip(d["area"]),
        "pincode": _or_none(pincode, pincode.str.match(r"^\d{6}$")),
        "signup_date": signup.where(signup <= pd.Timestamp(as_of).normalize()).dt.strftime("%Y-%m-%d"),
        "loyalty_tier": tier.where(tier.isin(LOYALTY_TIERS), UNKNOWN),
        "is_active": _or_none(active, active.isin(["Y", "N"])),
        "dq_flags": d["dq_warnings"],
        "source_row_num": d["_source_row_num"],
    })
    return out.reset_index(drop=True)


def build_dim_product(scored: pd.DataFrame, as_of: str) -> pd.DataFrame:
    d = scored[scored["dq_critical"] == ""]
    mrp = parse_num(d["mrp"])
    cost = parse_num(d["cost_price"])
    category = strip(d["category"])
    perishable = strip(d["is_perishable"]).str.lower().map({"true": "true", "false": "false"})
    launch = parse_date(d["launch_date"])
    out = pd.DataFrame({
        "product_id": strip(d["product_id"]),
        "product_name": strip(d["product_name"]),
        "category": category.where(category != "", UNKNOWN),
        "sub_category": strip(d["sub_category"]),
        "brand": strip(d["brand"]),
        "mrp": mrp.where(mrp > 0),
        "cost_price": cost.where(cost > 0),
        "unit": strip(d["unit"]),
        "is_perishable": perishable,
        "launch_date": launch.dt.strftime("%Y-%m-%d"),
        "dq_flags": d["dq_warnings"],
        "source_row_num": d["_source_row_num"],
    })
    return out.reset_index(drop=True)


def build_dim_store(scored: pd.DataFrame, as_of: str) -> pd.DataFrame:
    d = scored[scored["dq_critical"] == ""]
    zone = capitalize(d["zone"])
    cap = parse_num(d["capacity_orders_per_day"])
    pincode = strip(d["pincode"])
    out = pd.DataFrame({
        "store_id": strip(d["store_id"]),
        "store_name": strip(d["store_name"]),
        "zone": zone.where(zone.isin(ZONES), UNKNOWN),
        "area": strip(d["area"]),
        "pincode": _or_none(pincode, pincode.str.match(r"^\d{6}$")),
        "open_date": parse_date(d["open_date"]).dt.strftime("%Y-%m-%d"),
        "manager_name": strip(d["manager_name"]),
        "capacity_orders_per_day": cap.where(cap > 0),
        "dq_flags": d["dq_warnings"],
        "source_row_num": d["_source_row_num"],
    })
    return out.reset_index(drop=True)


def build_fact_orders(scored: pd.DataFrame, dim_c: pd.DataFrame, dim_p: pd.DataFrame, dim_s: pd.DataFrame,
                      etl_run_id: str) -> pd.DataFrame:
    d = scored[scored["dq_critical"] == ""].reset_index(drop=True)
    ts = parse_ts(d["order_ts"])
    status = strip(d["order_status"]).str.upper()
    qty = parse_num(d["quantity"], integer=True)
    price = parse_num(d["unit_price"])
    disc = parse_num(d["discount_pct"])
    amount = parse_num(d["order_amount"])
    minutes = parse_num(d["delivery_minutes"])
    pid = strip(d["product_id"])

    # join to the dimensions: every key must resolve (validation guarantees it; fail loudly if it ever doesn't)
    for col, dim, key in (("customer_id", dim_c, "customer_id"), ("product_id", dim_p, "product_id"),
                          ("store_id", dim_s, "store_id")):
        orphans = ~strip(d[col]).isin(set(dim[key]))
        if orphans.any():
            raise AssertionError(f"{int(orphans.sum())} fact rows have a {col} missing from the dimension")
    cost = pid.map(dim_p.set_index("product_id")["cost_price"].astype(float))

    delivered = status == "DELIVERED"
    net_revenue = amount.where(delivered, 0.0).round(2)
    margin = (amount - qty * cost).round(2).where(delivered, 0.0)
    within = (minutes <= 15).where(delivered)

    fact = pd.DataFrame({
        "order_id": strip(d["order_id"]),
        "order_ts": ts.dt.strftime("%Y-%m-%d %H:%M:%S"),
        "order_date": ts.dt.strftime("%Y-%m-%d"),
        "customer_id": strip(d["customer_id"]),
        "store_id": strip(d["store_id"]),
        "product_id": pid,
        "quantity": qty.astype("Int64"),
        "unit_price": price,
        "discount_pct": disc,
        "order_amount": amount,
        "payment_mode": std_payment_mode(d["payment_mode"]),
        "order_status": status,
        "channel": strip(d["channel"]),
        "delivery_minutes": minutes.astype("Int64"),
        "net_revenue": net_revenue,
        "gross_margin": margin,
        "within_15_min": within,
        "source_file": d["_source_file"],
        "source_row_num": d["_source_row_num"],
        "etl_run_id": etl_run_id,
    })
    if fact["order_id"].duplicated().any():
        raise AssertionError("duplicate order_id reached the fact table")
    return fact


def transform(rt: dict) -> dict:
    as_of = rt["as_of"]
    customers, products, stores, orders = (read_scored(rt, n) for n in ("customers", "products", "stores", "orders"))

    dim_c = build_dim_customer(customers, as_of)
    dim_p = build_dim_product(products, as_of)
    dim_s = build_dim_store(stores, as_of)
    for name, dim, key in (("customer", dim_c, "customer_id"), ("product", dim_p, "product_id"), ("store", dim_s, "store_id")):
        if dim[key].duplicated().any():  # defensive: validation rejects extra copies, this proves it
            raise AssertionError(f"duplicate {key} in dim_{name}")
    fact = build_fact_orders(orders, dim_c, dim_p, dim_s, rt["run_id"])

    out = work_path(rt)
    for name, frame in (("dim_customer", dim_c), ("dim_product", dim_p), ("dim_store", dim_s), ("fact_orders", fact)):
        frame.to_csv(out / f"{name}.csv", index=False)
    stats = {"dim_customer": len(dim_c), "dim_product": len(dim_p), "dim_store": len(dim_s), "fact_orders": len(fact)}
    print(f"[Transform] {stats}")
    return stats
