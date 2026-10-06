"""
include/nammamart/etl/validate.py
---------------------------------
ETL step 2 - VALIDATE. Applies the rulebook in `dq_catalog.py` (Python / pandas version).

Each `validate_<file>` takes the raw text rows from Extract and returns a `Scored` object:
  * df       every input row + `dq_critical`, `dq_warnings`, `dq_status` (PASS | WARN | FAIL), `is_duplicate_copy`
  * results  one dq_results row per check (rows_checked / rows_failed / status)
Rows with a Critical failure are split off to quarantine by the caller; Warning rows continue.

The ELT pipeline implements the same rules in SQL (include/sql/stg_*.sql); the two are kept identical by
tests/nammamart/test_parity.py, which compares rows_failed for every single check.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from nammamart import dq_catalog as dq
from nammamart.audit import make_result
from nammamart.config import (
    AMOUNT_TOLERANCE, CHANNELS, FRESHNESS_HOURS, LOYALTY_TIERS, MAX_DELIVERY_MINUTES, ORDER_STATUSES,
    PAYMENT_STATUSES, PRICE_DEVIATION_FACTOR, TYPICAL_DELIVERY_RANGE, VOLUME_DROP_THRESHOLD, ZONES,
)
from nammamart.standardise import (
    EMAIL_REGEX, capitalize, join_reasons, parse_date, parse_num, parse_ts, std_payment_mode, strip,
)


@dataclass
class Scored:
    table: str
    df: pd.DataFrame
    results: list[dict]
    batch_results: list[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    @property
    def passed(self) -> pd.DataFrame:
        return self.df[self.df["dq_critical"] == ""]

    @property
    def failed(self) -> pd.DataFrame:
        return self.df[self.df["dq_critical"] != ""]


# ------------------------------------------------------------------------------------ shared plumbing
def _finalise(table: str, df: pd.DataFrame, masks: dict[str, pd.Series]) -> Scored:
    checks = dq.checks_for(table)
    names = [c.name for c in checks]
    problems = set(names) ^ set(masks)
    if problems:
        raise AssertionError(f"{table}: checks implemented != catalogue: {sorted(problems)}")
    masks = {k: v.fillna(False).astype(bool) for k, v in masks.items()}
    crit = [c.name for c in checks if c.severity == dq.CRITICAL]
    warn = [c.name for c in checks if c.severity == dq.WARNING]
    out = df.copy()
    out["dq_critical"] = join_reasons(masks, crit, df.index)
    out["dq_warnings"] = join_reasons(masks, warn, df.index)
    out["dq_status"] = np.where(out["dq_critical"] != "", "FAIL", np.where(out["dq_warnings"] != "", "WARN", "PASS"))
    results = [make_result(c, len(df), int(masks[c.name].sum())) for c in checks]
    return Scored(table=table, df=out, results=results)


def _duplicate_flags(keys: pd.Series, order_by: list[pd.Series] | None = None) -> pd.Series:
    """
    True for every EXTRA copy of a key (the survivor is the first row after `order_by`, default file order).
    Blank keys never count as duplicates of each other (they fail the NOT NULL check instead).
    """
    frame = pd.DataFrame({"k": keys})
    for i, s in enumerate(order_by or []):
        frame[f"o{i}"] = s
    frame["row"] = np.arange(len(frame))
    sort_cols = [c for c in frame.columns if c.startswith("o")] + ["row"]
    ordered = frame.sort_values(sort_cols, kind="stable")
    ordered = ordered[ordered["k"] != ""]
    rank = ordered.groupby("k").cumcount()
    dup = pd.Series(False, index=keys.index)
    dup.loc[ordered.index[rank.to_numpy() > 0]] = True
    return dup


def _blank(s: pd.Series) -> pd.Series:
    return strip(s) == ""


# ------------------------------------------------------------------------------------ customers
def validate_customers(raw: pd.DataFrame, as_of: str) -> Scored:
    cid = strip(raw["customer_id"])
    email = strip(raw["email"])
    tier_raw = raw["loyalty_tier"].fillna("")
    tier = capitalize(tier_raw)
    signup = parse_date(raw["signup_date"])
    as_of_date = pd.Timestamp(as_of).normalize()

    masks = {
        "CUS_CUSTOMER_ID_NOT_NULL": cid == "",
        "CUS_CITY_NOT_NULL": _blank(raw["city"]),
        "CUS_CUSTOMER_ID_UNIQUE": _duplicate_flags(cid),
        "CUS_EMAIL_FORMAT": ~email.str.match(EMAIL_REGEX),
        "CUS_PHONE_10_DIGITS": ~strip(raw["phone"]).str.match(r"^\d{10}$"),
        "CUS_PINCODE_6_DIGITS": ~strip(raw["pincode"]).str.match(r"^\d{6}$"),
        "CUS_IS_ACTIVE_VALID": ~strip(raw["is_active"]).str.upper().isin(["Y", "N"]),
        "CUS_SIGNUP_DATE_VALID": signup.isna(),
        "CUS_LOYALTY_TIER_CASING": tier.isin(LOYALTY_TIERS) & (tier_raw != tier),
        "CUS_LOYALTY_TIER_VALID": ~tier.isin(LOYALTY_TIERS),
        "CUS_SIGNUP_DATE_NOT_FUTURE": signup.notna() & (signup > as_of_date),
    }
    scored = _finalise("customers", raw, masks)
    scored.df["is_duplicate_copy"] = masks["CUS_CUSTOMER_ID_UNIQUE"].to_numpy()
    return scored


# ------------------------------------------------------------------------------------ products
def validate_products(raw: pd.DataFrame, as_of: str) -> Scored:
    pid = strip(raw["product_id"])
    mrp = parse_num(raw["mrp"])
    cost = parse_num(raw["cost_price"])
    launch = parse_date(raw["launch_date"])
    is_old = strip(raw["product_name"]).str.contains(r"\(old\)", case=False, regex=True)
    as_of_date = pd.Timestamp(as_of).normalize()

    masks = {
        "PRD_PRODUCT_ID_NOT_NULL": pid == "",
        "PRD_CATEGORY_NOT_NULL": _blank(raw["category"]),
        # the survivor of a duplicated product_id is the current row, not the '(old)' one
        "PRD_PRODUCT_ID_UNIQUE": _duplicate_flags(pid, [is_old.astype(int)]),
        "PRD_LAUNCH_DATE_VALID": launch.isna(),
        "PRD_MRP_POSITIVE": ~(mrp > 0),
        "PRD_COST_PRICE_POSITIVE": ~(cost > 0),
        "PRD_COST_LE_MRP": (mrp > 0) & (cost > 0) & (cost > mrp),
        "PRD_LAUNCH_DATE_NOT_FUTURE": launch.notna() & (launch > as_of_date),
    }
    scored = _finalise("products", raw, masks)
    scored.df["is_duplicate_copy"] = masks["PRD_PRODUCT_ID_UNIQUE"].to_numpy()
    return scored


# ------------------------------------------------------------------------------------ stores
def validate_stores(raw: pd.DataFrame, as_of: str) -> Scored:
    sid = strip(raw["store_id"])
    zone = capitalize(raw["zone"])
    cap = parse_num(raw["capacity_orders_per_day"])
    opened = parse_date(raw["open_date"])
    as_of_date = pd.Timestamp(as_of).normalize()

    masks = {
        "STO_STORE_ID_NOT_NULL": sid == "",
        "STO_ZONE_NOT_NULL": zone == "",
        "STO_STORE_ID_UNIQUE": _duplicate_flags(sid),
        "STO_ZONE_VALID": (zone != "") & ~zone.isin(ZONES),
        "STO_PINCODE_6_DIGITS": ~strip(raw["pincode"]).str.match(r"^\d{6}$"),
        "STO_CAPACITY_POSITIVE": ~(cap > 0),
        "STO_OPEN_DATE_NOT_FUTURE": opened.notna() & (opened > as_of_date),
    }
    scored = _finalise("stores", raw, masks)
    scored.df["is_duplicate_copy"] = masks["STO_STORE_ID_UNIQUE"].to_numpy()
    return scored


# ------------------------------------------------------------------------------------ orders
def validate_orders(
    raw: pd.DataFrame,
    customers: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
    as_of: str,
    volume_baseline: int | None = None,
) -> Scored:
    """
    raw        base orders + any increment files, with `_file_seq` (0 = orders.csv, 1.. = increments)
    customers / products / stores   the SCORED dimension frames (keys are checked against all of them)
    """
    raw = raw.copy()
    raw["_file_seq"] = raw["_file_seq"].astype(int)
    oid_all = strip(raw["order_id"])

    # A later file supersedes an earlier version of the same order (this is how corrections arrive).
    latest_seq = raw.groupby(oid_all)["_file_seq"].transform("max")
    superseded = (oid_all != "") & (raw["_file_seq"] < latest_seq)
    base_rows = int((raw["_file_seq"] == 0).sum())
    cur = raw[~superseded].reset_index(drop=True)

    oid = strip(cur["order_id"])
    cid = strip(cur["customer_id"])
    pid = strip(cur["product_id"])
    sid = strip(cur["store_id"])
    ts_text = strip(cur["order_ts"])
    ts = parse_ts(cur["order_ts"])
    status_raw = cur["order_status"].fillna("")
    status = status_raw.str.strip().str.upper()
    mode_raw = cur["payment_mode"].fillna("")
    mode = std_payment_mode(cur["payment_mode"])
    qty = parse_num(cur["quantity"], integer=True)
    price = parse_num(cur["unit_price"])
    disc = parse_num(cur["discount_pct"])
    amount = parse_num(cur["order_amount"])
    minutes = parse_num(cur["delivery_minutes"])
    minutes_blank = _blank(cur["delivery_minutes"])
    as_of_ts = pd.Timestamp(as_of)

    cust_keys = set(strip(customers["customer_id"])) - {""}
    prod_keys = set(strip(products["product_id"])) - {""}
    store_keys = set(strip(stores["store_id"])) - {""}
    surv = products[~products["is_duplicate_copy"]]
    surv_mrp = parse_num(surv["mrp"])
    # only a VALID catalogue price is a fair yardstick (a bad MRP is already reported by PRD_MRP_POSITIVE)
    mrp_by_product = pd.Series(surv_mrp.where(surv_mrp > 0).to_numpy(), index=strip(surv["product_id"]).to_numpy())
    mrp_by_product = mrp_by_product[~mrp_by_product.index.duplicated()]
    mrp = pid.map(mrp_by_product)

    formula = (qty * price * (1 - disc / 100)).round(2)
    numbers_ok = qty.notna() & price.notna() & disc.notna() & amount.notna() & (minutes.notna() | minutes_blank)

    dup_key = oid.where(oid != "", "#" + pd.Series(np.arange(len(cur)), index=cur.index).astype(str))
    is_dup = _duplicate_flags(dup_key, [cur["_file_seq"]])

    masks = {
        "ORD_ORDER_ID_NOT_NULL": oid == "",
        "ORD_CUSTOMER_ID_NOT_NULL": cid == "",
        "ORD_ORDER_TS_NOT_NULL": ts_text == "",
        "ORD_DELIVERED_HAS_MINUTES": (status == "DELIVERED") & minutes_blank,
        "ORD_ORDER_ID_UNIQUE": is_dup,
        "ORD_STATUS_VALID": ~status.isin(ORDER_STATUSES),
        "ORD_STATUS_CASING": status.isin(ORDER_STATUSES) & (status_raw != status),
        "ORD_PAYMENT_MODE_VALID": mode.isna(),
        "ORD_PAYMENT_MODE_STANDARDISED": mode.notna() & (mode_raw != mode),
        "ORD_ORDER_TS_VALID": (ts_text != "") & ts.isna(),
        "ORD_NUMBERS_VALID": ~numbers_ok,
        "ORD_DISCOUNT_RANGE": disc.notna() & ((disc < 0) | (disc > 100)),
        "ORD_CHANNEL_VALID": ~strip(cur["channel"]).isin(CHANNELS),
        "ORD_QUANTITY_POSITIVE": qty.notna() & (qty <= 0),
        "ORD_UNIT_PRICE_POSITIVE": price.notna() & (price <= 0),
        "ORD_AMOUNT_MATCHES_FORMULA": qty.notna() & price.notna() & disc.notna() & amount.notna()
        & ((formula - amount).abs() > AMOUNT_TOLERANCE),
        "ORD_DELIVERY_MINUTES_MAX": minutes.notna() & (minutes > MAX_DELIVERY_MINUTES),
        "ORD_DELIVERY_MINUTES_TYPICAL": minutes.notna() & (minutes <= MAX_DELIVERY_MINUTES)
        & ((minutes < TYPICAL_DELIVERY_RANGE[0]) | (minutes > TYPICAL_DELIVERY_RANGE[1])),
        "ORD_PRICE_MATCHES_CATALOGUE": price.notna() & mrp.notna() & ((price - mrp).abs() > AMOUNT_TOLERANCE),
        "ORD_PRICE_FAR_FROM_CATALOGUE": price.notna() & mrp.notna()
        & ((price > mrp * PRICE_DEVIATION_FACTOR) | (price * PRICE_DEVIATION_FACTOR < mrp)),
        "ORD_CUSTOMER_EXISTS": (cid != "") & ~cid.isin(cust_keys),
        "ORD_PRODUCT_EXISTS": ~pid.isin(prod_keys),
        "ORD_STORE_EXISTS": ~sid.isin(store_keys),
        "ORD_ORDER_TS_NOT_FUTURE": ts.notna() & (ts > as_of_ts),
    }
    scored = _finalise("orders", cur, masks)
    scored.df["is_duplicate_copy"] = is_dup.to_numpy()
    scored.stats = {"rows_superseded": int(superseded.sum()), "rows_in_files": int(len(raw)), "base_rows": base_rows}

    # ---- batch-level checks (one result row each)
    ok = scored.df["dq_critical"] == ""
    ok_ts = ts[ok.to_numpy() & ts.notna().to_numpy()]
    ok_ts = ok_ts[ok_ts <= as_of_ts]
    stale = ok_ts.empty or (as_of_ts - ok_ts.max()) > pd.Timedelta(hours=FRESHNESS_HOURS)

    dropped = volume_baseline is not None and volume_baseline > 0 and base_rows < volume_baseline * (1 - VOLUME_DROP_THRESHOLD)

    store_frame = pd.DataFrame({"sid": sid, "bad": (scored.df["dq_critical"] != "").to_numpy()})
    per_store = store_frame.groupby("sid")["bad"].agg(["size", "mean"])
    per_store = per_store[per_store["size"] >= 20]
    scored.batch_results = [
        make_result(dq.BY_NAME["BATCH_ORDERS_VOLUME"], base_rows, max(volume_baseline - base_rows, 0) if dropped else 0),
        make_result(dq.BY_NAME["BATCH_ORDERS_FRESH"], 1, 1 if stale else 0),
        make_result(dq.BY_NAME["BATCH_STORE_REJECT_RATE"], len(per_store), int((per_store["mean"] > 0.25).sum())),
    ]
    return scored


# ------------------------------------------------------------------------------------ payments
def validate_payments(raw: pd.DataFrame, orders: pd.DataFrame, as_of: str,
                      known_ids: set[str] | None = None, known_amounts: pd.Series | None = None) -> Scored:
    """
    `orders` is the SCORED orders frame (used for the order-exists and amount checks). `known_ids` / `known_amounts`
    add orders seen in earlier runs (etl/history.py) so a partial orders file does not make valid payments orphans.
    """
    pay_id = strip(raw["payment_id"])
    oid = strip(raw["order_id"])
    status = strip(raw["payment_status"]).str.upper()
    mode_raw = raw["payment_mode"].fillna("")
    mode = std_payment_mode(raw["payment_mode"])
    ts = parse_ts(raw["payment_ts"])
    amount = parse_num(raw["amount_paid"])
    as_of_ts = pd.Timestamp(as_of)

    order_ids = (set(strip(orders["order_id"])) | (known_ids or set())) - {""}
    surv = orders[~orders["is_duplicate_copy"]]
    order_amount = pd.Series(parse_num(surv["order_amount"]).to_numpy(), index=strip(surv["order_id"]).to_numpy())
    order_amount = order_amount[~order_amount.index.duplicated()]
    if known_amounts is not None and len(known_amounts):          # orders absent from today's file: use their loaded amount
        order_amount = pd.concat([order_amount, known_amounts[~known_amounts.index.isin(order_amount.index)]])
    expected = oid.map(order_amount)

    masks = {
        "PAY_PAYMENT_ID_NOT_NULL": pay_id == "",
        "PAY_ORDER_ID_NOT_NULL": oid == "",
        "PAY_GATEWAY_REF_PRESENT": _blank(raw["gateway_ref"]) & (mode.fillna("") != "Cash on Delivery"),
        "PAY_PAYMENT_ID_UNIQUE": _duplicate_flags(pay_id),
        "PAY_STATUS_VALID": ~status.isin(PAYMENT_STATUSES),
        "PAY_MODE_VALID": mode.isna(),
        "PAY_MODE_STANDARDISED": mode.notna() & (mode_raw != mode),
        "PAY_TS_VALID": ts.isna(),
        "PAY_AMOUNT_NUMERIC": amount.isna(),
        "PAY_AMOUNT_POSITIVE": amount.notna() & (amount <= 0),
        "PAY_ORDER_EXISTS": (oid != "") & ~oid.isin(order_ids),
        "PAY_AMOUNT_MATCHES_ORDER": (status == "SUCCESS") & amount.notna() & expected.notna()
        & ((amount - expected).abs() > AMOUNT_TOLERANCE),
        "PAY_TS_NOT_FUTURE": ts.notna() & (ts > as_of_ts),
    }
    scored = _finalise("payments", raw, masks)
    scored.df["is_duplicate_copy"] = masks["PAY_PAYMENT_ID_UNIQUE"].to_numpy()
    return scored
