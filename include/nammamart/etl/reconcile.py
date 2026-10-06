"""
include/nammamart/etl/reconcile.py
----------------------------------
ETL step 6 - RECONCILE clean orders with the payment gateway (payments.csv).

Writes `etl_payment_reconciliation`: ONE ROW PER MISMATCH, each with a reason and the amount that does not
reconcile. A fully reconciled order/payment pair produces no row. Reasons:

  MISSING_PAYMENT              DELIVERED/RETURNED order with no payment row at all
  PAYMENT_FEED_PENDING         same, but the order is newer than the latest payment in the gateway file (timing, not an error)
  AMOUNT_MISMATCH              DELIVERED order, SUCCESS payment, amounts differ
  PAYMENT_FAILED               DELIVERED order whose payment FAILED
  REFUNDED_BUT_DELIVERED       DELIVERED order whose payment was REFUNDED
  RETURNED_NOT_REFUNDED        RETURNED order whose payment is still SUCCESS (refund not issued)
  REFUND_AMOUNT_MISMATCH       RETURNED order, REFUNDED payment, amounts differ
  RETURNED_PAYMENT_FAILED      RETURNED order whose payment FAILED
  PAYMENT_FOR_CANCELLED_ORDER  payment exists for a CANCELLED order
  PAYMENT_MODE_MISMATCH        amounts agree but the payment mode differs from the order's
  PAYMENT_FOR_REJECTED_ORDER   payment for an order that exists in the source but was quarantined
  ORPHAN_PAYMENT               payment whose order_id exists nowhere in the orders source

The gateway is the source of truth for money received, so payments are NOT excluded because they failed a
data-quality check - anomalies must be visible here, not hidden. Only the extra copies of a duplicated
payment_id are dropped.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from nammamart.config import AMOUNT_TOLERANCE
from nammamart.etl import history
from nammamart.etl.transform import read_scored
from nammamart.standardise import parse_num, parse_ts, std_payment_mode, strip
from nammamart.warehouse import run_sql_file, warehouse

COLUMNS = ["order_id", "payment_id", "order_date", "order_status", "order_payment_mode", "payment_mode",
           "payment_status", "order_amount", "amount_paid", "unreconciled_amount", "mismatch_reason",
           "payment_dq_status"]


def build_reconciliation(fact: pd.DataFrame, payments: pd.DataFrame, orders_source: pd.DataFrame, as_of: str,
                         known_ids: set[str] | None = None) -> pd.DataFrame:
    """
    fact           clean orders (etl_fact_orders): order_id, order_ts, order_date, order_status, payment_mode, order_amount
    payments       SCORED payments (all rows, incl. the dq_status / is_duplicate_copy columns)
    orders_source  SCORED orders (to tell 'rejected' from 'never existed')
    known_ids      orders loaded or rejected in earlier runs (etl/history.py)
    """
    tol = AMOUNT_TOLERANCE
    pay = payments[~payments["is_duplicate_copy"]].copy()
    pay["oid"] = strip(pay["order_id"])
    pay["amount"] = parse_num(pay["amount_paid"])
    pay["pstatus"] = strip(pay["payment_status"]).str.upper()
    pay["pmode"] = std_payment_mode(pay["payment_mode"])
    pay["pts"] = parse_ts(pay["payment_ts"])
    as_of_ts = pd.Timestamp(as_of)
    valid_ts = pay.loc[pay["pts"].notna() & (pay["pts"] <= as_of_ts), "pts"]
    high_water = valid_ts.max() if len(valid_ts) else pd.NaT

    o = fact.copy()
    o["order_amount"] = o["order_amount"].astype(float)
    o["order_ts"] = pd.to_datetime(o["order_ts"])
    clean_ids = set(o["order_id"])
    source_ids = (set(strip(orders_source["order_id"])) | (known_ids or set())) - {""}

    # (A) delivered / returned orders with no payment row at all
    a = o[~o["order_id"].isin(set(pay["oid"])) & o["order_status"].isin(["DELIVERED", "RETURNED"])].copy()
    pending = a["order_ts"] > high_water if pd.notna(high_water) else pd.Series(False, index=a.index)
    a_out = pd.DataFrame({
        "order_id": a["order_id"], "payment_id": None, "order_date": a["order_date"],
        "order_status": a["order_status"], "order_payment_mode": a["payment_mode"], "payment_mode": None,
        "payment_status": None, "order_amount": a["order_amount"], "amount_paid": np.nan,
        "mismatch_reason": np.where(pending, "PAYMENT_FEED_PENDING", "MISSING_PAYMENT"),
        "payment_dq_status": None,
    })

    # (B) payments that belong to a clean order
    b = pay.merge(o, left_on="oid", right_on="order_id", how="inner", suffixes=("_p", ""))
    status, pstatus = b["order_status"], b["pstatus"]
    diff_ok = (b["order_amount"] - b["amount"]).abs() <= tol
    rules = [
        (status == "CANCELLED", "PAYMENT_FOR_CANCELLED_ORDER"),
        ((status == "DELIVERED") & (pstatus == "SUCCESS") & diff_ok, "OK"),
        ((status == "DELIVERED") & (pstatus == "SUCCESS"), "AMOUNT_MISMATCH"),
        ((status == "DELIVERED") & (pstatus == "FAILED"), "PAYMENT_FAILED"),
        ((status == "DELIVERED") & (pstatus == "REFUNDED"), "REFUNDED_BUT_DELIVERED"),
        ((status == "RETURNED") & (pstatus == "REFUNDED") & diff_ok, "OK"),
        ((status == "RETURNED") & (pstatus == "REFUNDED"), "REFUND_AMOUNT_MISMATCH"),
        ((status == "RETURNED") & (pstatus == "SUCCESS"), "RETURNED_NOT_REFUNDED"),
        ((status == "RETURNED") & (pstatus == "FAILED"), "RETURNED_PAYMENT_FAILED"),
    ]
    reason = pd.Series(np.select([c for c, _ in rules], [r for _, r in rules], default="UNKNOWN_PAYMENT_STATUS"), index=b.index)
    mode_differs = b["pmode"].isna() | (b["pmode"] != b["payment_mode"])
    reason = reason.where(~((reason == "OK") & mode_differs), "PAYMENT_MODE_MISMATCH")
    b = b.assign(mismatch_reason=reason)
    b = b[b["mismatch_reason"] != "OK"]
    b_out = pd.DataFrame({
        "order_id": b["order_id"], "payment_id": strip(b["payment_id"]), "order_date": b["order_date"],
        "order_status": b["order_status"], "order_payment_mode": b["payment_mode"], "payment_mode": b["pmode"],
        "payment_status": b["pstatus"], "order_amount": b["order_amount"], "amount_paid": b["amount"],
        "mismatch_reason": b["mismatch_reason"], "payment_dq_status": b["dq_status"],
    })

    # (C) payments with no clean order behind them
    c = pay[~pay["oid"].isin(clean_ids)]
    c_out = pd.DataFrame({
        "order_id": c["oid"].where(c["oid"] != "", None), "payment_id": strip(c["payment_id"]), "order_date": pd.NaT,
        "order_status": None, "order_payment_mode": None, "payment_mode": c["pmode"], "payment_status": c["pstatus"],
        "order_amount": np.nan, "amount_paid": c["amount"],
        "mismatch_reason": np.where(c["oid"].isin(source_ids), "PAYMENT_FOR_REJECTED_ORDER", "ORPHAN_PAYMENT"),
        "payment_dq_status": c["dq_status"],
    })

    out = pd.concat([a_out, b_out, c_out], ignore_index=True)
    out["unreconciled_amount"] = _unreconciled(out)
    return out[COLUMNS].sort_values(["mismatch_reason", "order_id", "payment_id"], na_position="last").reset_index(drop=True)


def _unreconciled(df: pd.DataFrame) -> pd.Series:
    r, oa, ap = df["mismatch_reason"], df["order_amount"], df["amount_paid"]
    diff = (oa - ap).abs()
    return pd.Series(np.select(
        [r.isin(["MISSING_PAYMENT", "PAYMENT_FEED_PENDING", "PAYMENT_FAILED", "REFUNDED_BUT_DELIVERED",
                 "RETURNED_PAYMENT_FAILED"]),
         r.isin(["AMOUNT_MISMATCH", "REFUND_AMOUNT_MISMATCH"]),
         r == "RETURNED_NOT_REFUNDED",
         r.isin(["PAYMENT_FOR_CANCELLED_ORDER", "ORPHAN_PAYMENT", "PAYMENT_FOR_REJECTED_ORDER"]),
         r == "PAYMENT_MODE_MISMATCH"],
        [oa, diff, ap, ap.abs(), 0.0], default=oa), index=df.index).round(2)


def reconcile_payments(rt: dict) -> dict:
    scored_payments = read_scored(rt, "payments")
    scored_orders = read_scored(rt, "orders")
    known_ids, _ = history.known_orders(rt)
    with warehouse(rt["warehouse"]) as con:
        fact = con.execute(
            "SELECT order_id, order_ts, CAST(order_date AS VARCHAR) AS order_date, order_status, payment_mode, "
            "CAST(order_amount AS DOUBLE) AS order_amount FROM etl_fact_orders"
        ).fetchdf()
        recon = build_reconciliation(fact, scored_payments, scored_orders, rt["as_of"], known_ids=known_ids)
        recon.insert(0, "run_id", rt["run_id"])
        con.execute("BEGIN TRANSACTION")
        con.execute("DELETE FROM etl_payment_reconciliation")  # current-state table: one row per open mismatch
        con.register("recon_rows", recon)
        con.execute(
            "INSERT INTO etl_payment_reconciliation SELECT run_id, order_id, payment_id, "
            "TRY_CAST(order_date AS DATE), order_status, order_payment_mode, payment_mode, payment_status, "
            "CAST(order_amount AS DECIMAL(12,2)), CAST(amount_paid AS DECIMAL(12,2)), "
            "CAST(unreconciled_amount AS DECIMAL(12,2)), mismatch_reason, payment_dq_status, "
            "current_timestamp::TIMESTAMP FROM recon_rows")
        con.execute("COMMIT")
        summary = con.execute(
            "SELECT mismatch_reason, count(*) AS rows, sum(unreconciled_amount) AS unreconciled "
            "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC").fetchdf()
    total = float(recon.loc[recon["mismatch_reason"] != "PAYMENT_FEED_PENDING", "unreconciled_amount"].sum())
    print(f"[Reconcile] {len(recon)} mismatches; unreconciled amount (excl. feed-pending) = {total:,.2f}")
    print(summary.to_string(index=False))
    return {"mismatches": len(recon), "unreconciled_amount": round(total, 2)}
