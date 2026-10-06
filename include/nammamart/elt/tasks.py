"""
include/nammamart/elt/tasks.py
------------------------------
The ELT DAG's task bodies as plain functions (the DAG file only wires them). Every transformation is a .sql file
in include/sql/ - Python only opens a connection, binds parameters and reports counts.
"""

from __future__ import annotations

import pandas as pd

from nammamart.config import ELT_DAG_ID
from nammamart.elt import extract, load_raw
from nammamart.warehouse import run_sql_file, warehouse

STAGING_TABLES = {
    "customers": "stg_customers",
    "products": "stg_products",
    "stores": "stg_stores",
    "payments": "stg_payments",
}

MARTS = {
    "daily_store_kpis": "mart_daily_store_kpis.sql",
    "category_margin": "mart_category_margin.sql",
    "delivery_sla": "mart_delivery_sla.sql",
    "payment_reconciliation": "mart_payment_reconciliation.sql",
    "top_customers": "mart_top_customers.sql",
    "revenue_by_payment_mode": "mart_revenue_by_payment_mode.sql",
    "dq_rejections": "mart_dq_rejections.sql",
    "store_capacity": "mart_store_capacity.sql",          # bonus: daily orders vs store capacity
}


def load_raw_table(kind: str, meta: dict, rt: dict) -> dict:
    return load_raw.load_raw_file(f"raw_{kind}", meta, rt)


def load_raw_increments(meta: dict, rt: dict) -> list[dict]:
    """Append next-day increment files to raw_orders (new batches; identical files are skipped)."""
    return [load_raw.load_raw_file("raw_orders", inc, rt) for inc in meta.get("increments", [])]


def _status_counts(con, table: str) -> dict:
    rows = dict(con.execute(f"SELECT dq_status, count(*) FROM {table} GROUP BY 1").fetchall())
    return {"rows": sum(rows.values()), "pass": rows.get("PASS", 0), "warn": rows.get("WARN", 0), "fail": rows.get("FAIL", 0)}


def build_staging(name: str, rt: dict) -> dict:
    """Staging for customers / products / stores / payments: one SQL file each, full rebuild from raw."""
    table = STAGING_TABLES[name]
    with warehouse(rt["warehouse"]) as con:
        load_raw.prepare_warehouse(con)
        run_sql_file(con, f"{table}.sql", {"as_of": rt["as_of"]})
        stats = _status_counts(con, table)
    print(f"[Staging] {table}: {stats['rows']} rows -> {stats['pass']} PASS, {stats['warn']} WARN, {stats['fail']} FAIL")
    return {"table": table, **stats}


def build_staging_orders(rt: dict) -> dict:
    """Staging for orders: evaluate every current raw row, then MERGE the survivors into stg_orders on order_id."""
    with warehouse(rt["warehouse"]) as con:
        load_raw.prepare_warehouse(con)
        run_sql_file(con, "stg_orders_batch.sql", {"as_of": rt["as_of"]})
        batch = _status_counts(con, "stg_orders_batch")
        con.execute("BEGIN TRANSACTION")
        try:
            actions = run_sql_file(con, "stg_orders_merge.sql", {"run_id": rt["run_id"]})
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        inserted = sum(1 for (a,) in actions if a == "INSERT")
        updated = sum(1 for (a,) in actions if a == "UPDATE")
        total = con.execute("SELECT count(*) FROM stg_orders").fetchone()[0]
        merged_src = con.execute("SELECT count(*) FROM stg_orders_batch WHERE NOT is_duplicate_copy AND order_id <> ''").fetchone()[0]
    stats = {"table": "stg_orders", **batch, "merge_inserted": inserted, "merge_updated": updated,
             "merge_unchanged": merged_src - inserted - updated, "stg_orders_total": total}
    print(f"[Staging] stg_orders_batch: {batch['rows']} current rows -> {batch['pass']} PASS, {batch['warn']} WARN, {batch['fail']} FAIL")
    print(f"[Staging] MERGE into stg_orders: +{inserted} inserted, ~{updated} updated, {stats['merge_unchanged']} unchanged -> {total} rows")
    return stats


def record_dq_results(rt: dict, orders_rows: int, dag_id: str = ELT_DAG_ID) -> dict:
    with warehouse(rt["warehouse"]) as con:
        load_raw.prepare_warehouse(con)
        run_sql_file(con, "elt_dq_results.sql",
                     {"run_id": rt["run_id"], "dag_id": dag_id, "as_of": rt["as_of"], "orders_rows": int(orders_rows)})
        rows = con.execute(
            "SELECT count(*), count(*) FILTER (WHERE rows_failed > 0) FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [rt["run_id"], dag_id]).fetchone()
    print(f"[DQ] {rows[0]} check results written to dq_results ({rows[1]} with failures)")
    return {"checks": rows[0], "checks_with_failures": rows[1]}


def build_mart(name: str, rt: dict) -> dict:
    with warehouse(rt["warehouse"]) as con:
        load_raw.prepare_warehouse(con)
        run_sql_file(con, "elt_views.sql")
        run_sql_file(con, MARTS[name], {"run_id": rt["run_id"], "as_of": rt["as_of"]})
        table = f"mart_{name}"
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    print(f"[Mart] {table}: {n} rows")
    return {"table": table, "rows": int(n)}


def elt_report(rt: dict) -> dict:
    """Print the answers to the business questions straight from the marts (visible in the task log)."""
    with warehouse(rt["warehouse"]) as con:
        zone = con.execute("SELECT zone, round(sum(net_revenue), 2) AS net_revenue, sum(delivered_orders) AS orders "
                           "FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC").fetchdf()
        cat = con.execute("SELECT * FROM mart_category_margin").fetchdf()
        sla = con.execute("SELECT store_id, store_name, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla").fetchdf()
        mode = con.execute("SELECT * FROM mart_revenue_by_payment_mode").fetchdf()
        recon = con.execute("SELECT mismatch_reason, count(*) AS rows, sum(unreconciled_amount) AS unreconciled "
                            "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 2 DESC").fetchdf()
        top = con.execute("SELECT * FROM mart_top_customers WHERE spend_rank <= 10").fetchdf()
        rej = con.execute("SELECT source_file, failure_reason, rows_rejected FROM mart_dq_rejections "
                          "WHERE run_id = ? ORDER BY 1, 3 DESC", [rt["run_id"]]).fetchdf()
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis").fetchone()[0]
    for title, frame in (("Q1 net revenue by zone", zone), ("Q2 category margin", cat), ("Q3 delivery SLA per store", sla),
                         ("Q4 revenue by payment mode", mode), ("Q4 reconciliation mismatches", recon),
                         ("Q5 top 10 customers (masked)", top), ("Q6 rows rejected this run", rej)):
        print(f"\n[Report] {title}\n{frame.to_string(index=False)}")
    print(f"\n[Report] TOTAL delivered net revenue = {total}")
    return {"total_net_revenue": float(total or 0)}


def count_rejected_rows(rt: dict) -> int:
    """Rows currently flagged FAIL across the five staging tables (the ELT equivalent of 'rows quarantined')."""
    with warehouse(rt["warehouse"]) as con:
        return int(con.execute(
            "SELECT (SELECT count(*) FROM stg_orders_batch WHERE dq_status = 'FAIL') "
            "+ (SELECT count(*) FROM stg_payments WHERE dq_status = 'FAIL') "
            "+ (SELECT count(*) FROM stg_customers WHERE dq_status = 'FAIL') "
            "+ (SELECT count(*) FROM stg_products WHERE dq_status = 'FAIL') "
            "+ (SELECT count(*) FROM stg_stores WHERE dq_status = 'FAIL')").fetchone()[0])


def run_elt_locally(rt: dict, dag_id: str = ELT_DAG_ID) -> dict:
    """The whole ELT, in DAG order, in one process (used by tests and the experiment harness)."""
    meta = {k: extract.extract_source(k, rt) for k in ("orders", "payments", "customers", "products", "stores")}
    raw = {k: load_raw_table(k, m, rt) for k, m in meta.items()}
    raw["orders_increments"] = load_raw_increments(meta["orders"], rt)
    stg = {k: build_staging(k, rt) for k in ("customers", "products", "stores")}
    stg["orders"] = build_staging_orders(rt)
    stg["payments"] = build_staging("payments", rt)
    dq = record_dq_results(rt, meta["orders"]["rows"], dag_id)
    marts = {n: build_mart(n, rt) for n in MARTS}
    rep = elt_report(rt)
    return {"extract": meta, "raw": raw, "staging": stg, "dq": dq, "marts": marts, "report": rep,
            "rows_rejected": count_rejected_rows(rt)}
