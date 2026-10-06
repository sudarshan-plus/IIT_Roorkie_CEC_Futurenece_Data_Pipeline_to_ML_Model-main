"""
include/nammamart/etl/load.py
-----------------------------
ETL step 5 - LOAD the star schema into DuckDB, idempotently, in ONE transaction.

  dims   MERGE (upsert) - same data in, same rows out
  fact   MERGE on order_id with a row hash:
           new order_id            -> INSERT
           same id, changed values -> UPDATE (and the old values are kept in etl_order_changes)
           same id, same values    -> untouched (re-runs never double-count revenue)
         An order whose latest version now fails a Critical check is removed from the fact table.
"""

from __future__ import annotations

import pandas as pd

from nammamart.etl.extract import work_path
from nammamart.etl.transform import read_scored
from nammamart.warehouse import ensure_core_tables, run_sql_file, warehouse


def _read(rt: dict, name: str) -> pd.DataFrame:
    return pd.read_csv(work_path(rt) / f"{name}.csv", dtype=str, keep_default_na=False)


def load_warehouse(rt: dict) -> dict:
    dim_c, dim_p, dim_s, fact = (_read(rt, n) for n in ("dim_customer", "dim_product", "dim_store", "fact_orders"))
    scored_orders = read_scored(rt, "orders")
    # An order is "rejected" when its SURVIVING row failed. The extra copy of a duplicated order_id is also
    # quarantined, but it shares its id with the clean survivor and must not evict it from the fact table.
    survivors_failed = (scored_orders["dq_critical"] != "") & ~scored_orders["is_duplicate_copy"]
    rejected = scored_orders.loc[survivors_failed, ["order_id"]].assign(order_id=lambda d: d["order_id"].str.strip())

    with warehouse(rt["warehouse"]) as con:
        ensure_core_tables(con)
        run_sql_file(con, "etl_tables.sql")
        con.register("batch_customer", dim_c)
        con.register("batch_product", dim_p)
        con.register("batch_store", dim_s)
        con.register("batch_fact", fact)
        con.register("rejected_orders", rejected)

        before = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        con.execute("BEGIN TRANSACTION")
        try:
            run_sql_file(con, "etl_load_dims.sql", {"run_id": rt["run_id"]})
            actions = run_sql_file(con, "etl_load_fact.sql", {"run_id": rt["run_id"]})
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        inserted = sum(1 for (a,) in actions if a == "INSERT")
        updated = sum(1 for (a,) in actions if a == "UPDATE")
        total = con.execute("SELECT count(*) FROM etl_fact_orders").fetchone()[0]
        removed = before + inserted - total

    stats = {
        "fact_inserted": inserted,
        "fact_updated": updated,
        "fact_unchanged": len(fact) - inserted - updated,
        "fact_removed": removed,
        "fact_total": total,
        "dim_customer": len(dim_c), "dim_product": len(dim_p), "dim_store": len(dim_s),
        "rows_loaded": len(fact) + len(dim_c) + len(dim_p) + len(dim_s),
    }
    print(f"[Load] etl_fact_orders: +{inserted} inserted, ~{updated} updated, {stats['fact_unchanged']} unchanged, "
          f"-{removed} removed -> {total} rows | dims: {len(dim_c)} customers, {len(dim_p)} products, {len(dim_s)} stores")
    return stats
