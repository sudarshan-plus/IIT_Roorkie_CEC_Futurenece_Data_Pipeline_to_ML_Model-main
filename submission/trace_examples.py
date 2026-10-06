"""
submission/trace_examples.py
----------------------------
Part E evidence, produced from a real warehouse:

  3. LINEAGE  - one number on mart_daily_store_kpis traced back to the exact source file and rows (ELT and ETL)
  8. AUDIT    - "pick any rejected order: show it exactly as received, the rule it broke, and prove it is not in revenue"
  9. CHANGE   - an order DELIVERED in September and RETURNED by the 2026-10-01 increment: what each pipeline shows

    python submission/trace_examples.py --warehouse <warehouse_sudarshan.duckdb> [--quarantine-dir <dir>]
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path

import duckdb


def table(con, sql, params=None) -> str:
    cursor = con.execute(sql, params or [])
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    out = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    out += ["| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows]
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--quarantine-dir", default="")
    args = parser.parse_args()
    con = duckdb.connect(args.warehouse, read_only=True)

    day, store = "2026-09-01", "S05"
    print(f"## 3. Lineage - mart_daily_store_kpis, store {store}, {day}\n")
    print("**The number (ELT mart):**\n")
    print(table(con, "SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis "
                     "WHERE order_date = ? AND store_id = ?", [day, store]))
    print("\n**ELT: the staging rows behind it** (`stg_orders` carries the raw file, batch and line number):\n")
    print(table(con, "SELECT order_id, order_status, order_amount, dq_status, _source_file, _batch_id, _source_row_num "
                     "FROM stg_orders WHERE store_id = ? AND CAST(order_ts AS DATE) = ? AND order_status = 'DELIVERED' "
                     "AND dq_status <> 'FAIL' ORDER BY order_id", [store, day]))
    print("\n**ELT: the same rows in `raw_orders`, exactly as received** (join on file + line number):\n")
    print(table(con, """SELECT r.order_id, r.order_ts, r.store_id, r.quantity, r.unit_price, r.discount_pct, r.order_amount,
                               r.order_status, r._source_file, r._source_row_num
                        FROM raw_orders r JOIN stg_orders s
                          ON s._source_file = r._source_file AND s._source_row_num = r._source_row_num AND s._batch_id = r._batch_id
                        WHERE s.store_id = ? AND CAST(s.order_ts AS DATE) = ? AND s.order_status = 'DELIVERED' AND s.dq_status <> 'FAIL'
                        ORDER BY r.order_id""", [store, day]))
    print("\n**ETL: the same number** (`etl_daily_zone_revenue` -> `etl_fact_orders`, which carries source file and row):\n")
    print(table(con, "SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue "
                     "WHERE order_date = ? AND store_id = ?", [day, store]))
    print()
    print(table(con, "SELECT order_id, order_status, order_amount, net_revenue, source_file, source_row_num, etl_run_id "
                     "FROM etl_fact_orders WHERE store_id = ? AND order_date = ? AND order_status = 'DELIVERED' ORDER BY order_id",
                [store, day]))

    rejected = "ORD001296"
    print(f"\n\n## 8. Audit - rejected order {rejected}\n")
    print("**ELT - as received (`raw_orders`):**\n")
    print(table(con, "SELECT * EXCLUDE (_loaded_at) FROM raw_orders WHERE order_id = ?", [rejected]))
    print("\n**ELT - the rule it broke (`stg_orders`):**\n")
    print(table(con, "SELECT order_id, order_status, quantity, order_amount, dq_status, failure_reason FROM stg_orders WHERE order_id = ?",
                [rejected]))
    print("\n**ELT - proof it is not in the CFO's revenue** (`v_orders_clean` feeds every mart):\n")
    print(table(con, """SELECT (SELECT count(*) FROM v_orders_clean WHERE order_id = ?) AS rows_in_marts_source,
                               (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis) AS mart_total_revenue,
                               (SELECT round(sum(order_amount), 2) FROM stg_orders WHERE dq_status <> 'FAIL' AND order_status = 'DELIVERED') AS clean_delivered_total""",
                [rejected]))
    print("\n**ETL - the quarantine row and its reason (`etl_quarantine`):**\n")
    print(table(con, "SELECT run_id, source_file, source_row_num, row_key, failure_reason FROM etl_quarantine "
                     "WHERE row_key = ? ORDER BY quarantined_at DESC LIMIT 1", [rejected]))
    print("\n**ETL - proof it is not in the finance table:**\n")
    print(table(con, "SELECT (SELECT count(*) FROM etl_fact_orders WHERE order_id = ?) AS rows_in_etl_fact_orders", [rejected]))
    qdir = Path(args.quarantine_dir) if args.quarantine_dir else None
    if qdir and qdir.exists():
        done = False
        for path in sorted(qdir.glob("orders_failed_*.csv")):
            with path.open(newline="") as handle:
                for row in csv.DictReader(handle):
                    if row["order_id"] == rejected:
                        print(f"\n**ETL - the full row as received, in the quarantine file `{path.name}`:**\n")
                        print("```\n" + ", ".join(f"{k}={v}" for k, v in row.items()) + "\n```")
                        done = True
                        break
            if done:
                break

    changed = "ORD000337"
    print(f"\n\n## 9. Change history - order {changed} (DELIVERED in September, RETURNED in the 2026-10-01 increment)\n")
    print("**ELT raw keeps BOTH versions:**\n")
    print(table(con, "SELECT order_id, order_status, order_amount, _source_file, _batch_id FROM raw_orders WHERE order_id = ? ORDER BY _batch_id", [changed]))
    print("\n**ELT staging holds the current version, history in `stg_order_changes`:**\n")
    print(table(con, "SELECT order_id, order_status, dq_status FROM stg_orders WHERE order_id = ?", [changed]))
    print()
    print(table(con, "SELECT order_id, old_status, new_status, old_order_amount, source_file FROM stg_order_changes WHERE order_id = ?", [changed]))
    print("\n**ETL fact holds the current version, history in `etl_order_changes`:**\n")
    print(table(con, "SELECT order_id, order_status, net_revenue FROM etl_fact_orders WHERE order_id = ?", [changed]))
    print()
    print(table(con, "SELECT order_id, old_status, new_status, old_net_revenue, new_net_revenue, source_file FROM etl_order_changes WHERE order_id = ?", [changed]))
    print("\n**September revenue - restated vs as-originally-reported (both views from the history tables):**\n")
    print(table(con, """SELECT
        (SELECT round(sum(net_revenue), 2) FROM mart_daily_store_kpis WHERE order_date < '2026-10-01') AS september_restated_elt,
        (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders WHERE order_date < '2026-10-01') AS september_restated_etl,
        (SELECT round(sum(old_net_revenue - new_net_revenue), 2) FROM etl_order_changes) AS reversed_by_returns,
        (SELECT round(sum(net_revenue), 2) FROM etl_fact_orders WHERE order_date < '2026-10-01')
          + (SELECT round(sum(old_net_revenue - new_net_revenue), 2) FROM etl_order_changes) AS september_as_originally_reported"""))


if __name__ == "__main__":
    main()
