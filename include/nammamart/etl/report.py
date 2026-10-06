"""
include/nammamart/etl/report.py
-------------------------------
ETL step 7 - REPORT. Builds etl_daily_zone_revenue and etl_category_margin and prints them in the task log
so the answer to "did today's numbers land, and what are they?" is visible without opening a SQL client.
"""

from __future__ import annotations

from nammamart.warehouse import run_sql_file, warehouse


def build_reports(rt: dict) -> dict:
    with warehouse(rt["warehouse"]) as con:
        run_sql_file(con, "etl_report.sql")
        zone = con.execute(
            "SELECT zone, count(DISTINCT order_date) AS days, sum(delivered_orders) AS orders, "
            "round(sum(net_revenue), 2) AS net_revenue FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC"
        ).fetchdf()
        latest = con.execute(
            "SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue "
            "FROM etl_daily_zone_revenue WHERE order_date = (SELECT max(order_date) FROM etl_daily_zone_revenue) "
            "GROUP BY order_date, zone ORDER BY zone").fetchdf()
        category = con.execute("SELECT * FROM etl_category_margin").fetchdf()
        total = con.execute("SELECT round(sum(net_revenue), 2) FROM etl_daily_zone_revenue").fetchone()[0]
    print("[Report] Net revenue by zone (all days loaded)")
    print(zone.to_string(index=False))
    print("\n[Report] Latest day by zone")
    print(latest.to_string(index=False))
    print("\n[Report] Revenue and gross margin by category")
    print(category.to_string(index=False))
    print(f"\n[Report] TOTAL delivered net revenue = {total}")
    return {"total_net_revenue": float(total or 0)}
