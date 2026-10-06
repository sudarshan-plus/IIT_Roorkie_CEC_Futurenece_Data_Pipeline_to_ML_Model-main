"""
ETL (pandas) and ELT (SQL) implement the same rulebook twice. This proves they agree - check by check, row by row,
and number by number. If these pass, "mart totals match ETL totals" is not a coincidence, it is guaranteed.
"""

from decimal import Decimal

import pytest

from nammamart.config import ELT_DAG_ID, ETL_DAG_ID


def rows(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def norm(value):
    """Make Decimal/float/None comparable."""
    if isinstance(value, (Decimal, float)):
        return round(float(value), 2)
    return value


def normalise(table):
    return sorted(tuple(norm(v) for v in row) for row in table)


# ----------------------------------------------------------------------------------- data quality
def test_dq_results_are_identical_check_by_check(base_run):
    sql = ("SELECT check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results "
           "WHERE dag_id = ? ORDER BY check_name")
    etl = rows(base_run.etl(), sql, [ETL_DAG_ID])
    elt = rows(base_run.elt(), sql, [ELT_DAG_ID])
    assert len(etl) == len(elt) == 66
    assert etl == elt


def test_every_check_in_the_catalogue_has_a_result_every_run(base_run):
    from nammamart.dq_catalog import CHECKS
    names = {r[0] for r in rows(base_run.elt(), "SELECT check_name FROM dq_results WHERE dag_id = ?", [ELT_DAG_ID])}
    assert names == {c.name for c in CHECKS}


def test_row_level_rejections_and_reasons_are_identical(base_run):
    etl = rows(base_run.etl(), "SELECT source_file, row_key, failure_reason FROM etl_quarantine")
    elt = rows(base_run.elt(), """
        SELECT _source_file, order_id, failure_reason FROM stg_orders_batch WHERE dq_status = 'FAIL'
        UNION ALL SELECT _source_file, payment_id, failure_reason FROM stg_payments WHERE dq_status = 'FAIL'
        UNION ALL SELECT _source_file, customer_id, failure_reason FROM stg_customers WHERE dq_status = 'FAIL'
        UNION ALL SELECT _source_file, product_id, failure_reason FROM stg_products WHERE dq_status = 'FAIL'
        UNION ALL SELECT _source_file, store_id, failure_reason FROM stg_stores WHERE dq_status = 'FAIL'""")
    assert len(etl) == len(elt) == 142
    assert sorted(etl) == sorted(elt)


# ----------------------------------------------------------------------------------- dimensions (incl. PII masking)
def test_customer_dimension_and_masking_match(base_run):
    cols = ("customer_id, customer_name_masked, email_masked, phone_masked, city, area, pincode, "
            "signup_date, loyalty_tier, is_active")
    etl = rows(base_run.etl(), f"SELECT {cols} FROM etl_dim_customer")
    elt = rows(base_run.elt(), f"SELECT {cols} FROM stg_customers WHERE dq_status <> 'FAIL'")
    assert len(etl) == len(elt) == 400
    assert normalise(etl) == normalise(elt)


def test_product_dimension_matches(base_run):
    cols = "product_id, product_name, category, sub_category, brand, mrp, cost_price, unit, is_perishable, launch_date"
    etl = rows(base_run.etl(), f"SELECT {cols} FROM etl_dim_product")
    elt = rows(base_run.elt(), f"SELECT {cols} FROM stg_products WHERE dq_status <> 'FAIL'")
    assert len(etl) == len(elt) == 150
    assert normalise(etl) == normalise(elt)


def test_store_dimension_matches(base_run):
    cols = "store_id, store_name, zone, area, pincode, open_date, manager_name, capacity_orders_per_day"
    etl = rows(base_run.etl(), f"SELECT {cols} FROM etl_dim_store")
    elt = rows(base_run.elt(), f"SELECT {cols} FROM stg_stores WHERE dq_status <> 'FAIL'")
    assert len(etl) == len(elt) == 15
    assert normalise(etl) == normalise(elt)


# ----------------------------------------------------------------------------------- orders and business numbers
ORDER_COLS = ("order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, "
              "payment_mode, order_status, channel, delivery_minutes")


def test_clean_orders_are_identical(base_run):
    etl = rows(base_run.etl(), f"SELECT {ORDER_COLS} FROM etl_fact_orders")
    elt = rows(base_run.elt(), f"SELECT {ORDER_COLS} FROM stg_orders WHERE dq_status <> 'FAIL'")
    assert len(etl) == len(elt) == 2308
    assert normalise(etl) == normalise(elt)


def test_q1_daily_revenue_by_store_matches(base_run):
    etl = rows(base_run.etl(), "SELECT order_date, store_id, delivered_orders, net_revenue FROM etl_daily_zone_revenue")
    elt = rows(base_run.elt(), "SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis "
                               "WHERE delivered_orders > 0")
    assert normalise(etl) == normalise(elt)


def test_q2_category_margin_matches(base_run):
    cols = "category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct"
    assert normalise(rows(base_run.etl(), f"SELECT {cols} FROM etl_category_margin")) == \
           normalise(rows(base_run.elt(), f"SELECT {cols} FROM mart_category_margin"))


def test_q3_delivery_sla_matches(base_run):
    etl = rows(base_run.etl(), """
        SELECT store_id, count(*), round(avg(delivery_minutes), 2),
               round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 2)
        FROM etl_fact_orders WHERE order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL GROUP BY 1""")
    elt = rows(base_run.elt(), "SELECT store_id, delivered_orders, avg_delivery_minutes, pct_within_15_min FROM mart_delivery_sla")
    assert normalise(etl) == normalise(elt)


def test_q5_top_customers_match(base_run):
    etl = rows(base_run.etl(), """
        SELECT f.customer_id, sum(f.net_revenue) AS spend FROM etl_fact_orders f
        WHERE f.order_status = 'DELIVERED' GROUP BY 1 ORDER BY spend DESC, 1 LIMIT 10""")
    elt = rows(base_run.elt(), "SELECT customer_id, net_spend FROM mart_top_customers WHERE spend_rank <= 10 ORDER BY spend_rank, 1")
    assert normalise(etl) == normalise(elt)


def test_total_net_revenue_matches_to_the_paisa(base_run):
    etl = rows(base_run.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders")[0][0]
    elt = rows(base_run.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis")[0][0]
    assert etl == elt == Decimal("1134962.80")


# ----------------------------------------------------------------------------------- reconciliation
def test_payment_reconciliation_is_identical(base_run):
    sql = """SELECT coalesce(order_id, ''), coalesce(payment_id, ''), mismatch_reason, unreconciled_amount,
                    coalesce(order_status, ''), coalesce(payment_status, ''), coalesce(payment_dq_status, '')
             FROM {t}"""
    etl = rows(base_run.etl(), sql.format(t="etl_payment_reconciliation"))
    elt = rows(base_run.elt(), sql.format(t="mart_payment_reconciliation"))
    assert len(etl) == len(elt) == 181
    assert normalise(etl) == normalise(elt)


def test_orders_and_payments_reconcile_by_reason(base_run):
    got = dict(rows(base_run.elt(), "SELECT mismatch_reason, count(*) FROM mart_payment_reconciliation GROUP BY 1"))
    assert got == {"PAYMENT_FOR_REJECTED_ORDER": 90, "PAYMENT_FAILED": 42, "MISSING_PAYMENT": 28,
                   "AMOUNT_MISMATCH": 13, "ORPHAN_PAYMENT": 8}


# ----------------------------------------------------------------------------------- after the increment
def test_pipelines_still_agree_after_the_increment(full_run):
    etl = rows(full_run.etl(), f"SELECT {ORDER_COLS} FROM etl_fact_orders")
    elt = rows(full_run.elt(), f"SELECT {ORDER_COLS} FROM stg_orders WHERE dq_status <> 'FAIL'")
    assert len(etl) == len(elt) == 2458
    assert normalise(etl) == normalise(elt)
    assert rows(full_run.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders") == \
           rows(full_run.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis")
    etl_r = rows(full_run.etl(), "SELECT mismatch_reason, count(*), sum(unreconciled_amount) FROM etl_payment_reconciliation GROUP BY 1")
    elt_r = rows(full_run.elt(), "SELECT mismatch_reason, count(*), sum(unreconciled_amount) FROM mart_payment_reconciliation GROUP BY 1")
    assert normalise(etl_r) == normalise(elt_r)
