"""
Acceptance criteria 5 and 6:
  5. Re-running never double-counts revenue (fact row counts do not change; ELT raw does not grow for identical files).
  6. The next-day file loads incrementally: new orders appended, late corrections applied, no duplicates.
"""

from decimal import Decimal


def q(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def test_rerun_does_not_change_the_fact_table_or_revenue(full_run):
    r1, r2 = full_run.results["run1"], full_run.results["run2_rerun"]
    assert r1["etl"]["load"]["fact_inserted"] == 2308
    assert r2["etl"]["load"]["fact_inserted"] == 0 and r2["etl"]["load"]["fact_updated"] == 0
    assert r2["etl"]["load"]["fact_total"] == 2308
    # ELT: the merge touches nothing and raw gets no new rows
    assert r2["elt"]["staging"]["orders"]["merge_inserted"] == 0 and r2["elt"]["staging"]["orders"]["merge_updated"] == 0
    assert all(v["rows_loaded"] == 0 for k, v in r2["elt"]["raw"].items() if k != "orders_increments")


def test_elt_raw_is_append_only_and_exact_copy(base_run):
    con = base_run.elt()
    assert q(con, "SELECT count(*) FROM raw_orders")[0][0] == 2412           # every source row, bad ones included
    assert q(con, "SELECT count(*) FROM raw_customers")[0][0] == 405
    # raw really is untouched text: the unclean values are still there exactly as received
    assert q(con, "SELECT count(*) FROM raw_orders WHERE order_status = 'Pending??'")[0][0] == 3
    assert q(con, "SELECT count(*) FROM raw_orders WHERE order_ts = '31/09/2026 10:00'")[0][0] == 2
    assert q(con, "SELECT count(*) FROM raw_customers WHERE email = 'N/A'")[0][0] == 4
    assert q(con, "SELECT count(*) FROM raw_orders WHERE customer_id = ''")[0][0] == 18   # blanks stay blanks, not NULL
    cols = {r[0]: r[1] for r in q(con, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'raw_orders'")}
    assert all(t == "VARCHAR" for c, t in cols.items() if not c.startswith("_"))


def test_increment_inserts_150_and_updates_10(full_run):
    inc = full_run.results["run3_increment"]
    assert inc["etl"]["load"]["fact_inserted"] == 150 and inc["etl"]["load"]["fact_updated"] == 10
    assert inc["elt"]["staging"]["orders"]["merge_inserted"] == 150
    assert inc["elt"]["staging"]["orders"]["merge_updated"] == 10
    assert inc["etl"]["load"]["fact_total"] == 2458


def test_corrections_overwrite_without_duplicates(full_run):
    for con, table, status_filter in ((full_run.etl(), "etl_fact_orders", "1=1"), (full_run.elt(), "stg_orders", "1=1")):
        assert q(con, f"SELECT count(*) - count(DISTINCT order_id) FROM {table}")[0][0] == 0
        statuses = q(con, f"SELECT order_status FROM {table} WHERE order_id = 'ORD000337'")
        assert statuses == [("RETURNED",)]                                     # was DELIVERED in orders.csv
        assert q(con, f"SELECT count(*) FROM {table} WHERE order_status = 'RETURNED' AND order_id IN "
                      "('ORD000017','ORD000271','ORD000332','ORD000337','ORD000584','ORD000708','ORD000822','ORD001025','ORD001067','ORD001990')")[0][0] == 10


def test_second_increment_run_is_a_no_op(full_run):
    r = full_run.results["run4_rerun_increment"]
    assert r["etl"]["load"]["fact_inserted"] == 0 and r["etl"]["load"]["fact_updated"] == 0
    assert r["elt"]["staging"]["orders"]["merge_inserted"] == 0 and r["elt"]["staging"]["orders"]["merge_updated"] == 0
    assert all(v["rows_loaded"] == 0 for v in r["elt"]["raw"]["orders_increments"])


def test_elt_raw_keeps_both_versions_of_a_corrected_order(full_run):
    con = full_run.elt()
    versions = q(con, "SELECT _source_file, order_status FROM raw_orders WHERE order_id = 'ORD000337' ORDER BY _batch_id")
    assert versions == [("orders.csv", "DELIVERED"), ("orders_2026-10-01_increment.csv", "RETURNED")]
    assert q(con, "SELECT count(*) FROM raw_orders")[0][0] == 2412 + 160


def test_change_history_is_kept_in_both_pipelines(full_run):
    etl = q(full_run.etl(), "SELECT order_id, old_status, new_status, old_net_revenue, new_net_revenue FROM etl_order_changes ORDER BY 1")
    elt = q(full_run.elt(), "SELECT order_id, old_status, new_status, old_order_amount FROM stg_order_changes ORDER BY 1")
    assert len(etl) == len(elt) == 10
    assert all(o == "DELIVERED" and n == "RETURNED" and nr == 0 for _, o, n, _, nr in etl)
    assert [(a, b, c) for a, b, c, _ in elt] == [(a, b, c) for a, b, c, _, _ in etl]


def test_september_revenue_restated_by_returns(full_run):
    """Governance Q9: the 10 corrections remove their revenue from September; the history table explains the difference."""
    returned = Decimal("4193.70")
    for con, sql in (
        (full_run.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders WHERE order_date < '2026-10-01'"),
        (full_run.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis WHERE order_date < '2026-10-01'"),
    ):
        assert q(con, sql)[0][0] == Decimal("1134962.80") - returned
