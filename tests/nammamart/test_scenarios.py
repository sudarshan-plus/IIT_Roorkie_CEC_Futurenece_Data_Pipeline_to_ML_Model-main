"""
Regression tests for what the Part F experiments FOUND. Each scenario reproduces the experiment on the experiment data
and asserts the behaviour after the fix - and that the two pipelines still agree.

  Exp 5/7  a re-delivered or partial base orders.csv must not undo corrections that arrived in the increment (ELT bug)
  Exp 7    a partial orders file must not turn valid payments into orphans (ETL cascade)
  Exp 8b   a self-consistent 100x price drift must be caught, not loaded into revenue (only a Warning before)
  Bonus    SCD Type 2 loyalty-tier history; store-capacity table
"""

import csv
import shutil
from decimal import Decimal

import pytest

from conftest import Pipelines
from nammamart.config import ELT_DAG_ID, ETL_DAG_ID
from nammamart.etl.tasks import run_etl_locally


def q(con, sql, params=None):
    return con.execute(sql, params or []).fetchall()


def n(value):
    return round(float(value), 2)


# ------------------------------------------------------------------------------------------ re-delivered base file
@pytest.fixture(scope="module")
def redelivery(tmp_path_factory, experiment_data):
    p = Pipelines(tmp_path_factory.mktemp("redelivery"))
    p.run("base", "r1", increments=False, data_dir=str(experiment_data["base"]))
    p.run("incr", "r2", increments=True, data_dir=str(experiment_data["with_increment"]))
    # orders.csv now arrives again with different content (old September data + 5 extra rows) next to the increment
    p.run("redelivered", "r3", increments=True, data_dir=str(experiment_data["exp5_bad_rows"]))
    return p


def test_redelivered_base_does_not_undo_the_increments_corrections(redelivery):
    for con, table in ((redelivery.etl(), "etl_fact_orders"), (redelivery.elt(), "stg_orders")):
        assert q(con, f"SELECT order_status FROM {table} WHERE order_id = 'ORD000337'") == [("RETURNED",)]
    etl = q(redelivery.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders")[0][0]
    elt = q(redelivery.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis")[0][0]
    assert etl == elt == Decimal("1210421.30")                     # before the fix the ELT said 1,214,615.00


def test_the_five_new_bad_rows_are_quarantined_with_their_reasons(redelivery):
    expected = {"ORD900001": "ORD_CUSTOMER_ID_NOT_NULL", "ORD900002": "ORD_QUANTITY_POSITIVE", "ORD900003": "ORD_STORE_EXISTS",
                "ORD900004": "ORD_STATUS_VALID", "ORD900005": "ORD_ORDER_TS_NOT_FUTURE"}
    etl = dict(q(redelivery.etl(), "SELECT row_key, failure_reason FROM etl_quarantine WHERE row_key LIKE 'ORD9%'"))
    elt = dict(q(redelivery.elt(), "SELECT order_id, failure_reason FROM stg_orders WHERE order_id LIKE 'ORD9%' AND dq_status = 'FAIL'"))
    assert etl == elt == expected
    assert q(redelivery.etl(), "SELECT count(*) FROM etl_fact_orders WHERE order_id LIKE 'ORD9%'")[0][0] == 0


# ------------------------------------------------------------------------------------------ partial orders file
@pytest.fixture(scope="module")
def partial_file(tmp_path_factory, experiment_data):
    p = Pipelines(tmp_path_factory.mktemp("partial"))
    p.run("base", "p1", increments=False, data_dir=str(experiment_data["base"]))
    p.run("incr", "p2", increments=True, data_dir=str(experiment_data["with_increment"]))
    p.run("partial", "p3", increments=True, data_dir=str(experiment_data["exp7_volume_drop"]))       # orders.csv has 50 rows
    return p


def test_volume_drop_is_flagged_by_both_pipelines(partial_file):
    for con, dag in ((partial_file.etl(), ETL_DAG_ID), (partial_file.elt(), ELT_DAG_ID)):
        row = q(con, "SELECT status, rows_checked, rows_failed FROM dq_results WHERE dag_id = ? AND check_name = 'BATCH_ORDERS_VOLUME' "
                     "ORDER BY checked_at DESC LIMIT 1", [dag])[0]
        assert row == ("WARN", 50, 2362)                           # 50 rows vs 2,412 the run before


def test_partial_orders_file_does_not_turn_payments_into_orphans(partial_file):
    for con, dag in ((partial_file.etl(), ETL_DAG_ID), (partial_file.elt(), ELT_DAG_ID)):
        failed = q(con, "SELECT rows_failed FROM dq_results WHERE dag_id = ? AND check_name = 'PAY_ORDER_EXISTS' "
                        "ORDER BY checked_at DESC LIMIT 1", [dag])[0][0]
        assert failed == 8                                         # only the 8 true orphans; before the fix the ETL rejected 2,172


def test_partial_file_leaves_revenue_unchanged_and_pipelines_agree(partial_file):
    etl = q(partial_file.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders")[0][0]
    elt = q(partial_file.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis")[0][0]
    assert etl == elt == Decimal("1210421.30")


# ------------------------------------------------------------------------------------------ price drift
@pytest.fixture(scope="module")
def price_drift(tmp_path_factory, experiment_data):
    p = Pipelines(tmp_path_factory.mktemp("drift"))
    p.run("base", "d1", increments=False, data_dir=str(experiment_data["base"]))
    p.run("incr", "d2", increments=True, data_dir=str(experiment_data["with_increment"]))
    p.run("drift", "d3", increments=True, data_dir=str(experiment_data["exp8b_consistent_drift"]))
    return p


def test_consistent_100x_price_drift_is_rejected_not_loaded(price_drift):
    for con, dag in ((price_drift.etl(), ETL_DAG_ID), (price_drift.elt(), ELT_DAG_ID)):
        row = q(con, "SELECT severity, status, rows_failed FROM dq_results WHERE dag_id = ? AND check_name = 'ORD_PRICE_FAR_FROM_CATALOGUE' "
                     "ORDER BY checked_at DESC LIMIT 1", [dag])[0]
        assert row[0] == "Critical" and row[1] == "FAIL" and row[2] > 100
    # revenue must not explode: before the fix the total went from 12.1 lakh to 83.8 lakh
    etl = q(price_drift.etl(), "SELECT sum(net_revenue) FROM etl_fact_orders")[0][0]
    elt = q(price_drift.elt(), "SELECT sum(net_revenue) FROM mart_daily_store_kpis")[0][0]
    assert etl == elt and etl < Decimal("1210421.30")
    assert q(price_drift.etl(), "SELECT count(*) FROM etl_fact_orders WHERE store_id = 'S08' AND source_file = 'orders.csv'")[0][0] == 0


def test_store_reject_rate_check_points_at_the_broken_store(price_drift):
    for con, dag in ((price_drift.etl(), ETL_DAG_ID), (price_drift.elt(), ELT_DAG_ID)):
        row = q(con, "SELECT status, rows_failed FROM dq_results WHERE dag_id = ? AND check_name = 'BATCH_STORE_REJECT_RATE' "
                     "ORDER BY checked_at DESC LIMIT 1", [dag])[0]
        assert row == ("WARN", 1)


# ------------------------------------------------------------------------------------------ bonus: SCD2
def test_scd2_keeps_loyalty_tier_history(tmp_path, experiment_data):
    from conftest import _runtime

    data = tmp_path / "customers_v2"
    shutil.copytree(experiment_data["base"], data)

    def run(run_id, tier_changes=None):
        if tier_changes is not None:
            path = data / "customers.csv"
            with path.open(newline="") as handle:
                rows = list(csv.reader(handle))
            header, body = rows[0], rows[1:]
            tier = header.index("loyalty_tier")
            for row in body:
                if row[0] in tier_changes:
                    row[tier] = tier_changes[row[0]]
            with path.open("w", newline="") as handle:
                csv.writer(handle).writerows([header, *body])
        import contextlib, io
        with contextlib.redirect_stdout(io.StringIO()):
            run_etl_locally(_runtime(tmp_path, "etl", run_id, False, data_dir=str(data)), ETL_DAG_ID)
        import duckdb
        return duckdb.connect(str(tmp_path / "etl" / "wh.duckdb"), read_only=True)

    con = run("scd1")
    assert q(con, "SELECT count(*), count(*) FILTER (WHERE is_current) FROM etl_dim_customer_scd2")[0] == (400, 400)
    current = dict(q(con, "SELECT customer_id, loyalty_tier FROM etl_dim_customer_scd2 WHERE customer_id IN ('C00001','C00002','C00003')"))
    con.close()

    # three customers are promoted
    changes = {cid: ("Gold" if tier != "Gold" else "Platinum") for cid, tier in current.items()}
    con = run("scd3", changes)
    assert q(con, "SELECT count(*), count(*) FILTER (WHERE is_current) FROM etl_dim_customer_scd2")[0] == (403, 400)
    history = q(con, "SELECT customer_id, loyalty_tier, is_current, valid_to IS NULL FROM etl_dim_customer_scd2 "
                     "WHERE customer_id = 'C00001' ORDER BY valid_from")
    assert len(history) == 2 and history[0][2] is False and history[1][2] is True and history[1][1] == changes["C00001"]
    con.close()

    con = run("scd4", changes)                                      # same tiers again: no new history
    assert q(con, "SELECT count(*) FROM etl_dim_customer_scd2")[0][0] == 403
    con.close()


# ------------------------------------------------------------------------------------------ bonus: store capacity
def test_store_capacity_tables_agree_and_handle_unknown_capacity(base_run):
    cols = "order_date, store_id, capacity_orders_per_day, orders_placed, utilisation_pct, over_capacity"
    etl = q(base_run.etl(), f"SELECT {cols} FROM etl_store_capacity ORDER BY 1, 2")
    elt = q(base_run.elt(), f"SELECT {cols} FROM mart_store_capacity ORDER BY 1, 2")
    assert len(etl) == len(elt) > 400
    assert [tuple(map(str, r)) for r in etl] == [tuple(map(str, r)) for r in elt]
    # S12's capacity is 0 in the master -> unknown capacity -> NULL utilisation, never a divide-by-zero
    assert all(r[4] is None for r in etl if r[1] == "S12")
    assert max(r[4] for r in etl if r[4] is not None) < 100            # nobody is over capacity in this data
