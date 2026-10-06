"""
Part C / Part A+B structure, machine-checked: both NammaMart DAGs parse, run at 09:00 IST Monday-Saturday with a
timezone-aware start_date, don't catch up, have retries/retry_delay/execution_timeout, extract in parallel, and finish
with an always-run audit task.
"""

from datetime import timedelta
from pathlib import Path

import pendulum
import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[2] / "dags"
ETL, ELT = "nammamart_etl_sudarshan", "nammamart_elt_sudarshan"
EXTRACTS = {"extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores"}


@pytest.fixture(scope="module")
def bag():
    try:                                    # Airflow 2.x / early 3.x
        return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    except TypeError:                       # newer 3.x dropped the argument
        return DagBag(dag_folder=str(DAGS_DIR))


def test_both_dags_parse_without_import_errors(bag):
    mine = {k: v for k, v in bag.import_errors.items() if "nammamart" in k}
    assert not mine, mine
    assert {ETL, ELT} <= set(bag.dags)


@pytest.mark.parametrize("dag_id", [ETL, ELT])
def test_schedule_is_0900_ist_monday_to_saturday(bag, dag_id):
    dag = bag.dags[dag_id]
    assert dag.timetable.summary == "0 9 * * 1-6"
    assert str(dag.timetable._timezone) in ("Asia/Kolkata", "Timezone('Asia/Kolkata')")
    # Airflow stores start_date in UTC: 2026-08-31 18:30 UTC *is* 2026-09-01 00:00 IST. A start_date naively left in UTC
    # would be 2026-09-01 00:00 UTC instead - that is exactly what this assertion guards against.
    assert dag.start_date == pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata")
    assert (dag.start_date.hour, dag.start_date.minute) == (18, 30)
    assert dag.catchup is False
    assert dag.max_active_runs == 1                                                         # DuckDB: one writer


@pytest.mark.parametrize("dag_id", [ETL, ELT])
def test_retries_retry_delay_and_timeout_are_set(bag, dag_id):
    args = bag.dags[dag_id].default_args
    assert args["retries"] >= 2
    assert args["retry_delay"] == timedelta(seconds=60)
    assert args["execution_timeout"] == timedelta(minutes=10)
    assert callable(args["on_failure_callback"])
    assert bag.dags[dag_id].tags


@pytest.mark.parametrize("dag_id", [ETL, ELT])
def test_five_extract_tasks_run_in_parallel(bag, dag_id):
    dag = bag.dags[dag_id]
    extracts = {t.task_id: t for t in dag.tasks if t.task_id in EXTRACTS}
    assert set(extracts) == EXTRACTS
    assert all(not t.upstream_task_ids for t in extracts.values())      # nothing to wait for => they start together


@pytest.mark.parametrize("dag_id", [ETL, ELT])
def test_audit_runs_even_when_upstream_fails(bag, dag_id):
    dag = bag.dags[dag_id]
    audit = dag.get_task("write_run_audit_row")
    assert str(audit.trigger_rule) in ("all_done", "TriggerRule.ALL_DONE")
    others = {t.task_id for t in dag.tasks} - {"write_run_audit_row", "run_verdict"}
    assert others <= set(audit.get_flat_relative_ids(upstream=True))     # it waits for EVERYTHING, however it ends


@pytest.mark.parametrize("dag_id", [ETL, ELT])
def test_a_failed_task_fails_the_run(bag, dag_id):
    """The last task must be able to fail the DAG run, otherwise a broken night looks green at run level."""
    dag = bag.dags[dag_id]
    verdict = dag.get_task("run_verdict")
    assert str(verdict.trigger_rule) in ("all_done", "TriggerRule.ALL_DONE")
    assert verdict.downstream_task_ids == set()                          # it is the only leaf: its state is the run's state
    assert "write_run_audit_row" in verdict.upstream_task_ids


def test_etl_flow_order(bag):
    dag = bag.dags[ETL]
    assert dag.get_task("validate_orders").upstream_task_ids >= {"validate_customers", "validate_products", "validate_stores"}
    assert "validate_orders" in dag.get_task("validate_payments").upstream_task_ids
    assert "quarantine_failed_rows" in dag.get_task("transform_data").upstream_task_ids
    assert {"transform_data", "log_dq_results"} <= dag.get_task("load_warehouse").upstream_task_ids


def test_elt_has_raw_staging_and_mart_layers(bag):
    ids = {t.task_id for t in bag.dags[ELT].tasks}
    assert {f"load_raw_{n}" for n in ("orders", "payments", "customers", "products", "stores")} <= ids
    assert "load_raw_orders_increment" in ids
    assert {f"stg_{n}" for n in ("orders", "payments", "customers", "products", "stores")} <= ids
    assert {f"mart_{n}" for n in ("daily_store_kpis", "category_margin", "delivery_sla", "payment_reconciliation", "top_customers")} <= ids
