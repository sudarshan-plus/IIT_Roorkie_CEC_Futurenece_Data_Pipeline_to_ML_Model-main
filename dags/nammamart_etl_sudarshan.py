"""
dags/nammamart_etl_sudarshan.py
-------------------------------
NammaMart ETL pipeline  -  "clean BEFORE loading"  (pandas -> DuckDB star schema)

Flow:
  extract_orders ----┐                                   ┌--> log_dq_results --┐
  extract_payments --┤                                   |                      |
  extract_customers -┼--> validate_* --> (5 results) ----+                      +--> load_warehouse --> reconcile_payments
  extract_products --┤    (customers/products/stores     |                      |        --> build_report
  extract_stores ----┘     first, then orders, then      └--> quarantine -------┴--> transform
                           payments)
  every task ---------------------------------------------> write_run_audit (all_done) --> run_verdict (fails the RUN if any task failed)

  * Finance wants ONLY validated rows in the reporting tables -> rows failing a Critical check are quarantined with a reason.
  * Idempotent: re-running never double counts (MERGE on order_id with a row hash).
  * Next-day increments (orders_*_increment.csv) are applied as upserts: new orders added, corrections overwrite.

Schedule: 09:00 IST, Monday-Saturday  (cron `0 9 * * 1-6`, timezone Asia/Kolkata = 03:30 UTC).
Tables written to include/warehouse_sudarshan.duckdb: etl_dim_customer / etl_dim_product / etl_dim_store / etl_fact_orders,
etl_payment_reconciliation, etl_daily_zone_revenue, etl_category_margin, etl_quarantine, etl_order_changes,
and the shared dq_results + pipeline_runs.
"""

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

import pendulum
from airflow.sdk import Param, TriggerRule, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

from nammamart.audit import notify_failure
from nammamart.config import ETL_DAG_ID, TZ
from nammamart.dag_support import (PARAM_HELP, fail_fast_on_schema_change, raise_if_run_failed, runtime_from_context,
                                   write_run_audit)
from nammamart.etl import extract as etl_extract
from nammamart.etl import load, quarantine, reconcile, report, tasks, transform

default_args = {
    "owner": "fde-sudarshan",
    "retries": 2,                                   # a transient problem (locked file, slow disk) should not wake anyone up
    "retry_delay": timedelta(seconds=60),           # 3 attempts inside ~3 minutes: still far ahead of the 9:30 stand-up
    "execution_timeout": timedelta(minutes=10),     # a hung task must fail, not silently eat the morning
    "on_failure_callback": notify_failure,          # Slack via the SLACK_WEBHOOK_URL Airflow Variable (log-only if unset)
}


@dag(
    dag_id=ETL_DAG_ID,
    description="NammaMart ETL: extract -> validate -> quarantine -> transform -> load star schema -> reconcile -> report",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone=TZ),      # 09:00 IST Mon-Sat
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),                # timezone-aware: without tz Airflow assumes UTC
    catchup=False,                                                  # each run reloads the full snapshot idempotently, so missed days need no backfill
    max_active_runs=1,                                              # DuckDB has a single writer: never two runs at once
    default_args=default_args,
    params={
        "as_of": Param("", type="string", description=PARAM_HELP["as_of"]),
        "load_increments": Param(True, type="boolean", description=PARAM_HELP["load_increments"]),
        "data_dir": Param("", type="string", description=PARAM_HELP["data_dir"]),
        "warehouse_path": Param("", type="string", description=PARAM_HELP["warehouse_path"]),
    },
    tags=["fde", "nammamart", "etl"],
    doc_md=__doc__,
)
def nammamart_etl_sudarshan():

    def rt():
        return runtime_from_context(get_current_context())

    # ── 1. EXTRACT: five tasks, one per file, run in parallel ───────────────────────────────────────
    @task
    def extract_orders():
        with fail_fast_on_schema_change():
            return etl_extract.extract_source("orders", rt())

    @task
    def extract_payments():
        with fail_fast_on_schema_change():
            return etl_extract.extract_source("payments", rt())

    @task
    def extract_customers():
        with fail_fast_on_schema_change():
            return etl_extract.extract_source("customers", rt())

    @task
    def extract_products():
        with fail_fast_on_schema_change():
            return etl_extract.extract_source("products", rt())

    @task
    def extract_stores():
        with fail_fast_on_schema_change():
            return etl_extract.extract_source("stores", rt())

    # ── 2. VALIDATE: dimensions first (orders need their keys), then orders, then payments ─────────
    @task
    def validate_customers(extracted):
        return tasks.validate_dimension("customers", rt())

    @task
    def validate_products(extracted):
        return tasks.validate_dimension("products", rt())

    @task
    def validate_stores(extracted):
        return tasks.validate_dimension("stores", rt())

    @task
    def validate_orders(extracted, customers, products, stores):
        return tasks.validate_orders(rt())

    @task
    def validate_payments(extracted, orders):
        return tasks.validate_payments(rt())

    # ── DQ scorecard + 3. QUARANTINE (both read the validation output) ─────────────────────────────
    @task
    def log_dq_results(customers, products, stores, orders, payments):
        return tasks.log_dq_results(rt(), [customers, products, stores, orders, payments])

    @task
    def quarantine_failed_rows(customers, products, stores, orders, payments):
        return quarantine.quarantine_failed_rows(rt())

    # ── 4. TRANSFORM → 5. LOAD → 6. RECONCILE → 7. REPORT ──────────────────────────────────────────
    @task
    def transform_data(quarantined):
        return transform.transform(rt())

    @task
    def load_warehouse(transformed, dq_logged):
        return load.load_warehouse(rt())

    @task
    def reconcile_payments(loaded):
        return reconcile.reconcile_payments(rt())

    @task
    def build_report(reconciled):
        return report.build_reports(rt())

    # ── RUN AUDIT: always runs, writes one row to pipeline_runs ────────────────────────────────────
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_run_audit_row():
        ctx = get_current_context()
        ti = ctx["ti"]
        pull = lambda task_id: ti.xcom_pull(task_ids=task_id, default=None) or {}
        extracted = [pull(t) for t in ("extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores")]
        loaded = pull("load_warehouse")
        quarantined = pull("quarantine_failed_rows")
        notes = {
            "files": {e["file"]: e["rows"] for e in extracted if e},
            "increment_files": {i["file"]: i["rows"] for e in extracted if e for i in e.get("increments", [])},
            "fact_inserted": loaded.get("fact_inserted"), "fact_updated": loaded.get("fact_updated"),
            "fact_unchanged": loaded.get("fact_unchanged"), "fact_total": loaded.get("fact_total"),
            "quarantine_files": [f["file"] for f in quarantined.get("files", [])],
            "mismatches": (pull("reconcile_payments") or {}).get("mismatches"),
        }
        return write_run_audit(
            ctx, ETL_DAG_ID, rt(),
            rows_extracted=sum(e.get("rows_total", 0) for e in extracted),
            rows_loaded=loaded.get("rows_loaded", 0),
            rows_quarantined=quarantined.get("rows_quarantined", 0),
            notes=notes, audit_task_id="write_run_audit_row",
        )

    # ── RUN VERDICT: the DAG's only leaf; fails when anything failed, so the RUN turns red (not just a task) ─────────
    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def run_verdict():
        raise_if_run_failed(get_current_context())

    # ── wiring ─────────────────────────────────────────────────────────────────────────────────────
    ex_orders, ex_payments = extract_orders(), extract_payments()
    ex_customers, ex_products, ex_stores = extract_customers(), extract_products(), extract_stores()

    v_customers = validate_customers(ex_customers)
    v_products = validate_products(ex_products)
    v_stores = validate_stores(ex_stores)
    v_orders = validate_orders(ex_orders, v_customers, v_products, v_stores)
    v_payments = validate_payments(ex_payments, v_orders)

    dq = log_dq_results(v_customers, v_products, v_stores, v_orders, v_payments)
    quarantined = quarantine_failed_rows(v_customers, v_products, v_stores, v_orders, v_payments)

    transformed = transform_data(quarantined)
    loaded = load_warehouse(transformed, dq)
    reconciled = reconcile_payments(loaded)
    reported = build_report(reconciled)

    audit = write_run_audit_row()
    [ex_orders, ex_payments, ex_customers, ex_products, ex_stores, v_customers, v_products, v_stores, v_orders,
     v_payments, dq, quarantined, transformed, loaded, reconciled, reported] >> audit
    audit >> run_verdict()


nammamart_etl_sudarshan()
