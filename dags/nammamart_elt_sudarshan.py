"""
dags/nammamart_elt_sudarshan.py
-------------------------------
NammaMart ELT pipeline  -  "load raw FIRST, clean later in SQL"  (DuckDB: raw -> staging -> mart)

Flow:
  extract_* (5, parallel: locate + schema contract + checksum, NO cleaning)
      -> load_raw_* (5)  exact text copy of each CSV, bad rows included, never deleted
      -> load_raw_orders_increment   append orders_*_increment.csv as a new batch
      -> stg_customers / stg_products / stg_stores  (SQL: cast, standardise, dedupe, mask PII, DQ rules)
      -> stg_orders   (SQL: DQ rules, then MERGE on order_id: new orders inserted, corrections overwrite)
      -> stg_payments
      -> dq_results   (SQL: the scorecard)
      -> mart_*  (8 SQL files)  -> report  -> write_run_audit (always) -> run_verdict (fails the RUN if any task failed)

  * Analytics / compliance want every raw record kept exactly as received -> raw_* are append-only audit tables.
  * Rejected rows are never deleted either: staging labels them dq_status = 'FAIL' + failure_reason, marts filter them out.
  * Idempotent: an identical file is skipped by SHA-256 (raw does not grow); the MERGE only touches rows whose hash changed.

Schedule: 09:00 IST, Monday-Saturday  (cron `0 9 * * 1-6`, timezone Asia/Kolkata = 03:30 UTC).
Layers in include/warehouse_sudarshan.duckdb: raw_*  ->  stg_*  ->  mart_*  (+ shared dq_results, pipeline_runs).
Every transformation is a .sql file in include/sql/.
"""

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

import pendulum
from airflow.sdk import Param, TriggerRule, dag, get_current_context, task
from airflow.timetables.trigger import CronTriggerTimetable

from nammamart.audit import notify_failure
from nammamart.config import ELT_DAG_ID, TZ
from nammamart.dag_support import (PARAM_HELP, fail_fast_on_schema_change, raise_if_run_failed, runtime_from_context,
                                   task_states, write_run_audit)
from nammamart.elt import extract as elt_extract
from nammamart.elt import tasks

default_args = {
    "owner": "fde-sudarshan",
    "retries": 2,                                   # a transient problem (locked file, slow disk) should not wake anyone up
    "retry_delay": timedelta(seconds=60),           # 3 attempts inside ~3 minutes: still far ahead of the 9:30 stand-up
    "execution_timeout": timedelta(minutes=10),     # a hung task must fail, not silently eat the morning
    "on_failure_callback": notify_failure,          # Slack via the SLACK_WEBHOOK_URL Airflow Variable (log-only if unset)
}


@dag(
    dag_id=ELT_DAG_ID,
    description="NammaMart ELT: extract -> load raw -> stage (SQL) -> MERGE increment -> marts (SQL)",
    schedule=CronTriggerTimetable("0 9 * * 1-6", timezone=TZ),      # 09:00 IST Mon-Sat
    start_date=pendulum.datetime(2026, 9, 1, tz=TZ),                # timezone-aware: without tz Airflow assumes UTC
    catchup=False,                                                  # every run re-derives staging/marts from raw idempotently, so missed days need no backfill
    max_active_runs=1,                                              # DuckDB has a single writer: never two runs at once
    default_args=default_args,
    params={
        "as_of": Param("", type="string", description=PARAM_HELP["as_of"]),
        "load_increments": Param(True, type="boolean", description=PARAM_HELP["load_increments"]),
        "data_dir": Param("", type="string", description=PARAM_HELP["data_dir"]),
        "warehouse_path": Param("", type="string", description=PARAM_HELP["warehouse_path"]),
    },
    tags=["fde", "nammamart", "elt"],
    doc_md=__doc__,
)
def nammamart_elt_sudarshan():

    def rt():
        return runtime_from_context(get_current_context())

    # ── 1. EXTRACT: five tasks in parallel; no cleaning, just "where is it, is the header right, what is its hash" ──
    @task
    def extract_orders():
        with fail_fast_on_schema_change():
            return elt_extract.extract_source("orders", rt())

    @task
    def extract_payments():
        with fail_fast_on_schema_change():
            return elt_extract.extract_source("payments", rt())

    @task
    def extract_customers():
        with fail_fast_on_schema_change():
            return elt_extract.extract_source("customers", rt())

    @task
    def extract_products():
        with fail_fast_on_schema_change():
            return elt_extract.extract_source("products", rt())

    @task
    def extract_stores():
        with fail_fast_on_schema_change():
            return elt_extract.extract_source("stores", rt())

    # ── 2. LOAD RAW: exact copies, all columns text ────────────────────────────────────────────────
    @task
    def load_raw_orders(meta):
        return tasks.load_raw_table("orders", meta, rt())

    @task
    def load_raw_payments(meta):
        return tasks.load_raw_table("payments", meta, rt())

    @task
    def load_raw_customers(meta):
        return tasks.load_raw_table("customers", meta, rt())

    @task
    def load_raw_products(meta):
        return tasks.load_raw_table("products", meta, rt())

    @task
    def load_raw_stores(meta):
        return tasks.load_raw_table("stores", meta, rt())

    @task
    def load_raw_orders_increment(meta, orders_loaded):
        return tasks.load_raw_increments(meta, rt())

    # ── 3. STAGING (SQL): dimensions first, then orders (MERGE), then payments ─────────────────────
    @task
    def stg_customers(loaded):
        return tasks.build_staging("customers", rt())

    @task
    def stg_products(loaded):
        return tasks.build_staging("products", rt())

    @task
    def stg_stores(loaded):
        return tasks.build_staging("stores", rt())

    @task
    def stg_orders(customers, products, stores, orders_loaded, increments_loaded):
        return tasks.build_staging_orders(rt())

    @task
    def stg_payments(orders, payments_loaded):
        return tasks.build_staging("payments", rt())

    @task
    def dq_results(meta, payments_staged):
        return tasks.record_dq_results(rt(), meta["rows"])

    # ── 4. MARTS (SQL): one task per table ─────────────────────────────────────────────────────────
    def mart(name):
        @task(task_id=f"mart_{name}")
        def _build():
            return tasks.build_mart(name, rt())
        return _build()

    @task
    def report():
        return tasks.elt_report(rt())

    # ── RUN AUDIT: always runs, writes one row to pipeline_runs ────────────────────────────────────
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_run_audit_row():
        ctx = get_current_context()
        ti = ctx["ti"]
        run_rt = rt()
        pull = lambda task_id: ti.xcom_pull(task_ids=task_id, default=None) or {}
        extracted = [pull(t) for t in ("extract_orders", "extract_payments", "extract_customers", "extract_products", "extract_stores")]
        raw = [pull(t) for t in ("load_raw_orders", "load_raw_payments", "load_raw_customers", "load_raw_products", "load_raw_stores")]
        increments = ti.xcom_pull(task_ids="load_raw_orders_increment", default=None) or []
        staged = pull("stg_orders")
        states = task_states(ctx)
        # only report rejects if staging really ran in THIS run (otherwise the tables hold yesterday's verdict)
        staged_now = all(states.get(t) == "success" for t in ("stg_customers", "stg_products", "stg_stores", "stg_orders", "stg_payments"))
        rejected = tasks.count_rejected_rows(run_rt) if staged_now else 0
        rows_extracted = sum(e.get("rows", 0) + sum(i["rows"] for i in e.get("increments", [])) for e in extracted if e)
        rows_loaded = sum(r.get("rows_loaded", 0) for r in raw if r) + sum(i.get("rows_loaded", 0) for i in increments)
        notes = {
            "raw_loads": {r["file"]: r["status"] for r in raw + list(increments) if r},
            "merge_inserted": staged.get("merge_inserted"), "merge_updated": staged.get("merge_updated"),
            "merge_unchanged": staged.get("merge_unchanged"), "stg_orders_total": staged.get("stg_orders_total"),
        }
        return write_run_audit(ctx, ELT_DAG_ID, run_rt, rows_extracted=rows_extracted, rows_loaded=rows_loaded,
                               rows_quarantined=rejected, notes=notes, audit_task_id="write_run_audit_row")

    # ── RUN VERDICT: the DAG's only leaf; fails when anything failed, so the RUN turns red (not just a task) ─────────
    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def run_verdict():
        raise_if_run_failed(get_current_context())

    # ── wiring ─────────────────────────────────────────────────────────────────────────────────────
    ex_orders, ex_payments = extract_orders(), extract_payments()
    ex_customers, ex_products, ex_stores = extract_customers(), extract_products(), extract_stores()

    raw_orders = load_raw_orders(ex_orders)
    raw_payments = load_raw_payments(ex_payments)
    raw_customers = load_raw_customers(ex_customers)
    raw_products = load_raw_products(ex_products)
    raw_stores = load_raw_stores(ex_stores)
    raw_increment = load_raw_orders_increment(ex_orders, raw_orders)

    s_customers = stg_customers(raw_customers)
    s_products = stg_products(raw_products)
    s_stores = stg_stores(raw_stores)
    s_orders = stg_orders(s_customers, s_products, s_stores, raw_orders, raw_increment)
    s_payments = stg_payments(s_orders, raw_payments)
    dq = dq_results(ex_orders, s_payments)

    marts = [mart(n) for n in tasks.MARTS]
    for m in marts:
        dq >> m
    reported = report()
    marts >> reported

    audit = write_run_audit_row()
    [ex_orders, ex_payments, ex_customers, ex_products, ex_stores, raw_orders, raw_payments, raw_customers, raw_products,
     raw_stores, raw_increment, s_customers, s_products, s_stores, s_orders, s_payments, dq, *marts, reported] >> audit
    audit >> run_verdict()


nammamart_elt_sudarshan()
