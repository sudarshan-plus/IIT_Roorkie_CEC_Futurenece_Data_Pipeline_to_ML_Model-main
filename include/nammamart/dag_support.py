"""
include/nammamart/dag_support.py
--------------------------------
The few Airflow-specific helpers both DAG files share. Importing this module must stay cheap and side-effect
free: Airflow imports the DAG files every ~30 seconds just to read their structure.
"""

from __future__ import annotations

import contextlib
import json
from datetime import datetime, timezone

from nammamart.audit import write_pipeline_run
from nammamart.config import resolve_runtime
from nammamart.etl.extract import SchemaContractError
from nammamart.warehouse import ensure_core_tables, warehouse

# Params shown in the "Trigger DAG" form. All optional; blank means "use the default".
PARAM_HELP = {
    "as_of": "Treat this IST time as 'now' for the future-date / freshness checks, e.g. 2026-10-30 09:00:00. Blank = real now.",
    "load_increments": "Also apply any orders_*_increment.csv found in the data folder (late corrections + new orders).",
    "data_dir": "Folder holding the source CSVs. Blank = include/data/nammamart.",
    "warehouse_path": "DuckDB file to write. Blank = include/warehouse_<owner>.duckdb.",
}


@contextlib.contextmanager
def fail_fast_on_schema_change():
    """
    A missing file may simply not have arrived yet, so it is retried. A changed schema will be just as wrong on
    the next attempt, so it must fail immediately (no retries) and alert, instead of burning the 9 AM window.
    """
    try:
        yield
    except SchemaContractError as exc:
        from airflow.sdk.exceptions import AirflowFailException

        raise AirflowFailException(str(exc)) from exc


def runtime_from_context(context: dict) -> dict:
    """DAG params + run id -> the concrete settings every task needs (see config.resolve_runtime)."""
    raw = context.get("params") or {}
    params = {k: getattr(v, "value", v) for k, v in dict(raw).items()}
    return resolve_runtime(params, context["run_id"])


def task_states(context: dict) -> dict[str, str]:
    """State of every task in THIS run, e.g. {'extract_orders': 'success', 'load_warehouse': 'failed'}."""
    ti = context["ti"]
    try:
        states = ti.get_task_states(dag_id=ti.dag_id, run_ids=[ti.run_id])
        return {task_id: str(state) for task_id, state in (states.get(ti.run_id) or {}).items()}
    except Exception as exc:  # defensive: auditing must never be the thing that fails
        print(f"[Audit] could not read task states: {exc!r}")
        return {}


AUDIT_TASK_ID = "write_run_audit_row"
VERDICT_TASK_ID = "run_verdict"


def failed_tasks(context: dict, exclude: tuple[str, ...] = ()) -> list[str]:
    """Tasks of this run that failed or never ran because something upstream failed."""
    return sorted(t for t, s in task_states(context).items() if t not in exclude and s in ("failed", "upstream_failed"))


def raise_if_run_failed(context: dict) -> None:
    """
    Last task of every DAG (trigger_rule=all_done). The audit task always succeeds - that is its job - and being the
    only leaf it would make Airflow show a broken night as a GREEN run (found in Experiment 4). This task is the new leaf:
    it fails when anything upstream failed, so the DAG run itself turns red and the failure is impossible to miss.
    """
    bad = failed_tasks(context, exclude=(VERDICT_TASK_ID,))
    if bad:
        from airflow.sdk.exceptions import AirflowFailException

        raise AirflowFailException(f"Run FAILED: {', '.join(bad)}. pipeline_runs has the row; the first failed task's log has the cause.")
    print("[Verdict] every task succeeded - run is healthy")


def write_run_audit(context: dict, dag_id: str, rt: dict, rows_extracted: int, rows_loaded: int,
                    rows_quarantined: int, notes: dict, audit_task_id: str) -> dict:
    """
    One row per run in `pipeline_runs`. Runs even when upstream tasks failed (trigger_rule=all_done), so a failed
    night still leaves a row saying which tasks broke. Clearing a failed task and re-running overwrites the row.
    """
    bad = failed_tasks(context, exclude=(audit_task_id,))
    status = "FAILED" if bad else "SUCCESS"
    dag_run = context.get("dag_run")
    start = getattr(dag_run, "start_date", None) or datetime.now(timezone.utc)
    end = datetime.now(timezone.utc)
    row = {
        "run_id": rt["run_id"], "dag_id": dag_id,
        "start_time": start.astimezone(timezone.utc).replace(tzinfo=None),
        "end_time": end.replace(tzinfo=None),
        "rows_extracted": int(rows_extracted), "rows_loaded": int(rows_loaded),
        "rows_quarantined": int(rows_quarantined),
        "status": status,
        "failed_tasks": ", ".join(bad),
        "notes": json.dumps(notes, default=str, sort_keys=True),
    }
    with warehouse(rt["warehouse"]) as con:
        ensure_core_tables(con)
        write_pipeline_run(con, row)
    print(f"[Audit] {dag_id} run {rt['run_id']}: {status}"
          + (f" (failed: {row['failed_tasks']})" if bad else "")
          + f" | extracted={row['rows_extracted']} loaded={row['rows_loaded']} quarantined={row['rows_quarantined']}")
    return row
