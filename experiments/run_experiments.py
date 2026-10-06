"""
experiments/run_experiments.py
------------------------------
Runs the ten Part F observability experiments on BOTH DAGs against a live Airflow (scheduler running) and records
what actually happened: task states, retry attempts and timings, the run-audit row, the data-quality results and
the warehouse counts. Results go to experiments/results/<experiment>.json.

Every experiment reads a COPY of the data (experiments/make_experiment_data.py) and writes a COPY of the baseline
warehouse, so the real files and the real warehouse are never touched and nothing has to be restored afterwards.

Needs: the `airflow` CLI and the Airflow metadata DB reachable from this process (e.g. `astro dev bash`), plus
duckdb and pandas. Usage:
    python experiments/run_experiments.py --scratch ~/nm_exp                # all experiments
    python experiments/run_experiments.py --scratch ~/nm_exp --only 5,6,7   # a subset (1-3 build the baseline first)
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))
sys.path.insert(0, str(ROOT / "experiments"))

import duckdb  # noqa: E402
import make_experiment_data  # noqa: E402
from nammamart.config import ELT_DAG_ID, ETL_DAG_ID  # noqa: E402

RESULTS = ROOT / "experiments" / "results"
DAGS = (ETL_DAG_ID, ELT_DAG_ID)
TERMINAL = {"success", "failed"}


# ------------------------------------------------------------------------------------------ Airflow plumbing
def airflow(*args: str) -> str:
    out = subprocess.run(["airflow", *args], capture_output=True, text=True, cwd=str(Path.home()))
    if out.returncode != 0:
        raise RuntimeError(f"airflow {' '.join(args)} failed:\n{out.stdout[-800:]}\n{out.stderr[-800:]}")
    return out.stdout


def trigger(dag_id: str, run_id: str, conf: dict) -> None:
    # Airflow 3 manual runs have no logical date unless one is given; `tasks clear` selects runs by it
    logical_date = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    airflow("dags", "trigger", dag_id, "-r", run_id, "-c", json.dumps(conf), "-l", logical_date)
    print(f"  triggered {dag_id} run_id={run_id}")
    time.sleep(0.01)


def clear_task(dag_id: str, run_id: str, task_id: str) -> dict:
    """
    'Clear' a task and everything downstream of it, exactly like the button in the Airflow UI (same REST endpoint).
    Airflow's CLI `tasks clear` selects runs by logical date and matched nothing for manually-triggered runs.
    """
    import requests

    base = os.environ.get("AIRFLOW_API_URL", "http://localhost:8080")
    token = requests.post(f"{base}/auth/token", json={"username": "admin", "password": "admin"}, timeout=30).json()["access_token"]
    response = requests.post(
        f"{base}/api/v2/dags/{dag_id}/clearTaskInstances",
        headers={"Authorization": f"Bearer {token}"},
        json={"dry_run": False, "task_ids": [task_id], "dag_run_id": run_id, "include_downstream": True,
              "only_failed": False, "reset_dag_runs": True},
        timeout=60,
    )
    response.raise_for_status()
    return response.json()


def _run_state(dag_id: str, run_id: str) -> str | None:
    from airflow.models.dagrun import DagRun
    from airflow.utils.session import create_session
    from sqlalchemy import select

    with create_session() as s:
        run = s.scalars(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)).first()
        return None if run is None else str(run.state)


def wait(dag_id: str, run_id: str, timeout: int = 900, left_terminal: bool = False) -> str:
    """Block until the run is success/failed. `left_terminal`: first wait for a cleared run to start again."""
    deadline = time.time() + timeout
    if left_terminal:
        while _run_state(dag_id, run_id) in TERMINAL and time.time() < deadline:
            time.sleep(1)
    while time.time() < deadline:
        state = _run_state(dag_id, run_id)
        if state in TERMINAL:
            return state
        time.sleep(2)
    raise TimeoutError(f"{dag_id} {run_id} still not finished after {timeout}s")


def task_report(dag_id: str, run_id: str) -> dict:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory
    from airflow.utils.session import create_session
    from sqlalchemy import select

    with create_session() as s:
        run = s.scalars(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)).first()
        tis = s.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.run_id == run_id)).all()
        history = s.scalars(select(TaskInstanceHistory).where(
            TaskInstanceHistory.dag_id == dag_id, TaskInstanceHistory.run_id == run_id)).all()
        t0 = min((t.start_date for t in tis if t.start_date), default=None)
        tasks = [{
            "task_id": t.task_id, "state": str(t.state), "try_number": t.try_number,
            "start_offset_s": None if not (t.start_date and t0) else round((t.start_date - t0).total_seconds(), 1),
            "duration_s": None if t.duration is None else round(float(t.duration), 1),
            "end": None if not t.end_date else t.end_date.isoformat(),
        } for t in sorted(tis, key=lambda x: (x.start_date is None, x.start_date))]
        attempts = [{
            "task_id": h.task_id, "try_number": h.try_number, "state": str(h.state),
            "start": None if not h.start_date else h.start_date.strftime("%H:%M:%S"),
            "end": None if not h.end_date else h.end_date.strftime("%H:%M:%S"),
            "duration_s": None if h.duration is None else round(float(h.duration), 1),
        } for h in sorted(history, key=lambda x: (x.task_id, x.try_number))]
        return {
            "run_state": str(run.state) if run else None,
            "run_duration_s": None if not (run and run.start_date and run.end_date) else round((run.end_date - run.start_date).total_seconds(), 1),
            "tasks": tasks, "retry_attempts": attempts,
            "states": {k: sum(1 for t in tasks if t["state"] == k) for k in sorted({t["state"] for t in tasks})},
            "logical_date": None if not run else run.logical_date.isoformat() if run.logical_date else None,
        }


def log_lines(dag_id: str, run_id: str, task_id: str, needle: str) -> list[str]:
    """Lines containing `needle` from the task's log files (what a person on call would actually see)."""
    base = Path(os.environ.get("AIRFLOW_HOME", Path.home() / "airflow")) / "logs" / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    found: list[str] = []
    for path in sorted(base.glob("attempt=*.log")):
        for line in path.read_text(errors="replace").splitlines():
            if needle in line:
                found.append(f"{path.name}: {line.strip()[:260]}")
    return found


# ------------------------------------------------------------------------------------------ warehouse evidence
def q(wh: Path, sql: str, params: list | None = None) -> list:
    if not wh.exists():
        return []
    con = duckdb.connect(str(wh), read_only=True)
    try:
        return con.execute(sql, params or []).fetchall()
    except duckdb.CatalogException:       # table not created yet (e.g. ELT tables right after an ETL-only run)
        return []
    finally:
        con.close()


def warehouse_counts(wh: Path) -> dict:
    one = lambda sql: (q(wh, sql) or [[None]])[0][0]
    return {
        "etl_fact_orders": one("SELECT count(*) FROM etl_fact_orders"),
        "etl_fact_delivered_revenue": float(one("SELECT coalesce(sum(net_revenue),0) FROM etl_fact_orders") or 0),
        "stg_orders_total": one("SELECT count(*) FROM stg_orders"),
        "stg_orders_clean": one("SELECT count(*) FROM stg_orders WHERE dq_status <> 'FAIL'"),
        "mart_delivered_revenue": float(one("SELECT coalesce(sum(net_revenue),0) FROM mart_daily_store_kpis") or 0),
        "raw_orders": one("SELECT count(*) FROM raw_orders"),
        "raw_payments": one("SELECT count(*) FROM raw_payments"),
        "raw_customers": one("SELECT count(*) FROM raw_customers"),
        "raw_file_registry": one("SELECT count(*) FROM raw_file_registry"),
        "etl_order_changes": one("SELECT count(*) FROM etl_order_changes"),
        "stg_order_changes": one("SELECT count(*) FROM stg_order_changes"),
    }


def evidence(dag_id: str, run_id: str, wh: Path) -> dict:
    audit = q(wh, "SELECT status, rows_extracted, rows_loaded, rows_quarantined, failed_tasks, notes FROM pipeline_runs "
                  "WHERE dag_id = ? AND run_id = ?", [dag_id, run_id])
    dq = q(wh, "SELECT check_name, severity, status, rows_checked, rows_failed FROM dq_results "
               "WHERE dag_id = ? AND run_id = ? AND rows_failed > 0 ORDER BY check_name", [dag_id, run_id])
    return {
        "pipeline_runs": None if not audit else dict(zip(
            ("status", "rows_extracted", "rows_loaded", "rows_quarantined", "failed_tasks", "notes"), audit[0])),
        "dq_checks_with_failures": [dict(zip(("check_name", "severity", "status", "rows_checked", "rows_failed"), r)) for r in dq],
        "dq_checks_written": (q(wh, "SELECT count(*) FROM dq_results WHERE dag_id = ? AND run_id = ?", [dag_id, run_id]) or [[0]])[0][0],
        "counts": warehouse_counts(wh),
    }


def save(name: str, payload: dict) -> None:
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / f"{name}.json").write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(f"  -> experiments/results/{name}.json")


# ------------------------------------------------------------------------------------------ experiment helpers
class Lab:
    def __init__(self, scratch: Path):
        self.scratch = scratch
        self.tag = dt.datetime.now().strftime("%m%d%H%M%S")
        self.data = make_experiment_data.build(scratch / "data")
        self.baseline = scratch / "baseline.duckdb"

    def conf(self, folder: str, wh: Path, increments: bool = True, as_of: str = "") -> dict:
        return {"data_dir": str(self.data[folder]), "warehouse_path": str(wh), "load_increments": increments, "as_of": as_of}

    def run_dag(self, exp: str, dag_id: str, conf: dict, wh: Path) -> dict:
        run_id = f"{exp}_{dag_id.split('_')[1]}_{self.tag}"
        trigger(dag_id, run_id, conf)
        state = wait(dag_id, run_id)
        report = task_report(dag_id, run_id)
        report.update(evidence(dag_id, run_id, wh))
        report.update({"dag_id": dag_id, "run_id": run_id, "final_state": state, "conf": conf})
        print(f"  {dag_id}: {state} in {report['run_duration_s']}s | tasks {report['states']}")
        return report

    def pair(self, exp: str, conf: dict, wh: Path) -> dict:
        return {dag: self.run_dag(exp, dag, conf, wh) for dag in DAGS}

    def fresh_copy_of_baseline(self, name: str) -> Path:
        target = self.scratch / f"{name}.duckdb"
        if target.exists():
            target.unlink()
        shutil.copy2(self.baseline, target)
        return target


# ------------------------------------------------------------------------------------------ the experiments
def exp1_to_3(lab: Lab, only: set[int]) -> None:
    wh = lab.scratch / "w_exp123.duckdb"
    if wh.exists():
        wh.unlink()
    print("EXPERIMENT 1 - happy path (fresh warehouse, September files only)")
    e1 = lab.pair("exp1", lab.conf("base", wh, increments=False), wh)
    save("exp1_happy_path", e1)

    print("EXPERIMENT 2 - re-run the same DAGs (idempotency)")
    e2 = lab.pair("exp2", lab.conf("base", wh, increments=False), wh)
    e2["counts_after_exp1"] = {d: e1[d]["counts"] for d in DAGS}
    save("exp2_rerun", e2)

    print("EXPERIMENT 3 - next-day increment file lands")
    e3 = lab.pair("exp3", lab.conf("with_increment", wh, increments=True), wh)
    save("exp3_increment", e3)

    print("EXPERIMENT 3b - re-run after the increment (must be a no-op)")
    e3b = lab.pair("exp3b", lab.conf("with_increment", wh, increments=True), wh)
    save("exp3b_rerun_after_increment", e3b)
    shutil.copy2(wh, lab.baseline)


def exp4_and_10(lab: Lab, only: set[int]) -> None:
    wh = lab.fresh_copy_of_baseline("w_exp4")
    folder = lab.data["exp4_missing_file"]
    conf = lab.conf("exp4_missing_file", wh)
    print("EXPERIMENT 4 - products.csv missing (both DAGs at once, as at 09:00)")
    runs = {}
    for dag in DAGS:
        run_id = f"exp4_{dag.split('_')[1]}_{lab.tag}"
        trigger(dag, run_id, conf)
        runs[dag] = run_id
    e4 = {}
    for dag, run_id in runs.items():
        state = wait(dag, run_id)
        rep = task_report(dag, run_id)
        rep.update(evidence(dag, run_id, wh))
        rep.update({"dag_id": dag, "run_id": run_id, "final_state": state,
                    "alert_log": log_lines(dag, run_id, "extract_products", "[Slack]"),
                    "error_log": log_lines(dag, run_id, "extract_products", "FileNotFoundError")[:2]})
        e4[dag] = rep
        print(f"  {dag}: {state} | {rep['states']}")
    save("exp4_missing_file", e4)

    print("EXPERIMENT 10 - fix the cause, then Clear the failed task (+ downstream)")
    (folder / "products.csv.missing").rename(folder / "products.csv")
    cleared_at = dt.datetime.now(dt.timezone.utc)
    e10 = {}
    for dag, run_id in runs.items():
        cleared = clear_task(dag, run_id, "extract_products")
        print(f"  cleared extract_products (+downstream) in {dag}: {cleared.get('total_entries')} task instances reset")
    for dag, run_id in runs.items():
        state = wait(dag, run_id, left_terminal=True)
        rep = task_report(dag, run_id)
        rep.update(evidence(dag, run_id, wh))
        # which tasks actually ran again after the Clear?
        rep["reran_after_clear"] = sorted(t["task_id"] for t in rep["tasks"]
                                          if t["end"] and dt.datetime.fromisoformat(t["end"]) > cleared_at)
        rep["not_rerun"] = sorted(t["task_id"] for t in rep["tasks"] if t["task_id"] not in rep["reran_after_clear"])
        rep.update({"dag_id": dag, "run_id": run_id, "final_state": state})
        e10[dag] = rep
        print(f"  {dag}: {state} | re-ran {len(rep['reran_after_clear'])} tasks, left {len(rep['not_rerun'])} untouched")
    save("exp10_recovery", e10)


def simple_experiment(lab: Lab, name: str, exp_key: str, folder: str, as_of: str = "", extra=None) -> None:
    wh = lab.fresh_copy_of_baseline(f"w_{exp_key}")
    before = warehouse_counts(wh)
    res = lab.pair(exp_key, lab.conf(folder, wh, as_of=as_of), wh)
    res["counts_before"] = before
    if extra:
        res["detail"] = extra(wh)
    save(name, res)


def exp5_detail(wh: Path) -> dict:
    return {
        "etl_quarantine_new_rows": q(wh, "SELECT row_key, failure_reason FROM etl_quarantine WHERE row_key LIKE 'ORD9%' ORDER BY 1"),
        "elt_stg_new_rows": q(wh, "SELECT order_id, dq_status, coalesce(failure_reason, '') FROM stg_orders WHERE order_id LIKE 'ORD9%' ORDER BY 1"),
        "etl_fact_has_new_rows": q(wh, "SELECT count(*) FROM etl_fact_orders WHERE order_id LIKE 'ORD9%'")[0][0],
        "mart_has_new_rows": q(wh, "SELECT count(*) FROM v_orders_clean WHERE order_id LIKE 'ORD9%'")[0][0],
    }


def exp8_detail(wh: Path) -> dict:
    return {
        "s08_revenue_in_fact": q(wh, "SELECT coalesce(sum(net_revenue), 0) FROM etl_fact_orders WHERE store_id = 'S08'")[0][0],
        "s08_revenue_in_mart": q(wh, "SELECT coalesce(sum(net_revenue), 0) FROM mart_daily_store_kpis WHERE store_id = 'S08'")[0][0],
        "s08_orders_in_fact": q(wh, "SELECT count(*) FROM etl_fact_orders WHERE store_id = 'S08'")[0][0],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scratch", required=True, help="scratch folder for experiment data and warehouse copies")
    parser.add_argument("--only", default="", help="comma list of experiment numbers (default: all)")
    args = parser.parse_args()
    only = {int(x) for x in args.only.split(",") if x} or set(range(1, 11)) | {80}
    scratch = Path(args.scratch).expanduser()
    scratch.mkdir(parents=True, exist_ok=True)
    lab = Lab(scratch)

    if only & {1, 2, 3}:
        exp1_to_3(lab, only)
    if not lab.baseline.exists():
        sys.exit("baseline warehouse missing: run experiments 1-3 first")
    if only & {4, 10}:
        exp4_and_10(lab, only)
    if 5 in only:
        print("EXPERIMENT 5 - five new bad rows")
        simple_experiment(lab, "exp5_new_bad_data", "exp5", "exp5_bad_rows", extra=exp5_detail)
    if 6 in only:
        print("EXPERIMENT 6 - schema change (order_amount -> total)")
        simple_experiment(lab, "exp6_schema_change", "exp6", "exp6_schema_change")
    if 7 in only:
        print("EXPERIMENT 7 - volume anomaly (50 orders)")
        simple_experiment(lab, "exp7_volume_anomaly", "exp7", "exp7_volume_drop")
    if 8 in only:
        print("EXPERIMENT 8 - distribution drift (unit_price x100, store S08)")
        simple_experiment(lab, "exp8_distribution_drift", "exp8", "exp8_price_drift", extra=exp8_detail)
    if 8 in only or 80 in only:
        print("EXPERIMENT 8b - consistent drift (unit_price AND order_amount x100, store S08)")
        simple_experiment(lab, "exp8b_consistent_drift", "exp8b", "exp8b_consistent_drift", extra=exp8_detail)
    if 9 in only:
        print("EXPERIMENT 9 - freshness (newest order ~30 days old at run time)")
        simple_experiment(lab, "exp9_freshness", "exp9", "with_increment", as_of="2026-10-31 09:00:00")
    print("done")


if __name__ == "__main__":
    main()
