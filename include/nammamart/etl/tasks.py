"""
include/nammamart/etl/tasks.py
------------------------------
The ETL DAG's task bodies as plain functions (the DAG file only wires them together). Keeping the logic here
means the same code can be run and tested without an Airflow scheduler (`run_etl_locally`).

Data hand-off between tasks is by files in the run's work folder (small stats go through XCom), so a retried
or cleared task can always re-read its inputs.
"""

from __future__ import annotations

import pandas as pd

from nammamart.audit import volume_baseline, write_dq_results
from nammamart.config import ETL_DAG_ID
from nammamart.etl import extract, history, load, quarantine, reconcile, report, transform, validate
from nammamart.etl.extract import work_path
from nammamart.warehouse import ensure_core_tables, warehouse

DIMENSION_VALIDATORS = {
    "customers": validate.validate_customers,
    "products": validate.validate_products,
    "stores": validate.validate_stores,
}


def _read_extract(rt: dict, kind: str) -> pd.DataFrame:
    return pd.read_csv(work_path(rt) / f"extract_{kind}.csv", dtype=str, keep_default_na=False)


def _save_scored(rt: dict, scored: validate.Scored) -> dict:
    scored.df.to_csv(work_path(rt) / f"scored_{scored.table}.csv", index=False)
    df = scored.df
    summary = {
        "table": scored.table,
        "rows": int(len(df)),
        "passed": int((df["dq_critical"] == "").sum()),
        "failed": int((df["dq_critical"] != "").sum()),
        "warned": int(((df["dq_critical"] == "") & (df["dq_warnings"] != "")).sum()),
        "results": scored.results + scored.batch_results,
        **scored.stats,
    }
    print(f"[Validate:{scored.table}] rows={summary['rows']} passed={summary['passed']} "
          f"quarantined={summary['failed']} (of the passed, {summary['warned']} carry warnings)")
    for r in summary["results"]:
        if r["rows_failed"]:
            print(f"    {r['status']:<4} {r['check_name']:<32} {r['dimension']:<12} {r['rows_failed']:>5} / {r['rows_checked']}")
    return summary


def validate_dimension(kind: str, rt: dict) -> dict:
    scored = DIMENSION_VALIDATORS[kind](_read_extract(rt, kind), rt["as_of"])
    return _save_scored(rt, scored)


def validate_orders(rt: dict, dag_id: str = ETL_DAG_ID) -> dict:
    baseline = None
    if _warehouse_has_history(rt):
        with warehouse(rt["warehouse"]) as con:
            ensure_core_tables(con)
            baseline = volume_baseline(con, dag_id, rt["run_id"])
    scored = validate.validate_orders(
        _read_extract(rt, "orders"),
        transform.read_scored(rt, "customers"),
        transform.read_scored(rt, "products"),
        transform.read_scored(rt, "stores"),
        rt["as_of"],
        volume_baseline=baseline,
    )
    return _save_scored(rt, scored)


def validate_payments(rt: dict) -> dict:
    known_ids, known_amounts = history.known_orders(rt)
    scored = validate.validate_payments(_read_extract(rt, "payments"), transform.read_scored(rt, "orders"), rt["as_of"],
                                        known_ids=known_ids, known_amounts=known_amounts)
    return _save_scored(rt, scored)


def _warehouse_has_history(rt: dict) -> bool:
    from pathlib import Path
    return Path(rt["warehouse"]).exists()


def log_dq_results(rt: dict, validations: list[dict], dag_id: str = ETL_DAG_ID) -> dict:
    """Write the result of EVERY check of this run to dq_results (passes included - the scorecard needs them)."""
    results = [r for v in validations for r in v["results"]]
    with warehouse(rt["warehouse"]) as con:
        ensure_core_tables(con)
        n = write_dq_results(con, rt["run_id"], dag_id, results)
    failed = [r for r in results if r["rows_failed"]]
    print(f"[DQ] {n} check results written to dq_results ({len(failed)} with failures)")
    return {"checks": n, "checks_with_failures": len(failed)}


def run_etl_locally(rt: dict, dag_id: str = ETL_DAG_ID) -> dict:
    """The whole ETL, in DAG order, in one process (used by tests and the experiment harness)."""
    stats = {kind: extract.extract_source(kind, rt) for kind in ("orders", "payments", "customers", "products", "stores")}
    validations = [validate_dimension(k, rt) for k in ("customers", "products", "stores")]
    validations.append(validate_orders(rt, dag_id))
    validations.append(validate_payments(rt))
    log_dq_results(rt, validations, dag_id)
    quar = quarantine.quarantine_failed_rows(rt)
    transform.transform(rt)
    loaded = load.load_warehouse(rt)
    recon = reconcile.reconcile_payments(rt)
    rep = report.build_reports(rt)
    return {"extract": stats, "validate": validations, "quarantine": quar, "load": loaded, "reconcile": recon,
            "report": rep}
