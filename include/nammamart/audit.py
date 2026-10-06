"""
include/nammamart/audit.py
--------------------------
Observability helpers shared by both DAGs:

  * write_dq_results   -> `dq_results`   (one row per check per run; safe to re-run / retry)
  * volume_baseline    -> rows seen by the previous run, used by the volume-drop check
  * write_pipeline_run -> `pipeline_runs` (one row per run; Clear + re-run overwrites it with the new outcome)
  * notify_failure     -> failure callback; posts to Slack when the Airflow Variable SLACK_WEBHOOK_URL is set
"""

from __future__ import annotations

import sys

import duckdb

from nammamart import dq_catalog as dq
from nammamart.config import INCLUDE_DIR


def make_result(check: dq.Check, rows_checked: int, rows_failed: int) -> dict:
    """One dq_results row (without run_id / dag_id / checked_at, which the writer adds)."""
    if rows_failed == 0:
        status = "PASS"
    else:
        status = "FAIL" if check.severity == dq.CRITICAL else "WARN"
    return {
        "check_name": check.name,
        "dimension": check.dimension,
        "severity": check.severity,
        "rows_checked": int(rows_checked),
        "rows_failed": int(rows_failed),
        "status": status,
    }


def write_dq_results(con: duckdb.DuckDBPyConnection, run_id: str, dag_id: str, results: list[dict]) -> int:
    """Replace this run's rows for these checks (so a task retry never double-writes), then insert."""
    if not results:
        return 0
    names = [r["check_name"] for r in results]
    placeholders = ",".join("?" * len(names))
    con.execute(
        f"DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name IN ({placeholders})",
        [run_id, dag_id, *names],
    )
    con.executemany(
        "INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp::TIMESTAMP)",
        [
            [run_id, dag_id, r["check_name"], r["dimension"], r["severity"], r["rows_checked"], r["rows_failed"], r["status"]]
            for r in results
        ],
    )
    return len(results)


def volume_baseline(con: duckdb.DuckDBPyConnection, dag_id: str, run_id: str, check_name: str = "BATCH_ORDERS_VOLUME"):
    """Rows the orders file had in the most recent EARLIER run of this DAG (None on the first run)."""
    row = con.execute(
        """
        SELECT rows_checked FROM dq_results
        WHERE dag_id = ? AND check_name = ? AND run_id <> ?
        ORDER BY checked_at DESC LIMIT 1
        """,
        [dag_id, check_name, run_id],
    ).fetchone()
    return None if row is None else int(row[0])


def write_pipeline_run(con: duckdb.DuckDBPyConnection, row: dict) -> None:
    con.execute("DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?", [row["run_id"], row["dag_id"]])
    con.execute(
        "INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            row["run_id"], row["dag_id"], row["start_time"], row["end_time"], row["rows_extracted"],
            row["rows_loaded"], row["rows_quarantined"], row["status"], row.get("failed_tasks", ""),
            row.get("notes", ""),
        ],
    )


# ------------------------------------------------------------------------------- alerting
def notify_failure(context: dict) -> None:
    """
    on_failure_callback: tell people a task failed. The Slack webhook is read from the Airflow Variable
    SLACK_WEBHOOK_URL (never from code or git). With no Variable set it just logs the alert.
    """
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "?")
    task_id = getattr(ti, "task_id", "?")
    run_id = context.get("run_id", "?")
    message = (
        f":rotating_light: NammaMart pipeline failure\n"
        f"DAG: {dag_id} | task: {task_id} | run: {run_id}\n"
        f"Error: {context.get('exception')}"
    )
    url = None
    try:
        from airflow.sdk import Variable

        url = Variable.get("SLACK_WEBHOOK_URL", default=None)
    except Exception as exc:  # no Variable / no Airflow context (tests) -> log only
        print(f"[alert] could not read SLACK_WEBHOOK_URL ({exc!r}); logging instead")

    if str(INCLUDE_DIR) not in sys.path:
        sys.path.insert(0, str(INCLUDE_DIR))
    from slack_notifier import send_slack_alert

    send_slack_alert(message, webhook_url=url)
