"""
submission/evidence/build_query_results.py
------------------------------------------
Renders the three tables the submission checklist asks for - dq_results, pipeline_runs and the payment reconciliation -
as Markdown and as one self-contained HTML page (easy to screenshot), straight from a warehouse file.

    python submission/evidence/build_query_results.py --warehouse <warehouse_sudarshan.duckdb> [--out submission/evidence/query_results]
"""

from __future__ import annotations

import argparse
import html
from pathlib import Path

import duckdb

QUERIES = [
    ("pipeline_runs - one row per run, failed runs included",
     "SELECT dag_id, run_id, status, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, failed_tasks "
     "FROM pipeline_runs ORDER BY start_time DESC LIMIT 12"),
    ("dq_results - the scorecard of the latest run of each DAG (checks that found something)",
     "SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results r "
     "WHERE rows_failed > 0 AND run_id = (SELECT run_id FROM dq_results x WHERE x.dag_id = r.dag_id ORDER BY checked_at DESC LIMIT 1) "
     "ORDER BY dag_id, rows_failed DESC, check_name LIMIT 24"),
    ("dq_results - every check is written every run (passes included): rows per DAG and status for the latest run",
     "SELECT dag_id, status, count(*) AS checks FROM dq_results r "
     "WHERE run_id = (SELECT run_id FROM dq_results x WHERE x.dag_id = r.dag_id ORDER BY checked_at DESC LIMIT 1) "
     "GROUP BY dag_id, status ORDER BY dag_id, status"),
    ("etl_payment_reconciliation - one row per order/payment mismatch (summary by reason)",
     "SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount "
     "FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC"),
    ("etl_payment_reconciliation - sample rows",
     "SELECT order_id, payment_id, order_status, payment_status, order_amount, amount_paid, unreconciled_amount, mismatch_reason "
     "FROM etl_payment_reconciliation WHERE mismatch_reason IN ('AMOUNT_MISMATCH', 'PAYMENT_FAILED', 'ORPHAN_PAYMENT', 'RETURNED_NOT_REFUNDED') "
     "ORDER BY mismatch_reason, order_id LIMIT 14"),
    ("mart_payment_reconciliation (ELT) - the same reconciliation, identical rows",
     "SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount "
     "FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC"),
]


def fetch(con, sql):
    cur = con.execute(sql)
    return [d[0] for d in cur.description], cur.fetchall()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--out", default="submission/evidence/query_results")
    args = parser.parse_args()
    con = duckdb.connect(args.warehouse, read_only=True)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    md, page = [], []
    for title, sql in QUERIES:
        cols, rows = fetch(con, sql)
        md.append(f"### {title}\n\n```sql\n{sql}\n```\n")
        md.append("| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
                  + "\n".join("| " + " | ".join("" if v is None else str(v) for v in r) + " |" for r in rows) + "\n")
        head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
        body = "".join("<tr>" + "".join(f"<td>{html.escape('' if v is None else str(v))}</td>" for v in r) + "</tr>" for r in rows)
        page.append(f"<h2>{html.escape(title)}</h2><pre>{html.escape(sql)}</pre><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>")

    (out / "query_results.md").write_text("\n".join(md), encoding="utf-8")
    (out / "query_results.html").write_text(
        """<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Query results</title><style>
:root{color-scheme:light dark;--line:#8884}
body{font-family:system-ui,sans-serif;margin:16px auto;max-width:1240px;padding:0 16px;font-size:14px}
h2{font-size:15px;margin:22px 0 4px}pre{font-size:12px;opacity:.7;white-space:pre-wrap;margin:2px 0 6px}
table{border-collapse:collapse;width:100%;font-size:12.5px}th,td{border:1px solid var(--line);padding:3px 7px;text-align:left;white-space:nowrap}
th{background:#8882}tr:nth-child(even) td{background:#8881}
</style></head><body><h1 style="font-size:18px">Warehouse query results</h1>"""
        + "".join(page) + "</body></html>", encoding="utf-8")
    print(f"wrote {out}/query_results.md and .html ({len(QUERIES)} queries)")


if __name__ == "__main__":
    main()
