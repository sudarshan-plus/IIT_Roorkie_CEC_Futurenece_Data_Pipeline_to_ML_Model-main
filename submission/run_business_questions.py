"""
submission/run_business_questions.py
------------------------------------
Runs the business-question queries of one pipeline against a warehouse and prints each query with its result as
Markdown (paste into SUBMISSION.md / screenshot for the submission).

    python submission/run_business_questions.py --warehouse include/warehouse_sudarshan.duckdb --pipeline etl
    python submission/run_business_questions.py --warehouse include/warehouse_sudarshan.duckdb --pipeline elt --max-rows 12
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

QUERIES = Path(__file__).resolve().parent / "queries"


def load_queries(pipeline: str) -> list[tuple[str, str, str]]:
    text = (QUERIES / f"business_questions_{pipeline}.sql").read_text(encoding="utf-8")
    blocks = re.split(r"^-- @(\S+)\s+(.*)$", text, flags=re.M)
    # blocks = [preamble, id1, title1, sql1, id2, title2, sql2, ...]
    return [(blocks[i], blocks[i + 1], blocks[i + 2].strip()) for i in range(1, len(blocks), 3)]


def to_markdown(columns, rows) -> str:
    def fmt(v):
        return "" if v is None else str(v)

    lines = ["| " + " | ".join(columns) + " |", "|" + "|".join("---" for _ in columns) + "|"]
    lines += ["| " + " | ".join(fmt(v) for v in row) + " |" for row in rows]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--pipeline", choices=["etl", "elt"], required=True)
    parser.add_argument("--max-rows", type=int, default=20, help="rows to print per query (the full result is in the warehouse)")
    args = parser.parse_args()

    con = duckdb.connect(args.warehouse, read_only=True)
    for qid, title, sql in load_queries(args.pipeline):
        cursor = con.execute(sql)
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
        shown = rows[: args.max_rows]
        print(f"### {qid}: {title}\n\n```sql\n{sql}\n```\n")
        print(to_markdown(columns, shown))
        if len(rows) > len(shown):
            print(f"\n_... {len(rows) - len(shown)} more rows ({len(rows)} total)_")
        print()


if __name__ == "__main__":
    main()
