"""
include/nammamart/etl/quarantine.py
-----------------------------------
ETL step 3 - QUARANTINE. Rows that failed a Critical check are set aside, never silently dropped:

  include/quarantine/<owner>/<file>_failed_<run_id>.csv   the rejected rows + `failure_reason`
  etl_quarantine                                          one metadata row per rejected row (for SQL / Business Q6)

PII is masked in the quarantine copy of customers: ops need the key and the reason to fix the source system,
not the person's phone number. (The untouched originals stay in the source export / ELT raw layer.)
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from nammamart.config import EXPECTED_COLUMNS
from nammamart.etl.extract import work_path
from nammamart.etl.transform import read_scored
from nammamart.masking import mask_email, mask_name, mask_phone
from nammamart.warehouse import ensure_core_tables, run_sql_file, warehouse

KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}


def quarantine_failed_rows(rt: dict) -> dict:
    qdir = Path(rt["quarantine_dir"])
    qdir.mkdir(parents=True, exist_ok=True)
    meta: list[pd.DataFrame] = []
    files: list[dict] = []

    for name, key in KEYS.items():
        scored = read_scored(rt, name)
        failed = scored[scored["dq_critical"] != ""]
        if failed.empty:
            continue
        for source_file, group in failed.groupby("_source_file"):
            out = group[EXPECTED_COLUMNS[name]].copy()
            if name == "customers":
                out["full_name"] = out["full_name"].map(mask_name)
                out["email"] = out["email"].map(mask_email)
                out["phone"] = out["phone"].map(mask_phone)
            out["failure_reason"] = group["dq_critical"]
            out["source_file"] = source_file
            out["source_row_num"] = group["_source_row_num"]
            path = qdir / f"{Path(source_file).stem}_failed_{rt['safe_run_id']}.csv"
            out.to_csv(path, index=False)
            files.append({"file": path.name, "rows": len(out)})
            meta.append(pd.DataFrame({
                "source_file": source_file,
                "source_row_num": group["_source_row_num"].astype(int),
                "row_key": group[key].str.strip(),
                "failure_reason": group["dq_critical"],
            }))

    total = sum(f["rows"] for f in files)
    rows = pd.concat(meta, ignore_index=True) if meta else pd.DataFrame(
        columns=["source_file", "source_row_num", "row_key", "failure_reason"])
    with warehouse(rt["warehouse"]) as con:
        ensure_core_tables(con)
        run_sql_file(con, "etl_tables.sql")
        con.execute("DELETE FROM etl_quarantine WHERE run_id = ?", [rt["run_id"]])
        if len(rows):
            con.register("q_rows", rows)
            con.execute(
                "INSERT INTO etl_quarantine SELECT ?, source_file, source_row_num, row_key, failure_reason, "
                "current_timestamp::TIMESTAMP FROM q_rows", [rt["run_id"]])
    for f in files:
        print(f"[Quarantine] {f['rows']:>4} rows -> {f['file']}")
    print(f"[Quarantine] total rejected rows this run: {total}")
    return {"rows_quarantined": total, "files": files}
