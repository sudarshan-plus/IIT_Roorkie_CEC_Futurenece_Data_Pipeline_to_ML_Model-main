"""
include/nammamart/elt/load_raw.py
---------------------------------
ELT step 2 - LOAD RAW. Copies a CSV into its raw_* table exactly as received (all text, bad rows included).

Idempotency without ever deleting raw data: each load is recorded in raw_file_registry with the file's SHA-256.
  * same file content again  -> skipped (a re-run adds 0 raw rows; nothing is double counted)
  * changed or new file      -> appended as a NEW batch with a new _batch_id (e.g. the next-day increment)
Older batches are never touched, so history can be replayed and audited.
"""

from __future__ import annotations

from nammamart import dq_catalog
from nammamart.warehouse import ensure_core_tables, run_sql_file, warehouse


def prepare_warehouse(con) -> None:
    ensure_core_tables(con)
    run_sql_file(con, "elt_tables.sql")
    # (re)seed the rulebook so SQL can join to it
    con.execute("DELETE FROM dq_check_catalog")
    con.executemany("INSERT INTO dq_check_catalog VALUES (?, ?, ?, ?, ?)", dq_catalog.catalogue_rows())


def load_raw_file(table: str, meta: dict, rt: dict) -> dict:
    """Load one file into `table`. Returns {status, rows_loaded, rows_in_file, batch_id}."""
    with warehouse(rt["warehouse"]) as con:
        prepare_warehouse(con)
        seen = con.execute(
            "SELECT batch_id FROM raw_file_registry WHERE table_name = ? AND file_sha256 = ?", [table, meta["sha256"]]
        ).fetchone()
        if seen:
            print(f"[Load raw] {meta['file']} already loaded as batch {seen[0]} (identical content) -> skipped, 0 rows added to {table}")
            return {"status": "skipped_unchanged", "rows_loaded": 0, "rows_in_file": meta["rows"], "batch_id": seen[0],
                    "file": meta["file"]}

        con.execute("BEGIN TRANSACTION")
        try:
            batch_id = con.execute("SELECT coalesce(max(batch_id), 0) + 1 FROM raw_file_registry").fetchone()[0]
            run_sql_file(
                con, "raw_load.sql",
                {"path": meta["path"], "source_file": meta["file"], "run_id": rt["run_id"], "batch_id": batch_id},
                fmt={"table": table},
            )
            loaded = con.execute(f"SELECT count(*) FROM {table} WHERE _batch_id = ?", [batch_id]).fetchone()[0]
            con.execute(
                "INSERT INTO raw_file_registry VALUES (?, ?, ?, ?, ?, ?, current_timestamp::TIMESTAMP)",
                [batch_id, table, meta["file"], meta["sha256"], loaded, rt["run_id"]],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    print(f"[Load raw] {meta['file']} -> {table}: {loaded} rows appended as batch {batch_id}")
    return {"status": "loaded", "rows_loaded": int(loaded), "rows_in_file": meta["rows"], "batch_id": int(batch_id),
            "file": meta["file"]}
