"""
include/nammamart/warehouse.py
------------------------------
DuckDB access for both pipelines.

DuckDB allows ONE writing process per database file. Airflow runs parallel tasks as separate processes
(and both DAGs are scheduled at 09:00), so two tasks can collide on the file lock. `connect()` therefore
retries with a short back-off instead of failing the task, which makes parallel tasks queue up politely.
"""

from __future__ import annotations

import contextlib
import random
import re
import time
from pathlib import Path

import duckdb

from nammamart.config import SQL_DIR

_LOCK_MARKERS = ("could not set lock", "conflicting lock", "database is locked", "already open")


def connect(path: str | Path, read_only: bool = False, timeout_s: float = 180.0) -> duckdb.DuckDBPyConnection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_s
    attempt = 0
    while True:
        try:
            return duckdb.connect(str(path), read_only=read_only)
        except duckdb.IOException as exc:
            locked = any(marker in str(exc).lower() for marker in _LOCK_MARKERS)
            if not locked or time.monotonic() > deadline:
                raise
            attempt += 1
            time.sleep(min(0.2 * 2 ** min(attempt, 5), 4.0) + random.random() * 0.3)


@contextlib.contextmanager
def warehouse(path: str | Path, read_only: bool = False):
    con = connect(path, read_only=read_only)
    try:
        yield con
    finally:
        con.close()


def run_sql_file(con: duckdb.DuckDBPyConnection, name: str, params: dict | None = None, fmt: dict | None = None):
    """
    Execute every statement of include/sql/<name> in order and return the rows of the LAST statement
    (or [] when it returns nothing). `$name` placeholders are bound from `params`; unused params are ignored.
    `fmt` fills `{table}`-style template slots (only for the few files written as templates, e.g. raw_load.sql).
    """
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    if fmt:
        sql = sql.format(**{k: safe_identifier(v) for k, v in fmt.items()})
    params = params or {}
    rows: list = []
    for statement in con.extract_statements(sql):
        wanted = {k: v for k, v in params.items() if k in statement.named_parameters}
        cursor = con.execute(statement, wanted) if wanted else con.execute(statement)
        try:
            rows = cursor.fetchall()
        except duckdb.Error:
            rows = []
    return rows


def ensure_core_tables(con: duckdb.DuckDBPyConnection) -> None:
    run_sql_file(con, "audit_tables.sql")


def table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    return con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
    ).fetchone()[0] > 0


def sql_literal(value: str) -> str:
    """Quote a string for embedding in SQL (only used for file paths from our own config)."""
    return "'" + str(value).replace("'", "''") + "'"


def safe_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"unsafe SQL identifier: {name!r}")
    return name
