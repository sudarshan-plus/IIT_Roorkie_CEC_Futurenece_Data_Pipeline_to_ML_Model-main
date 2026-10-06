"""
include/nammamart/elt/extract.py
--------------------------------
ELT step 1 - EXTRACT. Locate each source file and describe it - and do nothing else: no parsing, no cleaning.

For every file this returns {path, file, sha256, rows}. The schema contract (header must match the agreed
columns) is checked here so a renamed column stops the run before anything is loaded. The content hash is what
lets the raw loader skip a file it has already loaded (re-runs never duplicate raw rows).
"""

from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import duckdb

from nammamart.config import EXPECTED_COLUMNS, INCREMENT_GLOB, SOURCE_FILES
from nammamart.etl.extract import SchemaContractError


def _describe(path: Path, contract: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        header = next(csv.reader(handle), [])
    expected = EXPECTED_COLUMNS[contract]
    missing = [c for c in expected if c not in header]
    unexpected = [c for c in header if c not in expected]
    if missing or unexpected:
        raise SchemaContractError(
            f"{path.name}: schema changed. Missing columns: {missing or 'none'}; unexpected columns: {unexpected or 'none'}"
        )
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    con = duckdb.connect()  # in-memory: counting rows must not touch (or lock) the warehouse file
    rows = con.execute(
        "SELECT count(*) FROM read_csv($p, all_varchar = true, header = true, nullstr = '<<NULL>>')", {"p": str(path)}
    ).fetchone()[0]
    con.close()
    return {"path": str(path), "file": path.name, "sha256": digest, "rows": int(rows)}


def extract_source(kind: str, rt: dict) -> dict:
    data_dir = Path(rt["data_dir"])
    meta = _describe(data_dir / SOURCE_FILES[kind], kind)
    meta.update({"kind": kind, "table": f"raw_{kind}", "increments": []})
    if kind == "orders" and rt.get("load_increments", True):
        meta["increments"] = [_describe(p, "increment") for p in sorted(data_dir.glob(INCREMENT_GLOB))]
    extra = f" + {len(meta['increments'])} increment file(s) ({sum(i['rows'] for i in meta['increments'])} rows)" if meta["increments"] else ""
    print(f"[Extract:{kind}] {meta['file']}: {meta['rows']} rows, sha256 {meta['sha256'][:12]}...{extra}")
    return meta
