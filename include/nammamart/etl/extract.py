"""
include/nammamart/etl/extract.py
--------------------------------
ETL step 1 - EXTRACT. One call per source file (the DAG runs five of these in parallel).

Reads the CSV exactly as text (nothing is converted, `N/A` stays `N/A`), checks the header against the
schema contract, tags every row with its source file and row number (lineage), and stages it as a CSV in
the run's work folder for the next task. Failing here, loudly, is deliberate: a missing file or a renamed
column must stop the run rather than load wrong data.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from nammamart.config import EXPECTED_COLUMNS, INCREMENT_GLOB, SOURCE_FILES


class SchemaContractError(RuntimeError):
    """The file's header does not match the agreed columns."""


def work_path(rt: dict) -> Path:
    path = Path(rt["work_dir"]) / rt["safe_run_id"]
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_source(path: Path, contract: str, file_seq: int = 0) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Source file not found: {path}")
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    expected = EXPECTED_COLUMNS[contract]
    missing = [c for c in expected if c not in df.columns]
    unexpected = [c for c in df.columns if c not in expected]
    if missing or unexpected:
        raise SchemaContractError(
            f"{path.name}: schema changed. Missing columns: {missing or 'none'}; unexpected columns: {unexpected or 'none'}"
        )
    df = df[expected].copy()
    df["_source_file"] = path.name
    df["_source_row_num"] = range(1, len(df) + 1)
    df["_file_seq"] = file_seq
    return df


def extract_source(kind: str, rt: dict) -> dict:
    """Extract one logical file ('orders', 'payments', 'customers', 'products', 'stores')."""
    data_dir = Path(rt["data_dir"])
    base = data_dir / SOURCE_FILES[kind]
    frames = [read_source(base, kind)]
    increments = []

    if kind == "orders" and rt.get("load_increments", True):
        for seq, inc in enumerate(sorted(data_dir.glob(INCREMENT_GLOB)), start=1):
            frame = read_source(inc, "increment", file_seq=seq)
            frames.append(frame)
            increments.append({"file": inc.name, "rows": len(frame)})

    out = pd.concat(frames, ignore_index=True)
    target = work_path(rt) / f"extract_{kind}.csv"
    out.to_csv(target, index=False)
    stats = {
        "kind": kind,
        "file": base.name,
        "rows": len(frames[0]),
        "increments": increments,
        "rows_total": len(out),
    }
    print(f"[Extract:{kind}] {stats['rows']} rows from {base.name}"
          + (f" + {sum(i['rows'] for i in increments)} increment rows from {len(increments)} file(s)" if increments else ""))
    return stats
