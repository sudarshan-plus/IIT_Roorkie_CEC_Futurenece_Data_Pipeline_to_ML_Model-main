"""
experiments/benchmark_scale.py
------------------------------
Part G evidence: how do the two pipelines behave as the data grows? Replicates the September orders/payments N times
(unique ids, same defect rate), runs the ETL and the ELT end to end in a FRESH process for every (pipeline, scale),
and records wall time per stage and peak memory. No Airflow involved - this measures the transformation work itself.

    python experiments/benchmark_scale.py --scales 1,10,100,300 --work ~/nm_bench
Results: experiments/results/benchmark_scale.json   (the 1,000x figure is an extrapolation of the measured trend)
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))
SRC = ROOT / "include" / "data" / "nammamart"
RESULTS = ROOT / "experiments" / "results"


def build_data(folder: Path, scale: int) -> dict:
    import pandas as pd

    folder.mkdir(parents=True, exist_ok=True)
    for name in ("customers.csv", "products.csv", "stores.csv"):
        shutil.copy2(SRC / name, folder / name)
    orders = pd.read_csv(SRC / "orders.csv", dtype=str, keep_default_na=False)
    payments = pd.read_csv(SRC / "payments.csv", dtype=str, keep_default_na=False)
    parts_o, parts_p = [], []
    for k in range(scale):
        o, p = orders.copy(), payments.copy()
        if k:
            o["order_id"] = o["order_id"].where(o["order_id"] == "", o["order_id"] + f"-{k}")
            p["order_id"] = p["order_id"].where(p["order_id"] == "", p["order_id"] + f"-{k}")
            p["payment_id"] = p["payment_id"].where(p["payment_id"] == "", p["payment_id"] + f"-{k}")
        parts_o.append(o)
        parts_p.append(p)
    pd.concat(parts_o, ignore_index=True).to_csv(folder / "orders.csv", index=False)
    pd.concat(parts_p, ignore_index=True).to_csv(folder / "payments.csv", index=False)
    return {"orders": len(orders) * scale, "payments": len(payments) * scale}


def child(pipeline: str, folder: str, warehouse: str, work: str) -> None:
    """Run one pipeline once and print its timings as JSON (peak memory is this process's own)."""
    from nammamart.config import resolve_runtime

    rt = resolve_runtime({"data_dir": folder, "warehouse_path": warehouse, "work_dir": work + "/work",
                          "quarantine_dir": work + "/quarantine", "as_of": "2026-10-06 09:00:00",
                          "load_increments": False}, "bench")
    stages: dict[str, float] = {}

    def timed(label, fn, *a, **k):
        t0 = time.perf_counter()
        with contextlib.redirect_stdout(io.StringIO()):
            out = fn(*a, **k)
        stages[label] = round(time.perf_counter() - t0, 2)
        return out

    t_all = time.perf_counter()
    if pipeline == "etl":
        from nammamart.etl import extract, load, quarantine, reconcile, report, tasks, transform
        for kind in ("orders", "payments", "customers", "products", "stores"):
            timed(f"extract_{kind}", extract.extract_source, kind, rt)
        v = [timed(f"validate_{k}", tasks.validate_dimension, k, rt) for k in ("customers", "products", "stores")]
        v.append(timed("validate_orders", tasks.validate_orders, rt))
        v.append(timed("validate_payments", tasks.validate_payments, rt))
        timed("dq_results", tasks.log_dq_results, rt, v)
        timed("quarantine", quarantine.quarantine_failed_rows, rt)
        timed("transform", transform.transform, rt)
        timed("load", load.load_warehouse, rt)
        timed("reconcile", reconcile.reconcile_payments, rt)
        timed("report", report.build_reports, rt)
    else:
        from nammamart.elt import extract, tasks
        meta = {k: timed(f"extract_{k}", extract.extract_source, k, rt) for k in ("orders", "payments", "customers", "products", "stores")}
        for k, m in meta.items():
            timed(f"load_raw_{k}", tasks.load_raw_table, k, m, rt)
        for k in ("customers", "products", "stores"):
            timed(f"stg_{k}", tasks.build_staging, k, rt)
        timed("stg_orders", tasks.build_staging_orders, rt)
        timed("stg_payments", tasks.build_staging, "payments", rt)
        timed("dq_results", tasks.record_dq_results, rt, meta["orders"]["rows"])
        for m in tasks.MARTS:
            timed(f"mart_{m}", tasks.build_mart, m, rt)
    total = round(time.perf_counter() - t_all, 2)
    peak_mb = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 0)   # Linux: kilobytes
    print("RESULT " + json.dumps({"pipeline": pipeline, "total_s": total, "peak_mb": peak_mb, "stages": stages}))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scales", default="1,10,100")
    parser.add_argument("--work", default=str(Path.home() / "nm_bench"))
    parser.add_argument("--child", nargs=4, metavar=("PIPELINE", "FOLDER", "WAREHOUSE", "WORK"))
    args = parser.parse_args()
    if args.child:
        child(*args.child)
        return

    work = Path(args.work).expanduser()
    results = []
    for scale in [int(s) for s in args.scales.split(",")]:
        folder = work / f"data_x{scale}"
        rows = build_data(folder, scale)
        for pipeline in ("etl", "elt"):
            wh = work / f"wh_{pipeline}_x{scale}.duckdb"
            for leftover in work.glob(f"wh_{pipeline}_x{scale}.duckdb*"):
                leftover.unlink()
            shutil.rmtree(work / f"run_{pipeline}_x{scale}", ignore_errors=True)
            cmd = [sys.executable, __file__, "--child", pipeline, str(folder), str(wh), str(work / f"run_{pipeline}_x{scale}")]
            out = subprocess.run(cmd, capture_output=True, text=True)
            line = next((l for l in out.stdout.splitlines() if l.startswith("RESULT ")), None)
            if line is None:
                print(f"x{scale} {pipeline}: FAILED\n{out.stderr[-600:]}")
                results.append({"scale": scale, "pipeline": pipeline, **rows, "failed": out.stderr[-300:]})
                continue
            res = json.loads(line[7:])
            res.update({"scale": scale, **rows})
            results.append(res)
            print(f"x{scale:<4} {pipeline}: {rows['orders']:>9,} orders | {res['total_s']:>8.1f}s | peak {res['peak_mb']:>6.0f} MB")
            shutil.rmtree(work / f"run_{pipeline}_x{scale}", ignore_errors=True)
            for f in work.glob(f"wh_{pipeline}_x{scale}.duckdb*"):
                f.unlink()
        shutil.rmtree(folder, ignore_errors=True)
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "benchmark_scale.json").write_text(json.dumps(results, indent=2))
    print("-> experiments/results/benchmark_scale.json")


if __name__ == "__main__":
    main()
