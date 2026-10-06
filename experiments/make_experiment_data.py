"""
experiments/make_experiment_data.py
-----------------------------------
Builds the input folders for the Part F observability experiments - as COPIES, so the original files in
include/data/nammamart are never touched (nothing to "restore" afterwards).

  base                  the five September files, no increment                     (experiments 1, 2)
  with_increment        base + orders_2026-10-01_increment.csv                      (experiments 3, 9 and the baseline for 4-10)
  exp4_missing_file     with_increment, products.csv renamed away                   (experiments 4 and 10)
  exp5_bad_rows         with_increment, orders.csv + 5 deliberately bad rows        (experiment 5)
  exp6_schema_change    with_increment, orders.csv column order_amount -> total     (experiment 6)
  exp7_volume_drop      with_increment, orders.csv cut to its first 50 rows         (experiment 7)
  exp8_price_drift      with_increment, unit_price x 100 for store S08 only         (experiment 8)
  exp8b_consistent_drift with_increment, unit_price AND order_amount x 100 for S08  (experiment 8b)

Usage:  python experiments/make_experiment_data.py [output_dir]
        (default output_dir: include/data/nammamart_experiments)
"""

from __future__ import annotations

import csv
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "include" / "data" / "nammamart"
DEFAULT_OUT = ROOT / "include" / "data" / "nammamart_experiments"
INCREMENT = "orders_2026-10-01_increment.csv"
BASE_FILES = ["orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv"]

# Experiment 5: one valid-looking order per defect, each with exactly ONE thing wrong.
BAD_ROWS = [
    ["ORD900001", "2026-09-29 10:00:00", "", "S01", "P0001", "1", "107.0", "0", "107.0", "UPI", "DELIVERED", "App", "15"],        # null customer
    ["ORD900002", "2026-09-29 10:05:00", "C00001", "S01", "P0001", "-2", "107.0", "0", "-214.0", "UPI", "DELIVERED", "App", "15"],  # negative quantity
    ["ORD900003", "2026-09-29 10:10:00", "C00001", "S99", "P0001", "1", "107.0", "0", "107.0", "UPI", "DELIVERED", "App", "15"],    # unknown store
    ["ORD900004", "2026-09-29 10:15:00", "C00001", "S01", "P0001", "1", "107.0", "0", "107.0", "UPI", "SHIPPED", "App", "15"],      # status SHIPPED
    ["ORD900005", "2027-03-01 10:20:00", "C00001", "S01", "P0001", "1", "107.0", "0", "107.0", "UPI", "DELIVERED", "App", "15"],    # 2027 timestamp
]


def read_csv(path: Path) -> tuple[list[str], list[list[str]]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.reader(handle))
    return rows[0], rows[1:]


def write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        writer.writerows(rows)


def make_folder(out: Path, name: str, with_increment: bool) -> Path:
    folder = out / name
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    for file in BASE_FILES:
        shutil.copy2(SRC / file, folder / file)
    if with_increment:
        shutil.copy2(SRC / INCREMENT, folder / INCREMENT)
    return folder


def build(out: Path = DEFAULT_OUT) -> dict[str, Path]:
    folders = {
        "base": make_folder(out, "base", with_increment=False),
        "with_increment": make_folder(out, "with_increment", with_increment=True),
    }

    # 4. missing source file
    d = make_folder(out, "exp4_missing_file", True)
    (d / "products.csv").rename(d / "products.csv.missing")
    folders["exp4_missing_file"] = d

    # 5. new bad data
    d = make_folder(out, "exp5_bad_rows", True)
    header, rows = read_csv(d / "orders.csv")
    write_csv(d / "orders.csv", header, rows + BAD_ROWS)
    folders["exp5_bad_rows"] = d

    # 6. schema change
    d = make_folder(out, "exp6_schema_change", True)
    header, rows = read_csv(d / "orders.csv")
    write_csv(d / "orders.csv", ["total" if c == "order_amount" else c for c in header], rows)
    folders["exp6_schema_change"] = d

    # 7. volume anomaly
    d = make_folder(out, "exp7_volume_drop", True)
    header, rows = read_csv(d / "orders.csv")
    write_csv(d / "orders.csv", header, rows[:50])
    folders["exp7_volume_drop"] = d

    # 8 / 8b. distribution drift for one store
    for name, scale_amount in (("exp8_price_drift", False), ("exp8b_consistent_drift", True)):
        d = make_folder(out, name, True)
        header, rows = read_csv(d / "orders.csv")
        store, price, amount = header.index("store_id"), header.index("unit_price"), header.index("order_amount")
        for row in rows:
            if row[store] == "S08":
                row[price] = f"{float(row[price]) * 100:.1f}"
                if scale_amount:
                    row[amount] = f"{float(row[amount]) * 100:.2f}"
        write_csv(d / "orders.csv", header, rows)
        folders[name] = d
    return folders


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT
    for name, path in build(target).items():
        print(f"{name:<24} {path}  ({len(list(path.iterdir()))} files)")
