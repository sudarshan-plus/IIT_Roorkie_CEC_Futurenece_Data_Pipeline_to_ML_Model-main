"""
submission/verify_pii_masking.py
--------------------------------
Proof for acceptance criterion 8 ("customer personal data is protected: email and phone masked in every reporting table").

  1. Column scan - which tables even HAVE a column called email / phone / full_name? Only the restricted raw layer may.
  2. Value scan  - for EVERY text column of EVERY non-raw table, count rows whose value equals a real email, phone number
                   or customer name taken from raw_customers. Must be 0 everywhere.

    python submission/verify_pii_masking.py --warehouse include/warehouse_sudarshan.duckdb
Exit code 1 if any leak is found, so it can run in CI.
"""

from __future__ import annotations

import argparse
import sys

import duckdb

PII_COLUMN_WORDS = ("email", "phone", "full_name", "customer_name")
RESTRICTED = ("raw_customers",)          # the only table allowed to hold the real values


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse", required=True)
    args = parser.parse_args()
    con = duckdb.connect(args.warehouse, read_only=True)

    print("## 1. Column scan - tables with a PII-looking column name\n")
    rows = con.execute(
        "SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'main' ORDER BY 1, 2").fetchall()
    print("| table | column | verdict |\n|---|---|---|")
    bad_names = 0
    for table, column in rows:
        if not any(w in column.lower() for w in PII_COLUMN_WORDS):
            continue
        masked = column.endswith("_masked")
        ok = masked or table in RESTRICTED
        bad_names += not ok
        verdict = ("masked column" if masked else
                   "restricted raw layer (by design)" if table in RESTRICTED else "**UNMASKED NAME IN A REPORTING TABLE**")
        print(f"| {table} | {column} | {verdict} |")

    print("\n## 2. Value scan - real email / phone / name found in any non-raw table\n")
    con.execute("CREATE TEMP TABLE pii_contact AS SELECT DISTINCT trim(v) AS v FROM ("
                "SELECT email AS v FROM raw_customers UNION SELECT phone FROM raw_customers) WHERE trim(v) NOT IN ('', 'N/A')")
    con.execute("CREATE TEMP TABLE pii_name AS SELECT DISTINCT trim(full_name) AS v FROM raw_customers "
                "WHERE trim(full_name) NOT IN ('', 'N/A')")
    n_contact = con.execute("SELECT count(*) FROM pii_contact").fetchone()[0]
    n_name = con.execute("SELECT count(*) FROM pii_name").fetchone()[0]
    print(f"Searched every text column of every non-raw table for {n_contact} real emails/phones and {n_name} real customer names "
          "from `raw_customers`. (`stores.manager_name` is an *employee* - a different person; a few managers share a common name "
          "with a customer in this synthetic data, so that column is excluded from the name scan.)\n")
    print("| table | text columns scanned | rows with a real email/phone | rows with a real customer name |\n|---|---|---|---|")
    leaks = 0
    for (table,) in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY 1").fetchall():
        if table.startswith("raw_") or table.startswith("pii_"):
            continue                                   # raw_* are exact copies of the CSVs: the restricted layer
        cols = [c for (c,) in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? "
            "AND data_type = 'VARCHAR' ORDER BY ordinal_position", [table]).fetchall()]
        if not cols:
            continue
        contact = sum(con.execute(f'SELECT count(*) FROM "{table}" WHERE trim("{c}") IN (SELECT v FROM pii_contact)').fetchone()[0]
                      for c in cols)
        names = sum(con.execute(f'SELECT count(*) FROM "{table}" WHERE trim("{c}") IN (SELECT v FROM pii_name)').fetchone()[0]
                    for c in cols if c != "manager_name")
        leaks += contact + names
        print(f"| {table} | {len(cols)} | {contact} | {names} |")

    ok = leaks == 0 and bad_names == 0
    verdict = "PASS - no real customer email, phone or name outside the restricted raw layer" if ok else "FAIL"
    print(f"\n**Result: {verdict}** ({bad_names} unmasked column names, {leaks} leaked values)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
