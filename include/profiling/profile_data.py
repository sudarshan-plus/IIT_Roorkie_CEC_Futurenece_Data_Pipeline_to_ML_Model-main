"""
include/profiling/profile_data.py
---------------------------------
Part 0 - Discovery and data profiling.

Reads the six NammaMart CSVs exactly as received (everything as text) and prints:
  1. row counts, nulls, duplicate keys, min/max, distinct categories
  2. referential integrity across the four relationships
  3. business-rule checks (order_amount formula, status/mode lists, delivery, payments)

Run:  python include/profiling/profile_data.py [data_dir]
Nothing here writes to the warehouse; it is read-only discovery.
"""

import sys
from pathlib import Path

import pandas as pd

DATA_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / "data" / "nammamart"

FILES = {
    "orders": "orders.csv",
    "payments": "payments.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "stores": "stores.csv",
    "increment": "orders_2026-10-01_increment.csv",
}
KEYS = {
    "orders": "order_id", "payments": "payment_id", "customers": "customer_id",
    "products": "product_id", "stores": "store_id", "increment": "order_id",
}
NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "increment": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "products": ["mrp", "cost_price"],
    "stores": ["capacity_orders_per_day"],
    "customers": [],
}
DATES = {
    "orders": ["order_ts"], "increment": ["order_ts"], "payments": ["payment_ts"],
    "customers": ["signup_date"], "products": ["launch_date"], "stores": ["open_date"],
}
CATEGORICAL = {
    "orders": ["order_status", "payment_mode", "channel"],
    "increment": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_status", "payment_mode"],
    "customers": ["loyalty_tier", "is_active", "city"],
    "products": ["category", "sub_category", "unit", "is_perishable"],
    "stores": ["zone"],
}


def load(name):
    return pd.read_csv(DATA_DIR / FILES[name], dtype=str, keep_default_na=False)


def section(title):
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def profile_file(name, df):
    section(f"FILE: {FILES[name]}  rows={len(df)}  cols={len(df.columns)}")
    # a "null" is an empty string or whitespace-only in the raw text
    blank = df.apply(lambda c: c.str.strip().eq(""))
    print("Nulls / blanks per column:")
    print(blank.sum()[blank.sum() > 0].to_string() or "  none")

    key = KEYS[name]
    dup_keys = df[df[key].str.strip().duplicated(keep=False) & df[key].str.strip().ne("")]
    print(f"\nDuplicate {key} values: {dup_keys[key].nunique()} keys across {len(dup_keys)} rows")
    full_dups = df.duplicated(keep=False).sum()
    print(f"Fully identical duplicate rows: {full_dups}")

    ws = {c: int((df[c] != df[c].str.strip()).sum()) for c in df.columns}
    ws = {c: n for c, n in ws.items() if n}
    print(f"Columns with leading/trailing whitespace: {ws or 'none'}")

    for c in NUMERIC[name]:
        s = pd.to_numeric(df[c].replace("", pd.NA), errors="coerce")
        bad_text = int((df[c].str.strip().ne("") & s.isna()).sum())
        print(f"  {c:<22} min={s.min()}  max={s.max()}  mean={s.mean():.2f}  non-numeric-text={bad_text}  <=0: {(s <= 0).sum()}")
    for c in DATES[name]:
        s = pd.to_datetime(df[c].replace("", pd.NA), errors="coerce", format="mixed")
        bad = int((df[c].str.strip().ne("") & s.isna()).sum())
        print(f"  {c:<22} min={s.min()}  max={s.max()}  unparseable={bad}")
    for c in CATEGORICAL[name]:
        print(f"  distinct {c}: {dict(df[c].value_counts(dropna=False))}")


def main():
    d = {n: load(n) for n in FILES}
    for n, df in d.items():
        profile_file(n, df)

    section("EXTRA PROFILE: customers / products / stores")
    c, p, s = d["customers"], d["products"], d["stores"]
    print("email not matching a@b.c:", int((~c["email"].str.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")).sum() - (c["email"].str.strip() == "").sum()))
    print("phone not 10 digits:", int((~c["phone"].str.match(r"^\d{10}$")).sum()))
    print("pincode not 6 digits:", int((~c["pincode"].str.match(r"^\d{6}$")).sum()))
    print("duplicate email values:", int(c["email"].str.lower().duplicated(keep=False).sum()))
    print("products cost_price > mrp:", int((pd.to_numeric(p["cost_price"], errors="coerce") > pd.to_numeric(p["mrp"], errors="coerce")).sum()))
    print("stores pincode not 6 digits:", int((~s["pincode"].str.match(r"^\d{6}$")).sum()))

    section("REFERENTIAL INTEGRITY")
    o, pay, i = d["orders"], d["payments"], d["increment"]
    cust, prod, stor = set(c["customer_id"].str.strip()), set(p["product_id"].str.strip()), set(s["store_id"].str.strip())
    for label, frame in (("orders", o), ("increment", i)):
        print(f"{label}.customer_id not in customers: {int((~frame['customer_id'].str.strip().isin(cust)).sum())} (blank: {int((frame['customer_id'].str.strip() == '').sum())})")
        print(f"{label}.product_id  not in products : {int((~frame['product_id'].str.strip().isin(prod)).sum())}")
        print(f"{label}.store_id    not in stores   : {int((~frame['store_id'].str.strip().isin(stor)).sum())}")
    order_ids = set(o["order_id"].str.strip())
    orphan = pay[~pay["order_id"].str.strip().isin(order_ids)]
    print(f"payments.order_id not in orders: {len(orphan)}")
    cancelled = set(o.loc[o["order_status"].str.strip().str.upper() == "CANCELLED", "order_id"].str.strip())
    print(f"payments against CANCELLED orders: {int(pay['order_id'].str.strip().isin(cancelled).sum())}")
    delivered = o[o["order_status"].str.strip().str.upper() == "DELIVERED"]
    paid = set(pay["order_id"].str.strip())
    print(f"DELIVERED orders with no payment row: {int((~delivered['order_id'].str.strip().isin(paid)).sum())}")
    print(f"orders with >1 payment row: {int((pay.groupby(pay['order_id'].str.strip()).size() > 1).sum())}")

    section("BUSINESS RULES")
    for label, frame in (("orders", o), ("increment", i)):
        q = pd.to_numeric(frame["quantity"], errors="coerce")
        up = pd.to_numeric(frame["unit_price"], errors="coerce")
        dp = pd.to_numeric(frame["discount_pct"], errors="coerce")
        amt = pd.to_numeric(frame["order_amount"], errors="coerce")
        calc = (q * up * (1 - dp / 100)).round(2)
        mism = (calc - amt).abs() > 0.01
        print(f"{label}: order_amount != formula: {int(mism.sum())}")
        print(f"{label}: quantity <= 0: {int((q <= 0).sum())}   discount outside 0-100: {int(((dp < 0) | (dp > 100)).sum())}")
        st = frame["order_status"].str.strip().str.upper()
        dm = pd.to_numeric(frame["delivery_minutes"], errors="coerce")
        print(f"{label}: DELIVERED with no delivery_minutes: {int(((st == 'DELIVERED') & dm.isna()).sum())}")
        print(f"{label}: delivery_minutes > 120: {int((dm > 120).sum())}   <8 or >35: {int(((dm < 8) | (dm > 35)).sum())}   non-delivered with minutes: {int(((st != 'DELIVERED') & dm.notna()).sum())}")
        ts = pd.to_datetime(frame["order_ts"], errors="coerce", format="mixed")
        print(f"{label}: future order_ts (> 2026-10-06): {int((ts > pd.Timestamp('2026-10-06')).sum())}")

    # payment amount vs order amount
    o2 = o.assign(order_id=o["order_id"].str.strip()).drop_duplicates("order_id")
    m = pay.assign(order_id=pay["order_id"].str.strip()).merge(o2[["order_id", "order_amount", "payment_mode", "order_status"]], on="order_id", how="left", suffixes=("_pay", "_ord"))
    ok = m["payment_status"].str.strip().str.upper() == "SUCCESS"
    diff = (pd.to_numeric(m["amount_paid"], errors="coerce") - pd.to_numeric(m["order_amount"], errors="coerce")).abs() > 0.01
    print(f"payments SUCCESS with amount != order_amount: {int((ok & diff).sum())}")
    modemis = m["payment_mode_pay"].str.strip().str.upper() != m["payment_mode_ord"].str.strip().str.upper()
    print(f"payments whose mode differs from order mode: {int(modemis.sum())}")

    section("INCREMENT vs ORIGINAL")
    inc_ids = i["order_id"].str.strip()
    print(f"increment rows whose order_id exists in orders (corrections): {int(inc_ids.isin(order_ids).sum())}")
    print(f"increment rows with new order_id: {int((~inc_ids.isin(order_ids)).sum())}")
    corr = i[inc_ids.isin(order_ids)].merge(o2, on="order_id", suffixes=("_inc", "_orig"))
    print(corr[["order_id", "order_status_orig", "order_status_inc", "order_amount_orig", "order_amount_inc"]].to_string(index=False))


if __name__ == "__main__":
    main()
