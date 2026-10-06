"""
The defects found while profiling (Part 0) must be caught by the rulebook - with exactly these counts.
If a rule is changed by mistake, this is the test that notices.
"""

from nammamart.config import ETL_DAG_ID

# check -> rows failed on the 1-30 September files (as profiled in include/profiling/profile_data.py)
EXPECTED_FAILURES = {
    # orders.csv
    "ORD_ORDER_ID_UNIQUE": 12, "ORD_CUSTOMER_ID_NOT_NULL": 18, "ORD_ORDER_TS_NOT_NULL": 1, "ORD_DELIVERED_HAS_MINUTES": 8,
    "ORD_STATUS_VALID": 3, "ORD_STATUS_CASING": 2, "ORD_PAYMENT_MODE_VALID": 2, "ORD_PAYMENT_MODE_STANDARDISED": 8,
    "ORD_ORDER_TS_VALID": 5, "ORD_DISCOUNT_RANGE": 3, "ORD_QUANTITY_POSITIVE": 10, "ORD_AMOUNT_MATCHES_FORMULA": 18,
    "ORD_DELIVERY_MINUTES_MAX": 6, "ORD_PRODUCT_EXISTS": 9, "ORD_STORE_EXISTS": 5, "ORD_ORDER_TS_NOT_FUTURE": 7,
    # payments.csv
    "PAY_PAYMENT_ID_UNIQUE": 4, "PAY_MODE_VALID": 2, "PAY_MODE_STANDARDISED": 8, "PAY_AMOUNT_POSITIVE": 10,
    "PAY_ORDER_EXISTS": 8, "PAY_AMOUNT_MATCHES_ORDER": 14, "PAY_TS_NOT_FUTURE": 7,
    # customers.csv
    "CUS_CUSTOMER_ID_UNIQUE": 5, "CUS_CITY_NOT_NULL": 6, "CUS_EMAIL_FORMAT": 12, "CUS_PHONE_10_DIGITS": 7,
    "CUS_PINCODE_6_DIGITS": 3, "CUS_LOYALTY_TIER_CASING": 8, "CUS_SIGNUP_DATE_NOT_FUTURE": 3,
    # products.csv
    "PRD_PRODUCT_ID_UNIQUE": 2, "PRD_CATEGORY_NOT_NULL": 3, "PRD_MRP_POSITIVE": 1, "PRD_COST_LE_MRP": 4,
    # stores.csv
    "STO_ZONE_NOT_NULL": 1, "STO_CAPACITY_POSITIVE": 1,
    # batch level (as of 2026-10-06 the newest order is 6 days old)
    "BATCH_ORDERS_FRESH": 1,
}


def test_every_planted_defect_is_caught(base_run):
    con = base_run.etl()
    got = dict(con.execute(
        "SELECT check_name, rows_failed FROM dq_results WHERE dag_id = ? AND rows_failed > 0", [ETL_DAG_ID]).fetchall())
    assert got == EXPECTED_FAILURES


def test_issues_exist_in_every_data_file(base_run):
    """Acceptance: problems were found in every one of the five files (Part 0 / Part D)."""
    con = base_run.etl()
    prefixes = dict(con.execute(
        "SELECT substr(check_name, 1, 3), sum(rows_failed) FROM dq_results WHERE dag_id = ? GROUP BY 1", [ETL_DAG_ID]).fetchall())
    for prefix in ("ORD", "PAY", "CUS", "PRD", "STO"):
        assert prefixes[prefix] > 0, f"no issue detected in {prefix}"


def test_all_six_dimensions_are_checked_with_both_severities(base_run):
    con = base_run.etl()
    dims = {r[0] for r in con.execute("SELECT DISTINCT dimension FROM dq_results WHERE dag_id = ?", [ETL_DAG_ID]).fetchall()}
    assert dims == {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"}
    sev = {r[0] for r in con.execute("SELECT DISTINCT severity FROM dq_results WHERE dag_id = ?", [ETL_DAG_ID]).fetchall()}
    assert sev == {"Critical", "Warning"}


def test_total_rejected_rows(base_run):
    r = base_run.results["run1"]
    assert r["etl"]["quarantine"]["rows_quarantined"] == 142      # 104 orders + 31 payments + 5 customers + 2 products
    assert r["elt"]["rows_rejected"] == 142
