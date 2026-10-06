"""
include/nammamart/dq_catalog.py
-------------------------------
The data-quality rulebook: ONE catalogue, implemented twice (pandas in the ETL DAG, SQL in the ELT DAG).

  Critical -> the row is wrong or unusable: quarantined (ETL) / dq_status = 'FAIL' (ELT). Never reaches reporting.
  Warning  -> the row is usable but something is off: loaded, flagged and counted in dq_results.

Severity principle (see SUBMISSION.md, Part D):
  * Fact rows (orders, payments) follow the suggested severities.
  * Dimension rows are quarantined only for KEY problems (blank key, duplicate key). A blank/odd descriptive
    attribute is a Warning, because quarantining a store, product or customer would orphan or silently delete
    valid orders (e.g. store S07 has a blank zone: rejecting it would remove 7.7% of delivered revenue).

The order of this list is the order in which reasons are written in `failure_reason`.
"""

from __future__ import annotations

from dataclasses import dataclass

CRITICAL = "Critical"
WARNING = "Warning"

COMPLETENESS = "Completeness"
UNIQUENESS = "Uniqueness"
VALIDITY = "Validity"
ACCURACY = "Accuracy"
CONSISTENCY = "Consistency"
TIMELINESS = "Timeliness"

DIMENSIONS = (COMPLETENESS, UNIQUENESS, VALIDITY, ACCURACY, CONSISTENCY, TIMELINESS)


@dataclass(frozen=True)
class Check:
    name: str
    table: str          # orders | payments | customers | products | stores | batch
    dimension: str
    severity: str
    description: str


C, W = CRITICAL, WARNING

CHECKS: list[Check] = [
    # ------------------------------------------------------------------ orders
    Check("ORD_ORDER_ID_NOT_NULL", "orders", COMPLETENESS, C, "order_id is present"),
    Check("ORD_CUSTOMER_ID_NOT_NULL", "orders", COMPLETENESS, C, "customer_id is present"),
    Check("ORD_ORDER_TS_NOT_NULL", "orders", COMPLETENESS, C, "order_ts is present"),
    Check("ORD_DELIVERED_HAS_MINUTES", "orders", COMPLETENESS, C, "a DELIVERED order has delivery_minutes"),
    Check("ORD_ORDER_ID_UNIQUE", "orders", UNIQUENESS, C, "order_id appears once (extra copies rejected, first kept)"),
    Check("ORD_STATUS_VALID", "orders", VALIDITY, C, "order_status is DELIVERED / CANCELLED / RETURNED"),
    Check("ORD_STATUS_CASING", "orders", VALIDITY, W, "order_status needed trimming/upper-casing (fixed in transform)"),
    Check("ORD_PAYMENT_MODE_VALID", "orders", VALIDITY, C, "payment_mode is UPI / Card / Cash on Delivery / Wallet (known aliases mapped)"),
    Check("ORD_PAYMENT_MODE_STANDARDISED", "orders", VALIDITY, W, "payment_mode spelling was mapped to the standard value"),
    Check("ORD_ORDER_TS_VALID", "orders", VALIDITY, C, "order_ts parses as YYYY-MM-DD HH:MM:SS"),
    Check("ORD_NUMBERS_VALID", "orders", VALIDITY, C, "quantity, unit_price, discount_pct, order_amount are numeric"),
    Check("ORD_DISCOUNT_RANGE", "orders", VALIDITY, C, "discount_pct is between 0 and 100"),
    Check("ORD_CHANNEL_VALID", "orders", VALIDITY, W, "channel is App or Web"),
    Check("ORD_QUANTITY_POSITIVE", "orders", ACCURACY, C, "quantity > 0"),
    Check("ORD_UNIT_PRICE_POSITIVE", "orders", ACCURACY, C, "unit_price > 0"),
    Check("ORD_AMOUNT_MATCHES_FORMULA", "orders", ACCURACY, C, "order_amount = quantity x unit_price x (1 - discount/100)"),
    Check("ORD_DELIVERY_MINUTES_MAX", "orders", ACCURACY, C, "delivery_minutes <= 120"),
    Check("ORD_DELIVERY_MINUTES_TYPICAL", "orders", ACCURACY, W, "delivery_minutes inside the normal 8-35 minute band"),
    Check("ORD_PRICE_MATCHES_CATALOGUE", "orders", ACCURACY, W, "unit_price equals the catalogue MRP of the product"),
    Check("ORD_PRICE_FAR_FROM_CATALOGUE", "orders", ACCURACY, C, "unit_price is within 3x of the catalogue MRP (a 100x price is not believable)"),
    Check("ORD_CUSTOMER_EXISTS", "orders", CONSISTENCY, C, "customer_id exists in customers"),
    Check("ORD_PRODUCT_EXISTS", "orders", CONSISTENCY, C, "product_id exists in products"),
    Check("ORD_STORE_EXISTS", "orders", CONSISTENCY, C, "store_id exists in stores"),
    Check("ORD_ORDER_TS_NOT_FUTURE", "orders", TIMELINESS, C, "order_ts is not in the future"),
    # ------------------------------------------------------------------ payments
    Check("PAY_PAYMENT_ID_NOT_NULL", "payments", COMPLETENESS, C, "payment_id is present"),
    Check("PAY_ORDER_ID_NOT_NULL", "payments", COMPLETENESS, C, "order_id is present"),
    Check("PAY_GATEWAY_REF_PRESENT", "payments", COMPLETENESS, W, "non cash-on-delivery payments carry a gateway_ref"),
    Check("PAY_PAYMENT_ID_UNIQUE", "payments", UNIQUENESS, C, "payment_id appears once (extra copies rejected, first kept)"),
    Check("PAY_STATUS_VALID", "payments", VALIDITY, C, "payment_status is SUCCESS / FAILED / REFUNDED"),
    Check("PAY_MODE_VALID", "payments", VALIDITY, C, "payment_mode is a valid mode (known aliases mapped)"),
    Check("PAY_MODE_STANDARDISED", "payments", VALIDITY, W, "payment_mode spelling was mapped to the standard value"),
    Check("PAY_TS_VALID", "payments", VALIDITY, C, "payment_ts parses as YYYY-MM-DD HH:MM:SS"),
    Check("PAY_AMOUNT_NUMERIC", "payments", VALIDITY, C, "amount_paid is numeric"),
    Check("PAY_AMOUNT_POSITIVE", "payments", ACCURACY, C, "amount_paid > 0"),
    Check("PAY_ORDER_EXISTS", "payments", CONSISTENCY, C, "payment belongs to an order that exists in orders"),
    Check("PAY_AMOUNT_MATCHES_ORDER", "payments", CONSISTENCY, W, "a SUCCESS payment equals its order_amount (detail in reconciliation)"),
    Check("PAY_TS_NOT_FUTURE", "payments", TIMELINESS, C, "payment_ts is not in the future"),
    # ------------------------------------------------------------------ customers
    Check("CUS_CUSTOMER_ID_NOT_NULL", "customers", COMPLETENESS, C, "customer_id is present"),
    Check("CUS_CITY_NOT_NULL", "customers", COMPLETENESS, W, "city is present"),
    Check("CUS_CUSTOMER_ID_UNIQUE", "customers", UNIQUENESS, C, "one row per customer_id (extra copies rejected, first kept)"),
    Check("CUS_EMAIL_FORMAT", "customers", VALIDITY, W, "email looks like name@domain.tld (invalid -> not shown)"),
    Check("CUS_PHONE_10_DIGITS", "customers", VALIDITY, W, "phone is exactly 10 digits"),
    Check("CUS_PINCODE_6_DIGITS", "customers", VALIDITY, W, "pincode is exactly 6 digits (invalid -> NULL)"),
    Check("CUS_IS_ACTIVE_VALID", "customers", VALIDITY, W, "is_active is Y or N"),
    Check("CUS_SIGNUP_DATE_VALID", "customers", VALIDITY, W, "signup_date parses as YYYY-MM-DD"),
    Check("CUS_LOYALTY_TIER_CASING", "customers", CONSISTENCY, W, "loyalty_tier uses one casing (Bronze/Silver/Gold/Platinum)"),
    Check("CUS_LOYALTY_TIER_VALID", "customers", CONSISTENCY, W, "loyalty_tier is a known tier"),
    Check("CUS_SIGNUP_DATE_NOT_FUTURE", "customers", TIMELINESS, W, "signup_date is not in the future (invalid -> NULL)"),
    # ------------------------------------------------------------------ products
    Check("PRD_PRODUCT_ID_NOT_NULL", "products", COMPLETENESS, C, "product_id is present"),
    Check("PRD_CATEGORY_NOT_NULL", "products", COMPLETENESS, W, "category is present (missing -> UNKNOWN)"),
    Check("PRD_PRODUCT_ID_UNIQUE", "products", UNIQUENESS, C, "one row per product_id (current row kept, '(old)' rows rejected)"),
    Check("PRD_LAUNCH_DATE_VALID", "products", VALIDITY, W, "launch_date parses as YYYY-MM-DD"),
    Check("PRD_MRP_POSITIVE", "products", ACCURACY, W, "mrp > 0 (invalid -> NULL)"),
    Check("PRD_COST_PRICE_POSITIVE", "products", ACCURACY, W, "cost_price > 0 (invalid -> margin is NULL)"),
    Check("PRD_COST_LE_MRP", "products", ACCURACY, W, "cost_price <= mrp"),
    Check("PRD_LAUNCH_DATE_NOT_FUTURE", "products", TIMELINESS, W, "launch_date is not in the future"),
    # ------------------------------------------------------------------ stores
    Check("STO_STORE_ID_NOT_NULL", "stores", COMPLETENESS, C, "store_id is present"),
    Check("STO_ZONE_NOT_NULL", "stores", COMPLETENESS, W, "zone is present (missing -> UNKNOWN)"),
    Check("STO_STORE_ID_UNIQUE", "stores", UNIQUENESS, C, "one row per store_id"),
    Check("STO_ZONE_VALID", "stores", VALIDITY, W, "zone is North / South / East / West"),
    Check("STO_PINCODE_6_DIGITS", "stores", VALIDITY, W, "pincode is exactly 6 digits"),
    Check("STO_CAPACITY_POSITIVE", "stores", ACCURACY, W, "capacity_orders_per_day > 0 (invalid -> NULL)"),
    Check("STO_OPEN_DATE_NOT_FUTURE", "stores", TIMELINESS, W, "open_date is not in the future"),
    # ------------------------------------------------------------------ batch level (one row per run)
    Check("BATCH_ORDERS_VOLUME", "batch", COMPLETENESS, W, "orders file volume did not drop > 90% versus the previous run"),
    Check("BATCH_ORDERS_FRESH", "batch", TIMELINESS, W, "latest valid order is not older than 1 day at run time"),
    Check("BATCH_STORE_REJECT_RATE", "batch", ACCURACY, W, "no store has > 25% of its orders rejected (price/feed drift)"),
]

BY_NAME = {c.name: c for c in CHECKS}
BY_TABLE: dict[str, list[Check]] = {}
for _c in CHECKS:
    BY_TABLE.setdefault(_c.table, []).append(_c)

assert len(BY_NAME) == len(CHECKS), "duplicate check name in catalogue"


def checks_for(table: str) -> list[Check]:
    return BY_TABLE[table]


def catalogue_rows() -> list[tuple]:
    """(check_name, source_table, dimension, severity, description) for seeding the ELT catalogue table."""
    return [(c.name, c.table, c.dimension, c.severity, c.description) for c in CHECKS]
