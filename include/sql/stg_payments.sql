-- include/sql/stg_payments.sql
-- STAGING for payments: SQL only. Newest raw snapshot of the gateway export ->
--   * standardise payment_mode / payment_status, parse timestamp and amount
--   * dedupe on payment_id (extra copies flagged FAIL)
--   * quality rules incl. "payment belongs to an order" and "SUCCESS amount = order_amount" (needs stg_orders)
-- Payments that fail a check stay visible here and in the reconciliation mart: the gateway is the source of truth
-- for money received, so anomalies must be shown, not hidden.
-- Parameters: $as_of.

CREATE OR REPLACE TABLE stg_payments AS
WITH cur AS (
    SELECT * FROM raw_payments WHERE _batch_id = (SELECT max(_batch_id) FROM raw_payments)
),
t AS (
    SELECT cur.*,
           trim(payment_id) AS pay_id,
           trim(order_id)   AS oid,
           upper(trim(payment_status)) AS status_u,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' END AS mode_std,
           CASE WHEN regexp_full_match(trim(payment_ts), '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
                THEN try_strptime(trim(payment_ts), '%Y-%m-%d %H:%M:%S') END AS ts,
           CASE WHEN regexp_full_match(trim(amount_paid), '[+-]?\d+(\.\d+)?') THEN CAST(trim(amount_paid) AS DOUBLE) END AS amount,
           row_number() OVER (
               PARTITION BY CASE WHEN trim(payment_id) = '' THEN '#' || _source_row_num ELSE trim(payment_id) END
               ORDER BY _source_row_num) AS rn
    FROM cur
),
j AS (
    SELECT t.*, CAST(o.order_amount AS DOUBLE) AS expected_amount
    FROM t
    LEFT JOIN stg_orders o ON o.order_id = t.oid
),
f AS (
    SELECT j.*,
           list_filter([
               CASE WHEN pay_id = '' THEN 'PAY_PAYMENT_ID_NOT_NULL' END,
               CASE WHEN oid = ''    THEN 'PAY_ORDER_ID_NOT_NULL' END,
               CASE WHEN rn > 1      THEN 'PAY_PAYMENT_ID_UNIQUE' END,
               CASE WHEN status_u NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'PAY_STATUS_VALID' END,
               CASE WHEN mode_std IS NULL THEN 'PAY_MODE_VALID' END,
               CASE WHEN ts IS NULL THEN 'PAY_TS_VALID' END,
               CASE WHEN amount IS NULL THEN 'PAY_AMOUNT_NUMERIC' END,
               CASE WHEN amount IS NOT NULL AND amount <= 0 THEN 'PAY_AMOUNT_POSITIVE' END,
               CASE WHEN oid <> '' AND oid NOT IN (SELECT order_id FROM stg_orders WHERE order_id <> '') THEN 'PAY_ORDER_EXISTS' END,
               CASE WHEN ts IS NOT NULL AND ts > CAST($as_of AS TIMESTAMP) THEN 'PAY_TS_NOT_FUTURE' END
           ], x -> x IS NOT NULL) AS dq_critical,
           list_filter([
               CASE WHEN trim(gateway_ref) = '' AND coalesce(mode_std, '') <> 'Cash on Delivery' THEN 'PAY_GATEWAY_REF_PRESENT' END,
               CASE WHEN mode_std IS NOT NULL AND payment_mode <> mode_std THEN 'PAY_MODE_STANDARDISED' END,
               CASE WHEN status_u = 'SUCCESS' AND amount IS NOT NULL AND expected_amount IS NOT NULL
                         AND abs(amount - expected_amount) > 0.011 THEN 'PAY_AMOUNT_MATCHES_ORDER' END
           ], x -> x IS NOT NULL) AS dq_warnings
    FROM j
)
SELECT
    pay_id AS payment_id,
    oid AS order_id,
    ts AS payment_ts,
    mode_std AS payment_mode,
    TRY_CAST(amount AS DECIMAL(12, 2)) AS amount_paid,
    status_u AS payment_status,
    trim(gateway_ref) AS gateway_ref,
    dq_critical,
    dq_warnings,
    CASE WHEN len(dq_critical) > 0 THEN 'FAIL' WHEN len(dq_warnings) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    NULLIF(array_to_string(dq_critical, '; '), '') AS failure_reason,
    NULLIF(array_to_string(dq_warnings, '; '), '') AS warning_reason,
    rn > 1 AS is_duplicate_copy,
    _batch_id, _source_file, _source_row_num, _run_id, _loaded_at
FROM f;
