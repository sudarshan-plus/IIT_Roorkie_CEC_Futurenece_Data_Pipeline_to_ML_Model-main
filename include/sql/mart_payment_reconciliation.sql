-- include/sql/mart_payment_reconciliation.sql
-- MART for Business Q4 (second half) / acceptance criterion 7: ONE ROW PER MISMATCH between clean orders and the
-- payment gateway, with a reason and the amount that does not reconcile. Same rules as include/nammamart/etl/reconcile.py:
--   MISSING_PAYMENT / PAYMENT_FEED_PENDING   DELIVERED or RETURNED order with no payment row (pending = order is newer
--                                            than the newest payment in the gateway file: a timing difference)
--   AMOUNT_MISMATCH / PAYMENT_FAILED / REFUNDED_BUT_DELIVERED                    for DELIVERED orders
--   RETURNED_NOT_REFUNDED / REFUND_AMOUNT_MISMATCH / RETURNED_PAYMENT_FAILED     for RETURNED orders
--   PAYMENT_FOR_CANCELLED_ORDER, PAYMENT_MODE_MISMATCH
--   PAYMENT_FOR_REJECTED_ORDER   money received for an order that failed a Critical check (so is not in revenue)
--   ORPHAN_PAYMENT               payment whose order_id exists nowhere in the orders source
-- Parameters: $run_id, $as_of.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH ords AS (
    SELECT order_id, order_ts, CAST(order_ts AS DATE) AS order_date, order_status, payment_mode,
           CAST(order_amount AS DOUBLE) AS order_amount
    FROM stg_orders WHERE dq_status <> 'FAIL'
),
pay AS (
    SELECT payment_id, order_id AS oid, payment_mode AS pmode, payment_status AS pstatus,
           CAST(amount_paid AS DOUBLE) AS amount, payment_ts AS pts, dq_status
    FROM stg_payments WHERE NOT is_duplicate_copy
),
hw AS (
    SELECT max(pts) AS high_water FROM pay WHERE pts IS NOT NULL AND pts <= CAST($as_of AS TIMESTAMP)
),
-- (A) delivered / returned orders with no payment row at all
a AS (
    SELECT o.order_id, CAST(NULL AS VARCHAR) AS payment_id, o.order_date, o.order_status,
           o.payment_mode AS order_payment_mode, CAST(NULL AS VARCHAR) AS payment_mode,
           CAST(NULL AS VARCHAR) AS payment_status, o.order_amount, CAST(NULL AS DOUBLE) AS amount_paid,
           CASE WHEN o.order_ts > (SELECT high_water FROM hw) THEN 'PAYMENT_FEED_PENDING' ELSE 'MISSING_PAYMENT' END AS mismatch_reason,
           CAST(NULL AS VARCHAR) AS payment_dq_status
    FROM ords o
    WHERE o.order_status IN ('DELIVERED', 'RETURNED') AND o.order_id NOT IN (SELECT oid FROM pay)
),
-- (B) payments that belong to a clean order
b0 AS (
    SELECT o.order_id, p.payment_id, o.order_date, o.order_status, o.payment_mode AS order_payment_mode,
           p.pmode AS payment_mode, p.pstatus AS payment_status, o.order_amount, p.amount AS amount_paid,
           CASE
               WHEN o.order_status = 'CANCELLED' THEN 'PAYMENT_FOR_CANCELLED_ORDER'
               WHEN o.order_status = 'DELIVERED' AND p.pstatus = 'SUCCESS'
                    THEN CASE WHEN abs(o.order_amount - p.amount) <= 0.011 THEN 'OK' ELSE 'AMOUNT_MISMATCH' END
               WHEN o.order_status = 'DELIVERED' AND p.pstatus = 'FAILED'   THEN 'PAYMENT_FAILED'
               WHEN o.order_status = 'DELIVERED' AND p.pstatus = 'REFUNDED' THEN 'REFUNDED_BUT_DELIVERED'
               WHEN o.order_status = 'RETURNED' AND p.pstatus = 'REFUNDED'
                    THEN CASE WHEN abs(o.order_amount - p.amount) <= 0.011 THEN 'OK' ELSE 'REFUND_AMOUNT_MISMATCH' END
               WHEN o.order_status = 'RETURNED' AND p.pstatus = 'SUCCESS'   THEN 'RETURNED_NOT_REFUNDED'
               WHEN o.order_status = 'RETURNED' AND p.pstatus = 'FAILED'    THEN 'RETURNED_PAYMENT_FAILED'
               ELSE 'UNKNOWN_PAYMENT_STATUS'
           END AS r0,
           p.dq_status AS payment_dq_status
    FROM pay p JOIN ords o ON o.order_id = p.oid
),
b AS (
    SELECT order_id, payment_id, order_date, order_status, order_payment_mode, payment_mode, payment_status,
           order_amount, amount_paid,
           CASE WHEN r0 = 'OK' AND (payment_mode IS NULL OR payment_mode <> order_payment_mode)
                THEN 'PAYMENT_MODE_MISMATCH' ELSE r0 END AS mismatch_reason,
           payment_dq_status
    FROM b0
),
-- (C) payments with no clean order behind them
c AS (
    SELECT NULLIF(p.oid, '') AS order_id, p.payment_id, CAST(NULL AS DATE) AS order_date,
           CAST(NULL AS VARCHAR) AS order_status, CAST(NULL AS VARCHAR) AS order_payment_mode,
           p.pmode AS payment_mode, p.pstatus AS payment_status, CAST(NULL AS DOUBLE) AS order_amount,
           p.amount AS amount_paid,
           CASE WHEN p.oid IN (SELECT order_id FROM stg_orders WHERE order_id <> '')
                THEN 'PAYMENT_FOR_REJECTED_ORDER' ELSE 'ORPHAN_PAYMENT' END AS mismatch_reason,
           p.dq_status AS payment_dq_status
    FROM pay p
    WHERE p.oid NOT IN (SELECT order_id FROM ords)
),
u AS (
    SELECT * FROM a UNION ALL SELECT * FROM b WHERE mismatch_reason <> 'OK' UNION ALL SELECT * FROM c
)
SELECT
    $run_id AS run_id,
    order_id, payment_id, order_date, order_status, order_payment_mode, payment_mode, payment_status,
    CAST(order_amount AS DECIMAL(12, 2)) AS order_amount,
    CAST(amount_paid AS DECIMAL(12, 2))  AS amount_paid,
    CAST(round(CASE
        WHEN mismatch_reason IN ('MISSING_PAYMENT', 'PAYMENT_FEED_PENDING', 'PAYMENT_FAILED',
                                 'REFUNDED_BUT_DELIVERED', 'RETURNED_PAYMENT_FAILED') THEN order_amount
        WHEN mismatch_reason IN ('AMOUNT_MISMATCH', 'REFUND_AMOUNT_MISMATCH') THEN abs(order_amount - amount_paid)
        WHEN mismatch_reason = 'RETURNED_NOT_REFUNDED' THEN amount_paid
        WHEN mismatch_reason IN ('PAYMENT_FOR_CANCELLED_ORDER', 'ORPHAN_PAYMENT', 'PAYMENT_FOR_REJECTED_ORDER')
             THEN abs(amount_paid)
        WHEN mismatch_reason = 'PAYMENT_MODE_MISMATCH' THEN 0.0
        ELSE order_amount END, 2) AS DECIMAL(12, 2)) AS unreconciled_amount,
    mismatch_reason,
    payment_dq_status,
    current_timestamp::TIMESTAMP AS reconciled_at
FROM u
ORDER BY mismatch_reason, order_id, payment_id;
