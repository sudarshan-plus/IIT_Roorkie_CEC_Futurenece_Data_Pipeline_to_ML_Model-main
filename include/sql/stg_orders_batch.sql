-- include/sql/stg_orders_batch.sql
-- STAGING step 1 for orders: SQL only. Evaluates the CURRENT version of every order in raw_orders.
--
--   current version : for each order_id the rows of the winning batch are used (an increment file outranks the base
--                     snapshot, a later increment outranks an earlier one - that is how next-day corrections arrive);
--                     the superseded versions stay in raw_orders for audit.
--   duplicates      : if the newest batch holds the same order_id twice, the first row survives, the extra copy is
--                     flagged ORD_ORDER_ID_UNIQUE (FAIL).
--   quality rules   : same rulebook as the ETL (include/nammamart/dq_catalog.py) -> dq_critical / dq_warnings /
--                     dq_status / failure_reason. Rejected rows are KEPT here, labelled; marts filter them out.
-- stg_orders_batch holds every evaluated row (incl. duplicate copies). The next step MERGEs the survivors into stg_orders.
-- Requires stg_customers / stg_products / stg_stores (built first). Parameters: $as_of.

CREATE OR REPLACE TABLE stg_orders_batch AS
WITH keyed AS (
    SELECT r.*,
           trim(r.order_id) AS oid,
           CASE WHEN trim(r.order_id) = '' THEN '#' || r._batch_id || '-' || r._source_row_num
                ELSE trim(r.order_id) END AS pkey
    FROM raw_orders r
),
-- Which version of an order wins? An increment file (orders_<date>_increment.csv) outranks the base snapshot, later
-- increments outrank earlier ones, and a re-load of the same file (changed content) outranks its older batch. So a
-- re-delivered base orders.csv can never silently undo a correction that arrived in an increment (found in Experiment 5).
latest AS (
    SELECT keyed.*,
           first_value(_batch_id) OVER (
               PARTITION BY pkey
               ORDER BY CASE WHEN regexp_matches(_source_file, '^orders_.*_increment\.csv$') THEN 1 ELSE 0 END DESC,
                        _source_file DESC, _batch_id DESC) AS _win_batch
    FROM keyed
),
cur AS (
    SELECT latest.*, row_number() OVER (PARTITION BY pkey ORDER BY _source_row_num) AS rn
    FROM latest WHERE _batch_id = _win_batch
),
t AS (
    SELECT cur.*,
           trim(customer_id) AS cid,
           trim(product_id)  AS pid,
           trim(store_id)    AS sid,
           trim(order_ts)    AS ts_text,
           CASE WHEN regexp_full_match(trim(order_ts), '\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}')
                THEN try_strptime(trim(order_ts), '%Y-%m-%d %H:%M:%S') END AS ts,
           upper(trim(order_status)) AS status_u,
           CASE lower(trim(payment_mode))
                WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cod' THEN 'Cash on Delivery'
                WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet' END AS mode_std,
           CASE WHEN regexp_full_match(trim(quantity), '[+-]?\d+') THEN CAST(trim(quantity) AS DOUBLE) END AS qty,
           CASE WHEN regexp_full_match(trim(unit_price), '[+-]?\d+(\.\d+)?') THEN CAST(trim(unit_price) AS DOUBLE) END AS price,
           CASE WHEN regexp_full_match(trim(discount_pct), '[+-]?\d+(\.\d+)?') THEN CAST(trim(discount_pct) AS DOUBLE) END AS disc,
           CASE WHEN regexp_full_match(trim(order_amount), '[+-]?\d+(\.\d+)?') THEN CAST(trim(order_amount) AS DOUBLE) END AS amount,
           CASE WHEN regexp_full_match(trim(delivery_minutes), '[+-]?\d+(\.\d+)?') THEN CAST(trim(delivery_minutes) AS DOUBLE) END AS minutes,
           trim(delivery_minutes) = '' AS minutes_blank,
           concat_ws('|', order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct,
                     order_amount, payment_mode, order_status, channel, delivery_minutes) AS raw_fp   -- the row exactly as received
    FROM cur
),
j AS (
    SELECT t.*,
           p.mrp AS cat_mrp,
           round(t.qty * t.price * (1 - t.disc / 100.0), 2) AS formula
    FROM t
    LEFT JOIN (SELECT product_id, mrp FROM stg_products WHERE NOT is_duplicate_copy AND mrp > 0) p ON p.product_id = t.pid
),
f AS (
    SELECT j.*,
           list_filter([
               CASE WHEN oid = '' THEN 'ORD_ORDER_ID_NOT_NULL' END,
               CASE WHEN cid = '' THEN 'ORD_CUSTOMER_ID_NOT_NULL' END,
               CASE WHEN ts_text = '' THEN 'ORD_ORDER_TS_NOT_NULL' END,
               CASE WHEN status_u = 'DELIVERED' AND minutes_blank THEN 'ORD_DELIVERED_HAS_MINUTES' END,
               CASE WHEN rn > 1 THEN 'ORD_ORDER_ID_UNIQUE' END,
               CASE WHEN status_u NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'ORD_STATUS_VALID' END,
               CASE WHEN mode_std IS NULL THEN 'ORD_PAYMENT_MODE_VALID' END,
               CASE WHEN ts_text <> '' AND ts IS NULL THEN 'ORD_ORDER_TS_VALID' END,
               CASE WHEN NOT (qty IS NOT NULL AND price IS NOT NULL AND disc IS NOT NULL AND amount IS NOT NULL
                              AND (minutes IS NOT NULL OR minutes_blank)) THEN 'ORD_NUMBERS_VALID' END,
               CASE WHEN disc IS NOT NULL AND (disc < 0 OR disc > 100) THEN 'ORD_DISCOUNT_RANGE' END,
               CASE WHEN qty IS NOT NULL AND qty <= 0 THEN 'ORD_QUANTITY_POSITIVE' END,
               CASE WHEN price IS NOT NULL AND price <= 0 THEN 'ORD_UNIT_PRICE_POSITIVE' END,
               CASE WHEN qty IS NOT NULL AND price IS NOT NULL AND disc IS NOT NULL AND amount IS NOT NULL
                         AND abs(formula - amount) > 0.011 THEN 'ORD_AMOUNT_MATCHES_FORMULA' END,
               CASE WHEN minutes IS NOT NULL AND minutes > 120 THEN 'ORD_DELIVERY_MINUTES_MAX' END,
               CASE WHEN price IS NOT NULL AND cat_mrp IS NOT NULL AND (price > cat_mrp * 3 OR price * 3 < cat_mrp)
                    THEN 'ORD_PRICE_FAR_FROM_CATALOGUE' END,
               CASE WHEN cid <> '' AND cid NOT IN (SELECT customer_id FROM stg_customers WHERE customer_id <> '')
                    THEN 'ORD_CUSTOMER_EXISTS' END,
               CASE WHEN pid NOT IN (SELECT product_id FROM stg_products WHERE product_id <> '') THEN 'ORD_PRODUCT_EXISTS' END,
               CASE WHEN sid NOT IN (SELECT store_id FROM stg_stores WHERE store_id <> '') THEN 'ORD_STORE_EXISTS' END,
               CASE WHEN ts IS NOT NULL AND ts > CAST($as_of AS TIMESTAMP) THEN 'ORD_ORDER_TS_NOT_FUTURE' END
           ], x -> x IS NOT NULL) AS dq_critical,
           list_filter([
               CASE WHEN status_u IN ('DELIVERED', 'CANCELLED', 'RETURNED') AND order_status <> status_u THEN 'ORD_STATUS_CASING' END,
               CASE WHEN mode_std IS NOT NULL AND payment_mode <> mode_std THEN 'ORD_PAYMENT_MODE_STANDARDISED' END,
               CASE WHEN trim(channel) NOT IN ('App', 'Web') THEN 'ORD_CHANNEL_VALID' END,
               CASE WHEN minutes IS NOT NULL AND minutes <= 120 AND (minutes < 8 OR minutes > 35) THEN 'ORD_DELIVERY_MINUTES_TYPICAL' END,
               CASE WHEN price IS NOT NULL AND cat_mrp IS NOT NULL AND abs(price - cat_mrp) > 0.011 THEN 'ORD_PRICE_MATCHES_CATALOGUE' END
           ], x -> x IS NOT NULL) AS dq_warnings
    FROM j
)
SELECT
    oid AS order_id,
    ts AS order_ts,
    cid AS customer_id,
    sid AS store_id,
    pid AS product_id,
    TRY_CAST(qty AS INTEGER) AS quantity,
    TRY_CAST(price AS DECIMAL(12, 2)) AS unit_price,
    TRY_CAST(disc AS DECIMAL(6, 2)) AS discount_pct,
    TRY_CAST(amount AS DECIMAL(12, 2)) AS order_amount,
    mode_std AS payment_mode,
    status_u AS order_status,
    trim(channel) AS channel,
    TRY_CAST(minutes AS INTEGER) AS delivery_minutes,
    dq_critical,
    dq_warnings,
    CASE WHEN len(dq_critical) > 0 THEN 'FAIL' WHEN len(dq_warnings) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    NULLIF(array_to_string(dq_critical, '; '), '') AS failure_reason,
    NULLIF(array_to_string(dq_warnings, '; '), '') AS warning_reason,
    rn > 1 AS is_duplicate_copy,
    _batch_id, _source_file, _source_row_num, _run_id, _loaded_at,
    -- fingerprint of the row as received + its quality verdict: a change in either means "new version"
    md5(concat_ws('|', raw_fp, array_to_string(dq_critical, ','), array_to_string(dq_warnings, ','))) AS row_hash
FROM f;
