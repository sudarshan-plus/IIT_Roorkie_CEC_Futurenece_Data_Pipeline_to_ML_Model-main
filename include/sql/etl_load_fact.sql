-- include/sql/etl_load_fact.sql
-- Idempotent load of etl_fact_orders from the transform output (view `batch_fact`).
-- Same data in -> same rows out: a re-run inserts 0, updates 0 (the row_hash is unchanged).
-- A correction (e.g. status DELIVERED -> RETURNED) hashes differently, so it UPDATES the existing row.
-- The statement list ends with the MERGE ... RETURNING so the caller can count inserts vs updates.

CREATE OR REPLACE TEMP TABLE etl_batch AS
SELECT
    order_id,
    CAST(order_ts AS TIMESTAMP)                  AS order_ts,
    CAST(order_date AS DATE)                     AS order_date,
    customer_id, store_id, product_id,
    CAST(quantity AS INTEGER)                    AS quantity,
    CAST(unit_price AS DECIMAL(12, 2))           AS unit_price,
    CAST(discount_pct AS DECIMAL(6, 2))          AS discount_pct,
    CAST(order_amount AS DECIMAL(12, 2))         AS order_amount,
    payment_mode, order_status, channel,
    TRY_CAST(NULLIF(delivery_minutes, '') AS INTEGER)  AS delivery_minutes,
    CAST(net_revenue AS DECIMAL(12, 2))          AS net_revenue,
    TRY_CAST(NULLIF(gross_margin, '') AS DECIMAL(12, 2)) AS gross_margin,
    TRY_CAST(NULLIF(within_15_min, '') AS BOOLEAN)       AS within_15_min,
    source_file,
    CAST(source_row_num AS INTEGER)              AS source_row_num
FROM batch_fact;

CREATE OR REPLACE TEMP TABLE etl_batch_hashed AS
SELECT *, md5(concat_ws('|', order_id, CAST(order_ts AS VARCHAR), customer_id, store_id, product_id,
              CAST(quantity AS VARCHAR), CAST(unit_price AS VARCHAR), CAST(discount_pct AS VARCHAR),
              CAST(order_amount AS VARCHAR), payment_mode, order_status, channel,
              COALESCE(CAST(delivery_minutes AS VARCHAR), ''), CAST(net_revenue AS VARCHAR),
              COALESCE(CAST(gross_margin AS VARCHAR), ''))) AS row_hash
FROM etl_batch;

-- keep a history of every correction before it overwrites the current row
INSERT INTO etl_order_changes
SELECT t.order_id, current_timestamp::TIMESTAMP, $run_id, s.source_file,
       t.order_status, s.order_status, t.order_amount, s.order_amount, t.net_revenue, s.net_revenue
FROM etl_fact_orders t
JOIN etl_batch_hashed s ON s.order_id = t.order_id
WHERE t.row_hash <> s.row_hash;

-- an order whose latest version now FAILS a Critical check must not stay in the finance table
DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM rejected_orders);

MERGE INTO etl_fact_orders AS t
USING etl_batch_hashed AS s ON t.order_id = s.order_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
    order_ts = s.order_ts, order_date = s.order_date, customer_id = s.customer_id, store_id = s.store_id,
    product_id = s.product_id, quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct,
    order_amount = s.order_amount, payment_mode = s.payment_mode, order_status = s.order_status,
    channel = s.channel, delivery_minutes = s.delivery_minutes, net_revenue = s.net_revenue,
    gross_margin = s.gross_margin, within_15_min = s.within_15_min, row_hash = s.row_hash,
    source_file = s.source_file, source_row_num = s.source_row_num, etl_run_id = $run_id,
    updated_at = current_timestamp::TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id, s.quantity, s.unit_price,
    s.discount_pct, s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes,
    s.net_revenue, s.gross_margin, s.within_15_min, s.row_hash, s.source_file, s.source_row_num, $run_id,
    current_timestamp::TIMESTAMP, current_timestamp::TIMESTAMP
)
RETURNING merge_action;
