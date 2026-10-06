-- include/sql/stg_orders_merge.sql
-- STAGING step 2 for orders: MERGE (upsert) the surviving rows of stg_orders_batch into stg_orders on order_id.
--   * order_id not in stg_orders yet        -> INSERT  (new orders)
--   * same order_id, row_hash changed       -> UPDATE  (corrections: e.g. DELIVERED -> RETURNED overwrite the old row)
--   * same order_id, row_hash identical     -> untouched (a re-run changes nothing)
-- The final statement is MERGE ... RETURNING so the caller can count inserted vs updated rows.

-- keep the old version of every row that is about to be overwritten
INSERT INTO stg_order_changes
SELECT t.order_id, current_timestamp::TIMESTAMP, $run_id, s._source_file,
       t.order_status, s.order_status, t.order_amount, s.order_amount, t.dq_status, s.dq_status
FROM stg_orders t
JOIN stg_orders_batch s ON s.order_id = t.order_id AND NOT s.is_duplicate_copy
WHERE t.row_hash <> s.row_hash;

MERGE INTO stg_orders AS t
USING (
    SELECT * FROM stg_orders_batch WHERE NOT is_duplicate_copy AND order_id <> ''
) AS s ON t.order_id = s.order_id
WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
    order_ts = s.order_ts, customer_id = s.customer_id, store_id = s.store_id, product_id = s.product_id,
    quantity = s.quantity, unit_price = s.unit_price, discount_pct = s.discount_pct, order_amount = s.order_amount,
    payment_mode = s.payment_mode, order_status = s.order_status, channel = s.channel,
    delivery_minutes = s.delivery_minutes, dq_critical = s.dq_critical, dq_warnings = s.dq_warnings,
    dq_status = s.dq_status, failure_reason = s.failure_reason, warning_reason = s.warning_reason,
    _batch_id = s._batch_id, _source_file = s._source_file, _source_row_num = s._source_row_num,
    _run_id = s._run_id, _loaded_at = s._loaded_at, row_hash = s.row_hash,
    updated_at = current_timestamp::TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.order_id, s.order_ts, s.customer_id, s.store_id, s.product_id, s.quantity, s.unit_price, s.discount_pct,
    s.order_amount, s.payment_mode, s.order_status, s.channel, s.delivery_minutes, s.dq_critical, s.dq_warnings,
    s.dq_status, s.failure_reason, s.warning_reason, s._batch_id, s._source_file, s._source_row_num, s._run_id,
    s._loaded_at, s.row_hash, current_timestamp::TIMESTAMP, current_timestamp::TIMESTAMP
)
RETURNING merge_action;
