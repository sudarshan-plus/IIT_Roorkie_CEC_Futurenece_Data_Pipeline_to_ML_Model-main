-- include/sql/elt_views.sql
-- The one place where "a clean order" is defined for every mart:
--   * only orders that did not fail a Critical check (dq_status <> 'FAIL')
--   * joined to the current store / product / customer rows (extra duplicate copies excluded)
--   * derived measures: net_revenue (DELIVERED only), gross_margin = revenue - quantity x cost_price, within_15_min
-- Customer columns here are the MASKED ones from stg_customers - no raw PII is reachable from the marts.

CREATE OR REPLACE VIEW v_orders_clean AS
SELECT
    o.order_id,
    o.order_ts,
    CAST(o.order_ts AS DATE)                                      AS order_date,
    o.customer_id, o.store_id, o.product_id,
    o.quantity, o.unit_price, o.discount_pct, o.order_amount,
    o.payment_mode, o.order_status, o.channel, o.delivery_minutes,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.order_amount ELSE 0 END                                AS net_revenue,
    CASE WHEN o.order_status = 'DELIVERED' THEN round(o.order_amount - o.quantity * p.cost_price, 2)
         ELSE 0 END                                                                                      AS gross_margin,
    CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes <= 15 END                             AS within_15_min,
    s.store_name, s.zone,
    p.category, p.product_name,
    c.loyalty_tier, c.customer_name_masked,
    o._source_file, o._source_row_num
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores    WHERE NOT is_duplicate_copy) s ON s.store_id    = o.store_id
LEFT JOIN (SELECT * FROM stg_products  WHERE NOT is_duplicate_copy) p ON p.product_id  = o.product_id
LEFT JOIN (SELECT * FROM stg_customers WHERE NOT is_duplicate_copy) c ON c.customer_id = o.customer_id
WHERE o.dq_status <> 'FAIL';
