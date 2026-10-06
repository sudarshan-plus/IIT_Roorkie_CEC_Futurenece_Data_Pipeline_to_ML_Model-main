-- submission/queries/business_questions_etl.sql
-- The six business questions answered with ONE simple query each on the ETL tables (etl_*).
-- Run:  python submission/run_business_questions.py --warehouse <path to warehouse_sudarshan.duckdb> --pipeline etl

-- @Q1a Daily net revenue by ZONE (delivered orders only, after discount)
SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_daily_zone_revenue
GROUP BY order_date, zone
ORDER BY order_date, zone;

-- @Q1b Daily net revenue by STORE
SELECT order_date, store_id, store_name, delivered_orders, net_revenue
FROM etl_daily_zone_revenue
ORDER BY order_date, store_id;

-- @Q2 Revenue and gross margin by product category (margin = revenue - quantity x cost_price)
SELECT category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct
FROM etl_category_margin;

-- @Q3 Average delivery time per store and % of delivered orders within 15 minutes
SELECT s.store_id, s.store_name,
       count(*)                                                       AS delivered_orders,
       round(avg(f.delivery_minutes), 2)                              AS avg_delivery_minutes,
       round(100.0 * avg(CASE WHEN f.within_15_min THEN 1 ELSE 0 END), 2) AS pct_within_15_min
FROM etl_fact_orders f
JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED' AND f.delivery_minutes IS NOT NULL
GROUP BY s.store_id, s.store_name
ORDER BY s.store_id;

-- @Q4a Revenue by payment mode
SELECT payment_mode, count(*) AS delivered_orders, round(sum(net_revenue), 2) AS net_revenue
FROM etl_fact_orders
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC;

-- @Q4b Amount that does not reconcile with the payment gateway (by reason; feed-pending is a timing difference)
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM etl_payment_reconciliation
GROUP BY mismatch_reason
UNION ALL
SELECT 'TOTAL (excl. PAYMENT_FEED_PENDING)', count(*), round(sum(unreconciled_amount), 2)
FROM etl_payment_reconciliation WHERE mismatch_reason <> 'PAYMENT_FEED_PENDING'
ORDER BY 3 DESC;

-- @Q5 Top 10 customers by spend, with loyalty tier (masked identity only)
SELECT rank() OVER (ORDER BY sum(f.net_revenue) DESC) AS spend_rank,
       f.customer_id, c.customer_name_masked, c.loyalty_tier,
       count(*) AS delivered_orders, round(sum(f.net_revenue), 2) AS net_spend
FROM etl_fact_orders f
JOIN etl_dim_customer c ON c.customer_id = f.customer_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.customer_id, c.customer_name_masked, c.loyalty_tier
ORDER BY spend_rank, f.customer_id
LIMIT 10;

-- @Q6 Rows rejected in the latest run, by file and by reason
SELECT source_file, reason AS failure_reason, count(*) AS rows_rejected
FROM (
    SELECT source_file, unnest(string_split(failure_reason, '; ')) AS reason
    FROM etl_quarantine
    WHERE run_id = (SELECT max_by(run_id, quarantined_at) FROM etl_quarantine)
)
GROUP BY source_file, reason
ORDER BY source_file, rows_rejected DESC, reason;
