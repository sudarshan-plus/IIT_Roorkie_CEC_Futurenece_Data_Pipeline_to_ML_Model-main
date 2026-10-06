-- submission/queries/business_questions_elt.sql
-- The six business questions answered with ONE simple query each on the ELT marts (mart_*).
-- Run:  python submission/run_business_questions.py --warehouse <path to warehouse_sudarshan.duckdb> --pipeline elt

-- @Q1a Daily net revenue by ZONE (delivered orders only, after discount)
SELECT order_date, zone, sum(delivered_orders) AS orders, round(sum(net_revenue), 2) AS net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
GROUP BY order_date, zone
ORDER BY order_date, zone;

-- @Q1b Daily net revenue by STORE
SELECT order_date, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis
WHERE delivered_orders > 0
ORDER BY order_date, store_id;

-- @Q2 Revenue and gross margin by product category (margin = revenue - quantity x cost_price)
SELECT category, delivered_orders, units_sold, revenue, cost_of_goods, gross_margin, margin_pct
FROM mart_category_margin;

-- @Q3 Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, store_name, delivered_orders, avg_delivery_minutes, pct_within_15_min
FROM mart_delivery_sla
ORDER BY store_id;

-- @Q4a Revenue by payment mode
SELECT payment_mode, delivered_orders, net_revenue, pct_of_revenue
FROM mart_revenue_by_payment_mode;

-- @Q4b Amount that does not reconcile with the payment gateway (by reason; feed-pending is a timing difference)
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount
FROM mart_payment_reconciliation
GROUP BY mismatch_reason
UNION ALL
SELECT 'TOTAL (excl. PAYMENT_FEED_PENDING)', count(*), round(sum(unreconciled_amount), 2)
FROM mart_payment_reconciliation WHERE mismatch_reason <> 'PAYMENT_FEED_PENDING'
ORDER BY 3 DESC;

-- @Q5 Top 10 customers by spend, with loyalty tier (masked identity only)
SELECT spend_rank, customer_id, customer_name_masked, loyalty_tier, delivered_orders, net_spend
FROM mart_top_customers
WHERE spend_rank <= 10
ORDER BY spend_rank, customer_id;

-- @Q6 Rows rejected in the latest run, by file and by reason
SELECT source_file, failure_reason, rows_rejected
FROM mart_dq_rejections
WHERE run_id = (SELECT max_by(run_id, run_date) FROM mart_dq_rejections)
ORDER BY source_file, rows_rejected DESC, failure_reason;
