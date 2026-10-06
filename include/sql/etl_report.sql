-- include/sql/etl_report.sql
-- ETL step 7 - REPORT tables for the 9:30 AM review. Rebuilt from the star schema on every run.

-- Business Q1: daily net revenue by zone and by store (DELIVERED orders only, after discount).
CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
SELECT f.order_date,
       s.zone,
       f.store_id,
       s.store_name,
       count(*)           AS delivered_orders,
       sum(f.net_revenue) AS net_revenue
FROM etl_fact_orders f
LEFT JOIN etl_dim_store s ON s.store_id = f.store_id
WHERE f.order_status = 'DELIVERED'
GROUP BY f.order_date, s.zone, f.store_id, s.store_name
ORDER BY f.order_date, s.zone, f.store_id;

-- Business Q2: revenue and gross margin by product category (margin = revenue - quantity x cost_price).
CREATE OR REPLACE TABLE etl_category_margin AS
SELECT p.category,
       count(*)                                        AS delivered_orders,
       sum(f.quantity)                                 AS units_sold,
       sum(f.net_revenue)                              AS revenue,
       sum(f.net_revenue) - sum(f.gross_margin)        AS cost_of_goods,
       sum(f.gross_margin)                             AS gross_margin,
       round(100.0 * sum(f.gross_margin) / NULLIF(sum(f.net_revenue), 0), 2) AS margin_pct
FROM etl_fact_orders f
LEFT JOIN etl_dim_product p ON p.product_id = f.product_id
WHERE f.order_status = 'DELIVERED'
GROUP BY p.category
ORDER BY revenue DESC;

-- BONUS - store-capacity check: orders placed per store per day versus capacity_orders_per_day (NULL capacity -> NULL utilisation).
CREATE OR REPLACE TABLE etl_store_capacity AS
SELECT f.order_date,
       f.store_id,
       any_value(s.store_name)                                        AS store_name,
       any_value(s.capacity_orders_per_day)                           AS capacity_orders_per_day,
       count(*)                                                       AS orders_placed,
       round(100.0 * count(*) / NULLIF(any_value(s.capacity_orders_per_day), 0), 2) AS utilisation_pct,
       count(*) > any_value(s.capacity_orders_per_day)                AS over_capacity
FROM etl_fact_orders f
LEFT JOIN etl_dim_store s ON s.store_id = f.store_id
GROUP BY f.order_date, f.store_id
ORDER BY f.order_date, f.store_id;
