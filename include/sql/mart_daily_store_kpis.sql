-- include/sql/mart_daily_store_kpis.sql
-- MART for Business Q1 (daily net revenue by zone and store) and the daily part of Q3 (delivery KPIs).
-- Grain: one row per day x store. Revenue counts DELIVERED orders only, after discount.
--   by zone:  SELECT order_date, zone, sum(net_revenue) FROM mart_daily_store_kpis GROUP BY 1, 2;

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    order_date,
    store_id,
    any_value(store_name)                                           AS store_name,
    any_value(zone)                                                 AS zone,
    count(*)                                                        AS orders_total,
    count(*) FILTER (WHERE order_status = 'DELIVERED')              AS delivered_orders,
    count(*) FILTER (WHERE order_status = 'CANCELLED')              AS cancelled_orders,
    count(*) FILTER (WHERE order_status = 'RETURNED')               AS returned_orders,
    sum(net_revenue)                                                AS net_revenue,
    sum(gross_margin)                                               AS gross_margin,
    round(avg(delivery_minutes) FILTER (WHERE order_status = 'DELIVERED'), 2) AS avg_delivery_minutes,
    round(100.0 * count(*) FILTER (WHERE within_15_min)
          / NULLIF(count(*) FILTER (WHERE order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL), 0), 2)
                                                                    AS pct_within_15_min
FROM v_orders_clean
GROUP BY order_date, store_id
ORDER BY order_date, store_id;
