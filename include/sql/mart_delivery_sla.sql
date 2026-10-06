-- include/sql/mart_delivery_sla.sql
-- MART for Business Q3: average delivery time per store and the % of DELIVERED orders delivered within 15 minutes.
-- One row per store (all days loaded). Orders rejected for impossible delivery times (> 120 min) are already
-- excluded upstream, so they no longer distort the "10-minute promise" KPI.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    store_id,
    any_value(store_name)                                              AS store_name,
    any_value(zone)                                                    AS zone,
    count(*)                                                           AS delivered_orders,
    round(avg(delivery_minutes), 2)                                    AS avg_delivery_minutes,
    count(*) FILTER (WHERE within_15_min)                              AS orders_within_15_min,
    round(100.0 * count(*) FILTER (WHERE within_15_min) / count(*), 2) AS pct_within_15_min
FROM v_orders_clean
WHERE order_status = 'DELIVERED' AND delivery_minutes IS NOT NULL
GROUP BY store_id
ORDER BY store_id;
