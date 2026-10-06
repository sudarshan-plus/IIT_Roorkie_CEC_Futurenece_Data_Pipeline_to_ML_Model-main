-- include/sql/mart_store_capacity.sql
-- BONUS - store-capacity check: orders placed per store per day versus capacity_orders_per_day from the store master.
-- utilisation_pct above 100 means the store took more orders than it can handle that day (over_capacity = true).
-- A store whose capacity is unknown or invalid (e.g. S12 has 0 in the master) gets NULL utilisation, not a division error.
-- Counts every clean order placed that day (any status): capacity is about orders accepted, not about revenue.

CREATE OR REPLACE TABLE mart_store_capacity AS
SELECT
    o.order_date,
    o.store_id,
    any_value(o.store_name)                                           AS store_name,
    any_value(s.capacity_orders_per_day)                              AS capacity_orders_per_day,
    count(*)                                                          AS orders_placed,
    round(100.0 * count(*) / NULLIF(any_value(s.capacity_orders_per_day), 0), 2) AS utilisation_pct,
    count(*) > any_value(s.capacity_orders_per_day)                   AS over_capacity
FROM v_orders_clean o
LEFT JOIN (SELECT * FROM stg_stores WHERE NOT is_duplicate_copy) s ON s.store_id = o.store_id
GROUP BY o.order_date, o.store_id
ORDER BY o.order_date, o.store_id;
