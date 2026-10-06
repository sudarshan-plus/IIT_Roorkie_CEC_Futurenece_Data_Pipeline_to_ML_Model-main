-- include/sql/mart_category_margin.sql
-- MART for Business Q2: revenue and gross margin by product category (DELIVERED orders only).
-- gross_margin = revenue - quantity x cost_price. Products with no category appear as UNKNOWN (not dropped).

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    coalesce(category, 'UNKNOWN')                                          AS category,
    count(*)                                                               AS delivered_orders,
    sum(quantity)                                                          AS units_sold,
    sum(net_revenue)                                                       AS revenue,
    sum(net_revenue) - sum(gross_margin)                                   AS cost_of_goods,
    sum(gross_margin)                                                      AS gross_margin,
    round(100.0 * sum(gross_margin) / NULLIF(sum(net_revenue), 0), 2)      AS margin_pct
FROM v_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY coalesce(category, 'UNKNOWN')
ORDER BY revenue DESC;
