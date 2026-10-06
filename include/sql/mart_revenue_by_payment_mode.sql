-- include/sql/mart_revenue_by_payment_mode.sql
-- MART for Business Q4 (first half): revenue by payment mode (DELIVERED orders). The second half - the amount that does
-- not reconcile with the gateway - comes from mart_payment_reconciliation.

CREATE OR REPLACE TABLE mart_revenue_by_payment_mode AS
SELECT
    payment_mode,
    count(*)                                    AS delivered_orders,
    sum(net_revenue)                            AS net_revenue,
    round(100.0 * sum(net_revenue) / sum(sum(net_revenue)) OVER (), 2) AS pct_of_revenue
FROM v_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY payment_mode
ORDER BY net_revenue DESC;
