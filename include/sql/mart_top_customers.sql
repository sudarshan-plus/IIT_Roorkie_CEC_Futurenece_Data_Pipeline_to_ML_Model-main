-- include/sql/mart_top_customers.sql
-- MART for Business Q5: customers ranked by spend (DELIVERED orders), with loyalty tier.
-- Only masked identity columns exist here (initials) - no raw name, email or phone. Top 10 = WHERE spend_rank <= 10.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    rank() OVER (ORDER BY sum(net_revenue) DESC)  AS spend_rank,
    customer_id,
    any_value(customer_name_masked)               AS customer_name_masked,
    any_value(loyalty_tier)                       AS loyalty_tier,
    count(*)                                      AS delivered_orders,
    sum(net_revenue)                              AS net_spend
FROM v_orders_clean
WHERE order_status = 'DELIVERED'
GROUP BY customer_id
ORDER BY spend_rank, customer_id;
