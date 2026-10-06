## 1. Column scan - tables with a PII-looking column name

| table | column | verdict |
|---|---|---|
| etl_dim_customer | customer_name_masked | masked column |
| etl_dim_customer | email_masked | masked column |
| etl_dim_customer | phone_masked | masked column |
| mart_top_customers | customer_name_masked | masked column |
| raw_customers | email | restricted raw layer (by design) |
| raw_customers | full_name | restricted raw layer (by design) |
| raw_customers | phone | restricted raw layer (by design) |
| stg_customers | customer_name_masked | masked column |
| stg_customers | email_masked | masked column |
| stg_customers | phone_masked | masked column |
| v_orders_clean | customer_name_masked | masked column |

## 2. Value scan - real email / phone / name found in any non-raw table

Searched every text column of every non-raw table for 801 real emails/phones and 280 real customer names from `raw_customers`. (`stores.manager_name` is an *employee* - a different person; a few managers share a common name with a customer in this synthetic data, so that column is excluded from the name scan.)

| table | text columns scanned | rows with a real email/phone | rows with a real customer name |
|---|---|---|---|
| dq_check_catalog | 5 | 0 | 0 |
| dq_results | 6 | 0 | 0 |
| etl_category_margin | 1 | 0 | 0 |
| etl_daily_zone_revenue | 3 | 0 | 0 |
| etl_dim_customer | 11 | 0 | 0 |
| etl_dim_customer_scd2 | 3 | 0 | 0 |
| etl_dim_product | 8 | 0 | 0 |
| etl_dim_store | 8 | 0 | 0 |
| etl_fact_orders | 10 | 0 | 0 |
| etl_order_changes | 5 | 0 | 0 |
| etl_payment_reconciliation | 9 | 0 | 0 |
| etl_quarantine | 4 | 0 | 0 |
| etl_store_capacity | 2 | 0 | 0 |
| mart_category_margin | 1 | 0 | 0 |
| mart_daily_store_kpis | 3 | 0 | 0 |
| mart_delivery_sla | 3 | 0 | 0 |
| mart_dq_rejections | 3 | 0 | 0 |
| mart_payment_reconciliation | 9 | 0 | 0 |
| mart_revenue_by_payment_mode | 1 | 0 | 0 |
| mart_store_capacity | 2 | 0 | 0 |
| mart_top_customers | 3 | 0 | 0 |
| pipeline_runs | 5 | 0 | 0 |
| stg_customers | 14 | 0 | 0 |
| stg_order_changes | 7 | 0 | 0 |
| stg_orders | 13 | 0 | 0 |
| stg_orders_batch | 13 | 0 | 0 |
| stg_payments | 10 | 0 | 0 |
| stg_products | 11 | 0 | 0 |
| stg_stores | 11 | 0 | 0 |
| v_orders_clean | 14 | 0 | 0 |

**Result: PASS - no real customer email, phone or name outside the restricted raw layer** (0 unmasked column names, 0 leaked values)
