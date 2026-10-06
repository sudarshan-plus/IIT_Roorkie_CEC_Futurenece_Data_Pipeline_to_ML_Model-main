## 3. Lineage - mart_daily_store_kpis, store S05, 2026-09-01

**The number (ELT mart):**

| order_date | store_id | delivered_orders | net_revenue |
|---|---|---|---|
| 2026-09-01 | S05 | 2 | 803.00 |

**ELT: the staging rows behind it** (`stg_orders` carries the raw file, batch and line number):

| order_id | order_status | order_amount | dq_status | _source_file | _batch_id | _source_row_num |
|---|---|---|---|---|---|---|
| ORD000049 | DELIVERED | 11.00 | PASS | orders.csv | 2 | 352 |
| ORD000062 | DELIVERED | 792.00 | PASS | orders.csv | 2 | 1977 |

**ELT: the same rows in `raw_orders`, exactly as received** (join on file + line number):

| order_id | order_ts | store_id | quantity | unit_price | discount_pct | order_amount | order_status | _source_file | _source_row_num |
|---|---|---|---|---|---|---|---|---|---|
| ORD000049 | 2026-09-01 16:58:15 | S05 | 1 | 11.0 | 0 | 11.0 | DELIVERED | orders.csv | 352 |
| ORD000062 | 2026-09-01 18:30:09 | S05 | 1 | 792.0 | 0 | 792.0 | DELIVERED | orders.csv | 1977 |

**ETL: the same number** (`etl_daily_zone_revenue` -> `etl_fact_orders`, which carries source file and row):

| order_date | store_id | delivered_orders | net_revenue |
|---|---|---|---|
| 2026-09-01 | S05 | 2 | 803.00 |

| order_id | order_status | order_amount | net_revenue | source_file | source_row_num | etl_run_id |
|---|---|---|---|---|---|---|
| ORD000049 | DELIVERED | 11.00 | 11.00 | orders.csv | 352 | demo_sept_etl_172522 |
| ORD000062 | DELIVERED | 792.00 | 792.00 | orders.csv | 1977 | demo_sept_etl_172522 |


## 8. Audit - rejected order ORD001296

**ELT - as received (`raw_orders`):**

| order_id | order_ts | customer_id | store_id | product_id | quantity | unit_price | discount_pct | order_amount | payment_mode | order_status | channel | delivery_minutes | _source_file | _run_id | _batch_id | _source_row_num |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ORD001296 | 2026-09-17 16:07:59 | C00305 | S03 | P0046 | -2 | 197.0 | 0 | -394.0 | UPI | DELIVERED | App | 35 | orders.csv | demo_sept_elt_172522 | 2 | 800 |

**ELT - the rule it broke (`stg_orders`):**

| order_id | order_status | quantity | order_amount | dq_status | failure_reason |
|---|---|---|---|---|---|
| ORD001296 | DELIVERED | -2 | -394.00 | FAIL | ORD_QUANTITY_POSITIVE |

**ELT - proof it is not in the CFO's revenue** (`v_orders_clean` feeds every mart):

| rows_in_marts_source | mart_total_revenue | clean_delivered_total |
|---|---|---|
| 0 | 1210421.30 | 1210421.30 |

**ETL - the quarantine row and its reason (`etl_quarantine`):**

| run_id | source_file | source_row_num | row_key | failure_reason |
|---|---|---|---|---|
| demo_incr_etl_172522 | orders.csv | 800 | ORD001296 | ORD_QUANTITY_POSITIVE |

**ETL - proof it is not in the finance table:**

| rows_in_etl_fact_orders |
|---|
| 0 |

**ETL - the full row as received, in the quarantine file `orders_failed_demo_incr_etl_172522.csv`:**

```
order_id=ORD001296, order_ts=2026-09-17 16:07:59, customer_id=C00305, store_id=S03, product_id=P0046, quantity=-2, unit_price=197.0, discount_pct=0, order_amount=-394.0, payment_mode=UPI, order_status=DELIVERED, channel=App, delivery_minutes=35, failure_reason=ORD_QUANTITY_POSITIVE, source_file=orders.csv, source_row_num=800
```


## 9. Change history - order ORD000337 (DELIVERED in September, RETURNED in the 2026-10-01 increment)

**ELT raw keeps BOTH versions:**

| order_id | order_status | order_amount | _source_file | _batch_id |
|---|---|---|---|---|
| ORD000337 | DELIVERED | 1748.0 | orders.csv | 2 |
| ORD000337 | RETURNED | 1748.0 | orders_2026-10-01_increment.csv | 6 |

**ELT staging holds the current version, history in `stg_order_changes`:**

| order_id | order_status | dq_status |
|---|---|---|
| ORD000337 | RETURNED | PASS |

| order_id | old_status | new_status | old_order_amount | source_file |
|---|---|---|---|---|
| ORD000337 | DELIVERED | RETURNED | 1748.00 | orders_2026-10-01_increment.csv |

**ETL fact holds the current version, history in `etl_order_changes`:**

| order_id | order_status | net_revenue |
|---|---|---|
| ORD000337 | RETURNED | 0.00 |

| order_id | old_status | new_status | old_net_revenue | new_net_revenue | source_file |
|---|---|---|---|---|---|
| ORD000337 | DELIVERED | RETURNED | 1748.00 | 0.00 | orders_2026-10-01_increment.csv |

**September revenue - restated vs as-originally-reported (both views from the history tables):**

| september_restated_elt | september_restated_etl | reversed_by_returns | september_as_originally_reported |
|---|---|---|---|
| 1130769.10 | 1130769.10 | 4193.70 | 1134962.80 |
