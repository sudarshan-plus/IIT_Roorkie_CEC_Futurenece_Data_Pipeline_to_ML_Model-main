### pipeline_runs - one row per run, failed runs included

```sql
SELECT dag_id, run_id, status, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, failed_tasks FROM pipeline_runs ORDER BY start_time DESC LIMIT 12
```

| dag_id | run_id | status | start_time | end_time | rows_extracted | rows_loaded | rows_quarantined | failed_tasks |
|---|---|---|---|---|---|---|---|---|
| nammamart_elt_sudarshan | demo_incr_elt_172522 | SUCCESS | 2026-10-06 17:26:55.601782 | 2026-10-06 17:27:34.035157 | 5372 | 160 | 142 |  |
| nammamart_etl_sudarshan | demo_incr_etl_172522 | SUCCESS | 2026-10-06 17:26:32.851092 | 2026-10-06 17:26:49.893660 | 5372 | 3023 | 142 |  |
| nammamart_elt_sudarshan | demo_sept_elt_172522 | SUCCESS | 2026-10-06 17:25:47.722710 | 2026-10-06 17:26:26.590455 | 5212 | 5212 | 142 |  |
| nammamart_etl_sudarshan | demo_sept_etl_172522 | SUCCESS | 2026-10-06 17:25:25.984823 | 2026-10-06 17:25:41.433593 | 5212 | 2873 | 142 |  |

### dq_results - the scorecard of the latest run of each DAG (checks that found something)

```sql
SELECT dag_id, check_name, dimension, severity, rows_checked, rows_failed, status FROM dq_results r WHERE rows_failed > 0 AND run_id = (SELECT run_id FROM dq_results x WHERE x.dag_id = r.dag_id ORDER BY checked_at DESC LIMIT 1) ORDER BY dag_id, rows_failed DESC, check_name LIMIT 24
```

| dag_id | check_name | dimension | severity | rows_checked | rows_failed | status |
|---|---|---|---|---|---|---|
| nammamart_elt_sudarshan | ORD_AMOUNT_MATCHES_FORMULA | Accuracy | Critical | 2562 | 18 | FAIL |
| nammamart_elt_sudarshan | ORD_CUSTOMER_ID_NOT_NULL | Completeness | Critical | 2562 | 18 | FAIL |
| nammamart_elt_sudarshan | PAY_AMOUNT_MATCHES_ORDER | Consistency | Warning | 2228 | 14 | WARN |
| nammamart_elt_sudarshan | CUS_EMAIL_FORMAT | Validity | Warning | 405 | 12 | WARN |
| nammamart_elt_sudarshan | ORD_ORDER_ID_UNIQUE | Uniqueness | Critical | 2562 | 12 | FAIL |
| nammamart_elt_sudarshan | ORD_QUANTITY_POSITIVE | Accuracy | Critical | 2562 | 10 | FAIL |
| nammamart_elt_sudarshan | PAY_AMOUNT_POSITIVE | Accuracy | Critical | 2228 | 10 | FAIL |
| nammamart_elt_sudarshan | ORD_PRODUCT_EXISTS | Consistency | Critical | 2562 | 9 | FAIL |
| nammamart_elt_sudarshan | CUS_LOYALTY_TIER_CASING | Consistency | Warning | 405 | 8 | WARN |
| nammamart_elt_sudarshan | ORD_DELIVERED_HAS_MINUTES | Completeness | Critical | 2562 | 8 | FAIL |
| nammamart_elt_sudarshan | ORD_PAYMENT_MODE_STANDARDISED | Validity | Warning | 2562 | 8 | WARN |
| nammamart_elt_sudarshan | PAY_MODE_STANDARDISED | Validity | Warning | 2228 | 8 | WARN |
| nammamart_elt_sudarshan | PAY_ORDER_EXISTS | Consistency | Critical | 2228 | 8 | FAIL |
| nammamart_elt_sudarshan | CUS_PHONE_10_DIGITS | Validity | Warning | 405 | 7 | WARN |
| nammamart_elt_sudarshan | ORD_ORDER_TS_NOT_FUTURE | Timeliness | Critical | 2562 | 7 | FAIL |
| nammamart_elt_sudarshan | PAY_TS_NOT_FUTURE | Timeliness | Critical | 2228 | 7 | FAIL |
| nammamart_elt_sudarshan | CUS_CITY_NOT_NULL | Completeness | Warning | 405 | 6 | WARN |
| nammamart_elt_sudarshan | ORD_DELIVERY_MINUTES_MAX | Accuracy | Critical | 2562 | 6 | FAIL |
| nammamart_elt_sudarshan | CUS_CUSTOMER_ID_UNIQUE | Uniqueness | Critical | 405 | 5 | FAIL |
| nammamart_elt_sudarshan | ORD_ORDER_TS_VALID | Validity | Critical | 2562 | 5 | FAIL |
| nammamart_elt_sudarshan | ORD_STORE_EXISTS | Consistency | Critical | 2562 | 5 | FAIL |
| nammamart_elt_sudarshan | PAY_PAYMENT_ID_UNIQUE | Uniqueness | Critical | 2228 | 4 | FAIL |
| nammamart_elt_sudarshan | PRD_COST_LE_MRP | Accuracy | Warning | 152 | 4 | WARN |
| nammamart_elt_sudarshan | CUS_PINCODE_6_DIGITS | Validity | Warning | 405 | 3 | WARN |

### dq_results - every check is written every run (passes included): rows per DAG and status for the latest run

```sql
SELECT dag_id, status, count(*) AS checks FROM dq_results r WHERE run_id = (SELECT run_id FROM dq_results x WHERE x.dag_id = r.dag_id ORDER BY checked_at DESC LIMIT 1) GROUP BY dag_id, status ORDER BY dag_id, status
```

| dag_id | status | checks |
|---|---|---|
| nammamart_elt_sudarshan | FAIL | 21 |
| nammamart_elt_sudarshan | PASS | 29 |
| nammamart_elt_sudarshan | WARN | 16 |
| nammamart_etl_sudarshan | FAIL | 21 |
| nammamart_etl_sudarshan | PASS | 29 |
| nammamart_etl_sudarshan | WARN | 16 |

### etl_payment_reconciliation - one row per order/payment mismatch (summary by reason)

```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM etl_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| PAYMENT_FOR_REJECTED_ORDER | 90 | 84812.98 |
| PAYMENT_FEED_PENDING | 150 | 79652.20 |
| PAYMENT_FAILED | 42 | 25722.25 |
| MISSING_PAYMENT | 28 | 17476.35 |
| ORPHAN_PAYMENT | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 10 | 4193.70 |
| AMOUNT_MISMATCH | 13 | 760.00 |

### etl_payment_reconciliation - sample rows

```sql
SELECT order_id, payment_id, order_status, payment_status, order_amount, amount_paid, unreconciled_amount, mismatch_reason FROM etl_payment_reconciliation WHERE mismatch_reason IN ('AMOUNT_MISMATCH', 'PAYMENT_FAILED', 'ORPHAN_PAYMENT', 'RETURNED_NOT_REFUNDED') ORDER BY mismatch_reason, order_id LIMIT 14
```

| order_id | payment_id | order_status | payment_status | order_amount | amount_paid | unreconciled_amount | mismatch_reason |
|---|---|---|---|---|---|---|---|
| ORD000178 | PAY0001139 | DELIVERED | SUCCESS | 378.00 | 353.00 | 25.00 | AMOUNT_MISMATCH |
| ORD000371 | PAY0002183 | DELIVERED | SUCCESS | 980.40 | 955.40 | 25.00 | AMOUNT_MISMATCH |
| ORD000481 | PAY0000586 | DELIVERED | SUCCESS | 102.60 | 2.60 | 100.00 | AMOUNT_MISMATCH |
| ORD001031 | PAY0001568 | DELIVERED | SUCCESS | 56.95 | -43.05 | 100.00 | AMOUNT_MISMATCH |
| ORD001039 | PAY0000161 | DELIVERED | SUCCESS | 282.20 | 232.20 | 50.00 | AMOUNT_MISMATCH |
| ORD001360 | PAY0000909 | DELIVERED | SUCCESS | 203.00 | 103.00 | 100.00 | AMOUNT_MISMATCH |
| ORD001572 | PAY0001813 | DELIVERED | SUCCESS | 502.40 | 402.40 | 100.00 | AMOUNT_MISMATCH |
| ORD001753 | PAY0001167 | DELIVERED | SUCCESS | 502.35 | 452.35 | 50.00 | AMOUNT_MISMATCH |
| ORD001832 | PAY0001499 | DELIVERED | SUCCESS | 374.30 | 349.30 | 25.00 | AMOUNT_MISMATCH |
| ORD001840 | PAY0000779 | DELIVERED | SUCCESS | 102.60 | 2.60 | 100.00 | AMOUNT_MISMATCH |
| ORD001864 | PAY0000323 | DELIVERED | SUCCESS | 192.95 | 182.95 | 10.00 | AMOUNT_MISMATCH |
| ORD001937 | PAY0000354 | DELIVERED | SUCCESS | 505.00 | 455.00 | 50.00 | AMOUNT_MISMATCH |
| ORD002185 | PAY0000429 | DELIVERED | SUCCESS | 243.20 | 218.20 | 25.00 | AMOUNT_MISMATCH |
| ORD941071 | PAY0002221 |  | SUCCESS |  | 618.36 | 618.36 | ORPHAN_PAYMENT |

### mart_payment_reconciliation (ELT) - the same reconciliation, identical rows

```sql
SELECT mismatch_reason, count(*) AS rows, round(sum(unreconciled_amount), 2) AS unreconciled_amount FROM mart_payment_reconciliation GROUP BY 1 ORDER BY 3 DESC
```

| mismatch_reason | rows | unreconciled_amount |
|---|---|---|
| PAYMENT_FOR_REJECTED_ORDER | 90 | 84812.98 |
| PAYMENT_FEED_PENDING | 150 | 79652.20 |
| PAYMENT_FAILED | 42 | 25722.25 |
| MISSING_PAYMENT | 28 | 17476.35 |
| ORPHAN_PAYMENT | 8 | 4580.57 |
| RETURNED_NOT_REFUNDED | 10 | 4193.70 |
| AMOUNT_MISMATCH | 13 | 760.00 |
