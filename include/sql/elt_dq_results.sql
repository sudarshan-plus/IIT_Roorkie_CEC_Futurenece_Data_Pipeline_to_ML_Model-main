-- include/sql/elt_dq_results.sql
-- Writes the data-quality scorecard for this run into dq_results (SQL only). One row per check, passes included.
-- Row-level checks are counted from the dq_critical / dq_warnings arrays that the staging SQL produced;
-- the three BATCH_ checks are computed below. Safe to re-run: the run's rows are replaced first.
-- Parameters: $run_id, $dag_id, $as_of, $orders_rows (rows in the orders file this run).

DELETE FROM dq_results WHERE run_id = $run_id AND dag_id = $dag_id;

INSERT INTO dq_results
WITH totals AS (
    SELECT 'orders' AS src, count(*) AS n FROM stg_orders_batch
    UNION ALL SELECT 'payments',  count(*) FROM stg_payments
    UNION ALL SELECT 'customers', count(*) FROM stg_customers
    UNION ALL SELECT 'products',  count(*) FROM stg_products
    UNION ALL SELECT 'stores',    count(*) FROM stg_stores
),
flagged AS (
    SELECT 'orders' AS src, unnest(list_concat(dq_critical, dq_warnings)) AS check_name FROM stg_orders_batch
    UNION ALL SELECT 'payments',  unnest(list_concat(dq_critical, dq_warnings)) FROM stg_payments
    UNION ALL SELECT 'customers', unnest(list_concat(dq_critical, dq_warnings)) FROM stg_customers
    UNION ALL SELECT 'products',  unnest(list_concat(dq_critical, dq_warnings)) FROM stg_products
    UNION ALL SELECT 'stores',    unnest(list_concat(dq_critical, dq_warnings)) FROM stg_stores
),
counts AS (
    SELECT src, check_name, count(*) AS failed FROM flagged GROUP BY src, check_name
),
-- previous run's order-file volume (the baseline for the volume-drop check)
prev AS (
    SELECT rows_checked FROM dq_results
    WHERE dag_id = $dag_id AND check_name = 'BATCH_ORDERS_VOLUME' AND run_id <> $run_id
    ORDER BY checked_at DESC LIMIT 1
),
latest_ok AS (
    SELECT max(order_ts) AS ts FROM stg_orders_batch WHERE dq_status <> 'FAIL' AND order_ts <= CAST($as_of AS TIMESTAMP)
),
store_rates AS (
    SELECT store_id, count(*) AS n, avg(CASE WHEN dq_status = 'FAIL' THEN 1.0 ELSE 0.0 END) AS reject_rate
    FROM stg_orders_batch GROUP BY store_id HAVING count(*) >= 20
),
batch AS (
    SELECT 'BATCH_ORDERS_VOLUME' AS check_name,
           CAST($orders_rows AS BIGINT) AS n,
           CASE WHEN (SELECT count(*) FROM prev) > 0 AND (SELECT rows_checked FROM prev) > 0
                     AND $orders_rows < (SELECT rows_checked FROM prev) * (1 - 0.90)
                THEN (SELECT rows_checked FROM prev) - CAST($orders_rows AS BIGINT) ELSE 0 END AS failed
    UNION ALL
    SELECT 'BATCH_ORDERS_FRESH', 1,
           CASE WHEN (SELECT ts FROM latest_ok) IS NULL
                     OR CAST($as_of AS TIMESTAMP) - (SELECT ts FROM latest_ok) > INTERVAL 24 HOUR THEN 1 ELSE 0 END
    UNION ALL
    SELECT 'BATCH_STORE_REJECT_RATE', (SELECT count(*) FROM store_rates),
           (SELECT count(*) FROM store_rates WHERE reject_rate > 0.25)
)
SELECT $run_id, $dag_id, c.check_name, c.dimension, c.severity,
       CASE WHEN c.source_table = 'batch' THEN b.n ELSE t.n END AS rows_checked,
       CASE WHEN c.source_table = 'batch' THEN b.failed ELSE coalesce(k.failed, 0) END AS rows_failed,
       CASE WHEN (CASE WHEN c.source_table = 'batch' THEN b.failed ELSE coalesce(k.failed, 0) END) = 0 THEN 'PASS'
            WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
       current_timestamp::TIMESTAMP
FROM dq_check_catalog c
LEFT JOIN totals t ON t.src = c.source_table
LEFT JOIN counts k ON k.src = c.source_table AND k.check_name = c.check_name
LEFT JOIN batch  b ON b.check_name = c.check_name
ORDER BY c.source_table, c.check_name;
