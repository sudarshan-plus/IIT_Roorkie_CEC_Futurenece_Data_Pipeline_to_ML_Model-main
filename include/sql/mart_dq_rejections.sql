-- include/sql/mart_dq_rejections.sql
-- MART for Business Q6: rows rejected, by source file and by reason. A row failing two rules is counted under each reason.
-- Appended per run (replaced if the same run_id is re-run), so "today" = WHERE run_date = current date.
-- Parameters: $run_id, $as_of.

DELETE FROM mart_dq_rejections WHERE run_id = $run_id;

INSERT INTO mart_dq_rejections
SELECT $run_id,
       CAST(CAST($as_of AS TIMESTAMP) AS DATE),
       source_file,
       reason,
       count(*)
FROM (
    SELECT _source_file AS source_file, unnest(dq_critical) AS reason FROM stg_orders_batch
    UNION ALL SELECT _source_file, unnest(dq_critical) FROM stg_payments
    UNION ALL SELECT _source_file, unnest(dq_critical) FROM stg_customers
    UNION ALL SELECT _source_file, unnest(dq_critical) FROM stg_products
    UNION ALL SELECT _source_file, unnest(dq_critical) FROM stg_stores
)
GROUP BY source_file, reason;
