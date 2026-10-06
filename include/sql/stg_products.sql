-- include/sql/stg_products.sql
-- STAGING for products: SQL only. Newest raw snapshot ->
--   * dedupe: when a product_id appears twice, the CURRENT row survives and the "(old)" row is flagged FAIL
--   * a missing category becomes UNKNOWN; an invalid mrp / cost_price becomes NULL (flagged, never guessed)
--   * quality rules -> dq_critical / dq_warnings / dq_status / failure_reason
-- Parameters: $as_of.

CREATE OR REPLACE TABLE stg_products AS
WITH cur AS (
    SELECT * FROM raw_products WHERE _batch_id = (SELECT max(_batch_id) FROM raw_products)
),
t AS (
    SELECT cur.*,
           trim(product_id) AS pid,
           CASE WHEN regexp_full_match(trim(mrp), '[+-]?\d+(\.\d+)?') THEN TRY_CAST(trim(mrp) AS DECIMAL(12, 2)) END AS mrp_n,
           CASE WHEN regexp_full_match(trim(cost_price), '[+-]?\d+(\.\d+)?') THEN TRY_CAST(trim(cost_price) AS DECIMAL(12, 2)) END AS cost_n,
           CASE WHEN regexp_full_match(trim(launch_date), '\d{4}-\d{2}-\d{2}')
                THEN CAST(try_strptime(trim(launch_date), '%Y-%m-%d') AS DATE) END AS launch_d,
           row_number() OVER (
               PARTITION BY CASE WHEN trim(product_id) = '' THEN '#' || _source_row_num ELSE trim(product_id) END
               ORDER BY CASE WHEN lower(trim(product_name)) LIKE '%(old)%' THEN 1 ELSE 0 END, _source_row_num) AS rn
    FROM cur
),
f AS (
    SELECT t.*,
           list_filter([
               CASE WHEN pid = '' THEN 'PRD_PRODUCT_ID_NOT_NULL' END,
               CASE WHEN rn > 1   THEN 'PRD_PRODUCT_ID_UNIQUE' END
           ], x -> x IS NOT NULL) AS dq_critical,
           list_filter([
               CASE WHEN trim(category) = '' THEN 'PRD_CATEGORY_NOT_NULL' END,
               CASE WHEN launch_d IS NULL THEN 'PRD_LAUNCH_DATE_VALID' END,
               CASE WHEN mrp_n IS NULL OR mrp_n <= 0 THEN 'PRD_MRP_POSITIVE' END,
               CASE WHEN cost_n IS NULL OR cost_n <= 0 THEN 'PRD_COST_PRICE_POSITIVE' END,
               CASE WHEN mrp_n > 0 AND cost_n > 0 AND cost_n > mrp_n THEN 'PRD_COST_LE_MRP' END,
               CASE WHEN launch_d IS NOT NULL AND launch_d > CAST(CAST($as_of AS TIMESTAMP) AS DATE) THEN 'PRD_LAUNCH_DATE_NOT_FUTURE' END
           ], x -> x IS NOT NULL) AS dq_warnings
    FROM t
)
SELECT
    pid AS product_id,
    trim(product_name) AS product_name,
    CASE WHEN trim(category) = '' THEN 'UNKNOWN' ELSE trim(category) END AS category,
    trim(sub_category) AS sub_category,
    trim(brand) AS brand,
    CASE WHEN mrp_n > 0 THEN mrp_n END AS mrp,
    CASE WHEN cost_n > 0 THEN cost_n END AS cost_price,
    trim(unit) AS unit,
    TRY_CAST(CASE lower(trim(is_perishable)) WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' END AS BOOLEAN) AS is_perishable,
    launch_d AS launch_date,
    dq_critical,
    dq_warnings,
    CASE WHEN len(dq_critical) > 0 THEN 'FAIL' WHEN len(dq_warnings) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    NULLIF(array_to_string(dq_critical, '; '), '') AS failure_reason,
    NULLIF(array_to_string(dq_warnings, '; '), '') AS warning_reason,
    rn > 1 AS is_duplicate_copy,
    _batch_id, _source_file, _source_row_num, _run_id, _loaded_at
FROM f;
