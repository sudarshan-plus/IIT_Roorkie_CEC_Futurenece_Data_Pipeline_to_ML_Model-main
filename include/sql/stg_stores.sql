-- include/sql/stg_stores.sql
-- STAGING for stores: SQL only. A blank zone becomes UNKNOWN (so the store's orders stay in the numbers, visibly
-- bucketed as UNKNOWN, instead of being rejected); capacity <= 0 becomes NULL.
-- Parameters: $as_of.

CREATE OR REPLACE TABLE stg_stores AS
WITH cur AS (
    SELECT * FROM raw_stores WHERE _batch_id = (SELECT max(_batch_id) FROM raw_stores)
),
t AS (
    SELECT cur.*,
           trim(store_id) AS sid,
           upper(substr(trim(zone), 1, 1)) || lower(substr(trim(zone), 2)) AS zone_c,
           CASE WHEN regexp_full_match(trim(capacity_orders_per_day), '[+-]?\d+(\.\d+)?')
                THEN CAST(trim(capacity_orders_per_day) AS DOUBLE) END AS cap_n,
           CASE WHEN regexp_full_match(trim(open_date), '\d{4}-\d{2}-\d{2}')
                THEN CAST(try_strptime(trim(open_date), '%Y-%m-%d') AS DATE) END AS open_d,
           row_number() OVER (
               PARTITION BY CASE WHEN trim(store_id) = '' THEN '#' || _source_row_num ELSE trim(store_id) END
               ORDER BY _source_row_num) AS rn
    FROM cur
),
f AS (
    SELECT t.*,
           list_filter([
               CASE WHEN sid = '' THEN 'STO_STORE_ID_NOT_NULL' END,
               CASE WHEN rn > 1   THEN 'STO_STORE_ID_UNIQUE' END
           ], x -> x IS NOT NULL) AS dq_critical,
           list_filter([
               CASE WHEN zone_c = '' THEN 'STO_ZONE_NOT_NULL' END,
               CASE WHEN zone_c <> '' AND zone_c NOT IN ('North', 'South', 'East', 'West') THEN 'STO_ZONE_VALID' END,
               CASE WHEN NOT regexp_full_match(trim(pincode), '\d{6}') THEN 'STO_PINCODE_6_DIGITS' END,
               CASE WHEN cap_n IS NULL OR cap_n <= 0 THEN 'STO_CAPACITY_POSITIVE' END,
               CASE WHEN open_d IS NOT NULL AND open_d > CAST(CAST($as_of AS TIMESTAMP) AS DATE) THEN 'STO_OPEN_DATE_NOT_FUTURE' END
           ], x -> x IS NOT NULL) AS dq_warnings
    FROM t
)
SELECT
    sid AS store_id,
    trim(store_name) AS store_name,
    CASE WHEN zone_c IN ('North', 'South', 'East', 'West') THEN zone_c ELSE 'UNKNOWN' END AS zone,
    trim(area) AS area,
    CASE WHEN regexp_full_match(trim(pincode), '\d{6}') THEN trim(pincode) END AS pincode,
    open_d AS open_date,
    trim(manager_name) AS manager_name,
    CASE WHEN cap_n > 0 THEN CAST(cap_n AS INTEGER) END AS capacity_orders_per_day,
    dq_critical,
    dq_warnings,
    CASE WHEN len(dq_critical) > 0 THEN 'FAIL' WHEN len(dq_warnings) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    NULLIF(array_to_string(dq_critical, '; '), '') AS failure_reason,
    NULLIF(array_to_string(dq_warnings, '; '), '') AS warning_reason,
    rn > 1 AS is_duplicate_copy,
    _batch_id, _source_file, _source_row_num, _run_id, _loaded_at
FROM f;
