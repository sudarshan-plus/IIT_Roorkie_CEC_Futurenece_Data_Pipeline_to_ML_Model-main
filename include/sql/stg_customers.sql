-- include/sql/stg_customers.sql
-- STAGING for customers: SQL only. Takes the newest raw snapshot, then
--   * casts / standardises   (trim, tier casing, UNKNOWN for a missing city)
--   * masks PII              (the raw full_name / email / phone are NOT carried into staging)
--   * dedupes                (first row per customer_id survives, the extra copies are flagged FAIL)
--   * applies the quality rules -> dq_critical / dq_warnings / dq_status / failure_reason
-- Parameters: $as_of (timestamp string, "now" for the future-date check).

CREATE OR REPLACE TABLE stg_customers AS
WITH cur AS (
    SELECT * FROM raw_customers WHERE _batch_id = (SELECT max(_batch_id) FROM raw_customers)
),
t AS (
    SELECT cur.*,
           trim(customer_id) AS cid,
           trim(email)       AS email_t,
           trim(phone)       AS phone_t,
           trim(full_name)   AS name_t,
           upper(substr(trim(loyalty_tier), 1, 1)) || lower(substr(trim(loyalty_tier), 2)) AS tier_c,
           CASE WHEN regexp_full_match(trim(signup_date), '\d{4}-\d{2}-\d{2}')
                THEN CAST(try_strptime(trim(signup_date), '%Y-%m-%d') AS DATE) END AS signup_d,
           row_number() OVER (
               PARTITION BY CASE WHEN trim(customer_id) = '' THEN '#' || _source_row_num ELSE trim(customer_id) END
               ORDER BY _source_row_num) AS rn
    FROM cur
),
f AS (
    SELECT t.*,
           list_filter([
               CASE WHEN cid = ''  THEN 'CUS_CUSTOMER_ID_NOT_NULL' END,
               CASE WHEN rn > 1    THEN 'CUS_CUSTOMER_ID_UNIQUE' END
           ], x -> x IS NOT NULL) AS dq_critical,
           list_filter([
               CASE WHEN trim(city) = '' THEN 'CUS_CITY_NOT_NULL' END,
               CASE WHEN NOT regexp_full_match(email_t, '[^@\s]+@[^@\s]+\.[^@\s]+') THEN 'CUS_EMAIL_FORMAT' END,
               CASE WHEN NOT regexp_full_match(phone_t, '\d{10}') THEN 'CUS_PHONE_10_DIGITS' END,
               CASE WHEN NOT regexp_full_match(trim(pincode), '\d{6}') THEN 'CUS_PINCODE_6_DIGITS' END,
               CASE WHEN upper(trim(is_active)) NOT IN ('Y', 'N') THEN 'CUS_IS_ACTIVE_VALID' END,
               CASE WHEN signup_d IS NULL THEN 'CUS_SIGNUP_DATE_VALID' END,
               CASE WHEN tier_c IN ('Bronze', 'Silver', 'Gold', 'Platinum') AND loyalty_tier <> tier_c
                    THEN 'CUS_LOYALTY_TIER_CASING' END,
               CASE WHEN tier_c NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN 'CUS_LOYALTY_TIER_VALID' END,
               CASE WHEN signup_d IS NOT NULL AND signup_d > CAST(CAST($as_of AS TIMESTAMP) AS DATE) THEN 'CUS_SIGNUP_DATE_NOT_FUTURE' END
           ], x -> x IS NOT NULL) AS dq_warnings
    FROM t
)
SELECT
    cid AS customer_id,
    -- PII masking (same rules as include/nammamart/masking.py)
    CASE WHEN name_t IN ('', 'N/A') THEN NULL
         ELSE array_to_string(list_transform(string_split_regex(name_t, '\s+'), x -> upper(x[1]) || '.'), ' ') END AS customer_name_masked,
    CASE WHEN email_t IN ('', 'N/A') THEN NULL
         WHEN position('@' IN email_t) > 0
              THEN substr(split_part(email_t, '@', 1), 1, 2) || '***@' || substr(email_t, position('@' IN email_t) + 1)
         ELSE substr(email_t, 1, 2) || '***' END AS email_masked,
    CASE WHEN phone_t IN ('', 'N/A') THEN NULL
         WHEN length(phone_t) < 5 THEN repeat('*', length(phone_t))
         ELSE substr(phone_t, 1, 2) || repeat('*', length(phone_t) - 4) || substr(phone_t, length(phone_t) - 1) END AS phone_masked,
    CASE WHEN trim(city) = '' THEN 'UNKNOWN' ELSE trim(city) END AS city,
    trim(area) AS area,
    CASE WHEN regexp_full_match(trim(pincode), '\d{6}') THEN trim(pincode) END AS pincode,
    CASE WHEN signup_d IS NOT NULL AND signup_d <= CAST(CAST($as_of AS TIMESTAMP) AS DATE) THEN signup_d END AS signup_date,
    CASE WHEN tier_c IN ('Bronze', 'Silver', 'Gold', 'Platinum') THEN tier_c ELSE 'UNKNOWN' END AS loyalty_tier,
    CASE WHEN upper(trim(is_active)) IN ('Y', 'N') THEN upper(trim(is_active)) END AS is_active,
    dq_critical,
    dq_warnings,
    CASE WHEN len(dq_critical) > 0 THEN 'FAIL' WHEN len(dq_warnings) > 0 THEN 'WARN' ELSE 'PASS' END AS dq_status,
    NULLIF(array_to_string(dq_critical, '; '), '') AS failure_reason,
    NULLIF(array_to_string(dq_warnings, '; '), '') AS warning_reason,
    rn > 1 AS is_duplicate_copy,
    _batch_id, _source_file, _source_row_num, _run_id, _loaded_at
FROM f;
