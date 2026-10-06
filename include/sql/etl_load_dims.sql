-- include/sql/etl_load_dims.sql
-- Idempotent upsert of the three dimensions from the transform output (registered as views
-- `batch_customer`, `batch_product`, `batch_store`). Re-running with the same data changes nothing but loaded_at.

MERGE INTO etl_dim_customer AS t
USING (
    SELECT customer_id, NULLIF(customer_name_masked, '') AS customer_name_masked,
           NULLIF(email_masked, '') AS email_masked, NULLIF(phone_masked, '') AS phone_masked, city, area,
           NULLIF(pincode, '') AS pincode,
           TRY_CAST(NULLIF(signup_date, '') AS DATE) AS signup_date,
           loyalty_tier, NULLIF(is_active, '') AS is_active, NULLIF(dq_flags, '') AS dq_flags,
           CAST(source_row_num AS INTEGER) AS source_row_num
    FROM batch_customer
) AS s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET
    customer_name_masked = s.customer_name_masked, email_masked = s.email_masked, phone_masked = s.phone_masked,
    city = s.city, area = s.area, pincode = s.pincode, signup_date = s.signup_date, loyalty_tier = s.loyalty_tier,
    is_active = s.is_active, dq_flags = s.dq_flags, source_row_num = s.source_row_num,
    etl_run_id = $run_id, loaded_at = current_timestamp::TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.customer_id, s.customer_name_masked, s.email_masked, s.phone_masked, s.city, s.area, s.pincode,
    s.signup_date, s.loyalty_tier, s.is_active, s.dq_flags, s.source_row_num, $run_id, current_timestamp::TIMESTAMP
);

-- SCD Type 2 (bonus): close the open row of a customer whose tier changed, then open a row for new/changed customers.
-- Idempotent: an unchanged tier touches nothing, so a re-run adds no history rows.
UPDATE etl_dim_customer_scd2 AS h
SET valid_to = current_timestamp::TIMESTAMP, is_current = false
FROM etl_dim_customer AS c
WHERE h.customer_id = c.customer_id AND h.is_current AND h.loyalty_tier <> c.loyalty_tier;

INSERT INTO etl_dim_customer_scd2
SELECT c.customer_id, c.loyalty_tier, current_timestamp::TIMESTAMP, NULL, true, $run_id
FROM etl_dim_customer AS c
LEFT JOIN etl_dim_customer_scd2 AS h ON h.customer_id = c.customer_id AND h.is_current
WHERE h.customer_id IS NULL;

MERGE INTO etl_dim_product AS t
USING (
    SELECT product_id, product_name, category, sub_category, brand,
           TRY_CAST(NULLIF(mrp, '') AS DECIMAL(12, 2)) AS mrp,
           TRY_CAST(NULLIF(cost_price, '') AS DECIMAL(12, 2)) AS cost_price,
           unit,
           TRY_CAST(NULLIF(is_perishable, '') AS BOOLEAN) AS is_perishable,
           TRY_CAST(NULLIF(launch_date, '') AS DATE) AS launch_date,
           NULLIF(dq_flags, '') AS dq_flags, CAST(source_row_num AS INTEGER) AS source_row_num
    FROM batch_product
) AS s ON t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
    product_name = s.product_name, category = s.category, sub_category = s.sub_category, brand = s.brand,
    mrp = s.mrp, cost_price = s.cost_price, unit = s.unit, is_perishable = s.is_perishable,
    launch_date = s.launch_date, dq_flags = s.dq_flags, source_row_num = s.source_row_num,
    etl_run_id = $run_id, loaded_at = current_timestamp::TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.product_id, s.product_name, s.category, s.sub_category, s.brand, s.mrp, s.cost_price, s.unit,
    s.is_perishable, s.launch_date, s.dq_flags, s.source_row_num, $run_id, current_timestamp::TIMESTAMP
);

MERGE INTO etl_dim_store AS t
USING (
    SELECT store_id, store_name, zone, area, NULLIF(pincode, '') AS pincode,
           TRY_CAST(NULLIF(open_date, '') AS DATE) AS open_date, manager_name,
           TRY_CAST(CAST(TRY_CAST(NULLIF(capacity_orders_per_day, '') AS DOUBLE) AS BIGINT) AS INTEGER) AS capacity_orders_per_day,
           NULLIF(dq_flags, '') AS dq_flags, CAST(source_row_num AS INTEGER) AS source_row_num
    FROM batch_store
) AS s ON t.store_id = s.store_id
WHEN MATCHED THEN UPDATE SET
    store_name = s.store_name, zone = s.zone, area = s.area, pincode = s.pincode, open_date = s.open_date,
    manager_name = s.manager_name, capacity_orders_per_day = s.capacity_orders_per_day, dq_flags = s.dq_flags,
    source_row_num = s.source_row_num, etl_run_id = $run_id, loaded_at = current_timestamp::TIMESTAMP
WHEN NOT MATCHED THEN INSERT VALUES (
    s.store_id, s.store_name, s.zone, s.area, s.pincode, s.open_date, s.manager_name, s.capacity_orders_per_day,
    s.dq_flags, s.source_row_num, $run_id, current_timestamp::TIMESTAMP
);
