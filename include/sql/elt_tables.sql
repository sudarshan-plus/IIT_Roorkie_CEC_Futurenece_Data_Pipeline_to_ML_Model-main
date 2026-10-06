-- include/sql/elt_tables.sql
-- ELT layer DDL (created once). Raw = exact text copy of each CSV + load metadata. Nothing here is ever
-- cleaned or deleted. Columns after the business columns:
--   _loaded_at / _source_file / _run_id   required audit columns
--   _batch_id        which load of that table this row came from (later batch = newer information)
--   _source_row_num  the row's position in its CSV (lineage: raw row -> staging row -> mart number)

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id VARCHAR, order_ts VARCHAR, customer_id VARCHAR, store_id VARCHAR, product_id VARCHAR,
    quantity VARCHAR, unit_price VARCHAR, discount_pct VARCHAR, order_amount VARCHAR, payment_mode VARCHAR,
    order_status VARCHAR, channel VARCHAR, delivery_minutes VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _batch_id BIGINT, _source_row_num BIGINT
);
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id VARCHAR, order_id VARCHAR, payment_ts VARCHAR, payment_mode VARCHAR, amount_paid VARCHAR,
    payment_status VARCHAR, gateway_ref VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _batch_id BIGINT, _source_row_num BIGINT
);
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id VARCHAR, full_name VARCHAR, email VARCHAR, phone VARCHAR, city VARCHAR, area VARCHAR,
    pincode VARCHAR, signup_date VARCHAR, loyalty_tier VARCHAR, is_active VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _batch_id BIGINT, _source_row_num BIGINT
);
CREATE TABLE IF NOT EXISTS raw_products (
    product_id VARCHAR, product_name VARCHAR, category VARCHAR, sub_category VARCHAR, brand VARCHAR,
    mrp VARCHAR, cost_price VARCHAR, unit VARCHAR, is_perishable VARCHAR, launch_date VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _batch_id BIGINT, _source_row_num BIGINT
);
CREATE TABLE IF NOT EXISTS raw_stores (
    store_id VARCHAR, store_name VARCHAR, zone VARCHAR, area VARCHAR, pincode VARCHAR, open_date VARCHAR,
    manager_name VARCHAR, capacity_orders_per_day VARCHAR,
    _loaded_at TIMESTAMP, _source_file VARCHAR, _run_id VARCHAR, _batch_id BIGINT, _source_row_num BIGINT
);

-- Which files have been loaded (by content hash). Loading an identical file again is skipped, so a re-run
-- never duplicates raw rows; a changed or new file is appended as a new batch.
CREATE TABLE IF NOT EXISTS raw_file_registry (
    batch_id     BIGINT,
    table_name   VARCHAR,
    source_file  VARCHAR,
    file_sha256  VARCHAR,
    rows_loaded  BIGINT,
    run_id       VARCHAR,
    loaded_at    TIMESTAMP
);

-- The data-quality rulebook (seeded from include/nammamart/dq_catalog.py every run).
CREATE TABLE IF NOT EXISTS dq_check_catalog (
    check_name    VARCHAR,
    source_table  VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    description   VARCHAR
);

-- Staging for orders is the MERGE target: one row per order_id, ALL statuses incl. rejected rows (dq_status = 'FAIL').
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id          VARCHAR,
    order_ts          TIMESTAMP,
    customer_id       VARCHAR,
    store_id          VARCHAR,
    product_id        VARCHAR,
    quantity          INTEGER,
    unit_price        DECIMAL(12, 2),
    discount_pct      DECIMAL(6, 2),
    order_amount      DECIMAL(12, 2),
    payment_mode      VARCHAR,
    order_status      VARCHAR,
    channel           VARCHAR,
    delivery_minutes  INTEGER,
    dq_critical       VARCHAR[],
    dq_warnings       VARCHAR[],
    dq_status         VARCHAR,        -- PASS | WARN | FAIL
    failure_reason    VARCHAR,
    warning_reason    VARCHAR,
    _batch_id         BIGINT,
    _source_file      VARCHAR,
    _source_row_num   BIGINT,
    _run_id           VARCHAR,
    _loaded_at        TIMESTAMP,
    row_hash          VARCHAR,
    first_loaded_at   TIMESTAMP,
    updated_at        TIMESTAMP
);

-- History of corrections: the old and new version of an order whose row changed in a later file.
CREATE TABLE IF NOT EXISTS stg_order_changes (
    order_id          VARCHAR,
    changed_at        TIMESTAMP,
    run_id            VARCHAR,
    source_file       VARCHAR,
    old_status        VARCHAR,
    new_status        VARCHAR,
    old_order_amount  DECIMAL(12, 2),
    new_order_amount  DECIMAL(12, 2),
    old_dq_status     VARCHAR,
    new_dq_status     VARCHAR
);

-- Business Q6: rows rejected, by run, file and reason (append per run, idempotent per run_id).
CREATE TABLE IF NOT EXISTS mart_dq_rejections (
    run_id          VARCHAR,
    run_date        DATE,
    source_file     VARCHAR,
    failure_reason  VARCHAR,
    rows_rejected   BIGINT
);
