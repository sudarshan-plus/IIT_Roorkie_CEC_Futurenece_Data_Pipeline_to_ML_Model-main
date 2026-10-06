-- include/sql/etl_tables.sql
-- ETL star schema in DuckDB. Created once (IF NOT EXISTS); every load is an idempotent upsert.
-- No raw email / phone / name column exists in any of these tables - they are masked before load.

CREATE TABLE IF NOT EXISTS etl_dim_customer (
    customer_id           VARCHAR PRIMARY KEY,
    customer_name_masked  VARCHAR,
    email_masked          VARCHAR,
    phone_masked          VARCHAR,
    city                  VARCHAR,
    area                  VARCHAR,
    pincode               VARCHAR,
    signup_date           DATE,
    loyalty_tier          VARCHAR,
    is_active             VARCHAR,
    dq_flags              VARCHAR,
    source_row_num        INTEGER,
    etl_run_id            VARCHAR,
    loaded_at             TIMESTAMP
);

-- BONUS - SCD Type 2: keeps the loyalty-tier HISTORY of each customer (the main dimension only holds the current tier).
-- One open row (is_current = true, valid_to NULL) per customer; a tier change closes it and opens a new one.
CREATE TABLE IF NOT EXISTS etl_dim_customer_scd2 (
    customer_id   VARCHAR,
    loyalty_tier  VARCHAR,
    valid_from    TIMESTAMP,
    valid_to      TIMESTAMP,
    is_current    BOOLEAN,
    etl_run_id    VARCHAR
);

CREATE TABLE IF NOT EXISTS etl_dim_product (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR,
    category        VARCHAR,
    sub_category    VARCHAR,
    brand           VARCHAR,
    mrp             DECIMAL(12, 2),
    cost_price      DECIMAL(12, 2),
    unit            VARCHAR,
    is_perishable   BOOLEAN,
    launch_date     DATE,
    dq_flags        VARCHAR,
    source_row_num  INTEGER,
    etl_run_id      VARCHAR,
    loaded_at       TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_dim_store (
    store_id                 VARCHAR PRIMARY KEY,
    store_name               VARCHAR,
    zone                     VARCHAR,
    area                     VARCHAR,
    pincode                  VARCHAR,
    open_date                DATE,
    manager_name             VARCHAR,
    capacity_orders_per_day  INTEGER,
    dq_flags                 VARCHAR,
    source_row_num           INTEGER,
    etl_run_id               VARCHAR,
    loaded_at                TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_fact_orders (
    order_id          VARCHAR PRIMARY KEY,
    order_ts          TIMESTAMP,
    order_date        DATE,
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
    net_revenue       DECIMAL(12, 2),   -- order_amount when DELIVERED, else 0
    gross_margin      DECIMAL(12, 2),   -- net_revenue - quantity x cost_price (DELIVERED only)
    within_15_min     BOOLEAN,          -- NULL unless DELIVERED
    row_hash          VARCHAR,
    source_file       VARCHAR,
    source_row_num    INTEGER,
    etl_run_id        VARCHAR,
    loaded_at         TIMESTAMP,
    updated_at        TIMESTAMP
);

-- Audit trail of corrections (e.g. DELIVERED -> RETURNED): lets finance see both the restated and the as-reported view.
CREATE TABLE IF NOT EXISTS etl_order_changes (
    order_id          VARCHAR,
    changed_at        TIMESTAMP,
    etl_run_id        VARCHAR,
    source_file       VARCHAR,
    old_status        VARCHAR,
    new_status        VARCHAR,
    old_order_amount  DECIMAL(12, 2),
    new_order_amount  DECIMAL(12, 2),
    old_net_revenue   DECIMAL(12, 2),
    new_net_revenue   DECIMAL(12, 2)
);

-- Every row that failed a Critical check (the full row is in the quarantine CSV; no PII is kept here).
CREATE TABLE IF NOT EXISTS etl_quarantine (
    run_id          VARCHAR,
    source_file     VARCHAR,
    source_row_num  INTEGER,
    row_key         VARCHAR,
    failure_reason  VARCHAR,
    quarantined_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_payment_reconciliation (
    run_id                VARCHAR,
    order_id              VARCHAR,
    payment_id            VARCHAR,
    order_date            DATE,
    order_status          VARCHAR,
    order_payment_mode    VARCHAR,
    payment_mode          VARCHAR,
    payment_status        VARCHAR,
    order_amount          DECIMAL(12, 2),
    amount_paid           DECIMAL(12, 2),
    unreconciled_amount   DECIMAL(12, 2),
    mismatch_reason       VARCHAR,
    payment_dq_status     VARCHAR,
    reconciled_at         TIMESTAMP
);
