-- include/sql/audit_tables.sql
-- Observability tables shared by the ETL and ELT DAGs (created once, then only appended to / upserted).

-- One row per (run, check): the data-quality scorecard over time (Part D).
CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,      -- PASS | WARN (Warning check failed) | FAIL (Critical check failed)
    checked_at    TIMESTAMP
);

-- One row per (run, DAG): "did today's run work, and if not, why?" (Part F).
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMP,
    end_time          TIMESTAMP,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR,   -- SUCCESS | FAILED
    failed_tasks      VARCHAR,
    notes             VARCHAR
);
