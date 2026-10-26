-- ============================================================================
-- Schema: governance
-- Description: Audit logging, metadata lineage, and automated quality testing
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS governance;

-- ----------------------------------------------------------------------------
-- 1. PIPELINE_AUDIT_LOG (Operational Observability & Lineage)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.pipeline_audit_log (
    log_id              BIGSERIAL PRIMARY KEY,
    pipeline_name       VARCHAR(100) NOT NULL,
    run_id              VARCHAR(100) NOT NULL,        -- Airflow DAG run ID or CLI timestamp
    source_name         VARCHAR(100) NOT NULL,
    source_url          TEXT,
    raw_records_read    INT NOT NULL,
    clean_records_loaded INT NOT NULL,
    execution_status    VARCHAR(20) NOT NULL,         -- 'SUCCESS' or 'FAILED'
    duration_seconds    NUMERIC(8, 2) NOT NULL,
    error_message       TEXT,
    executed_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. DATA_QUALITY_LOG (Automated Quality Checks & Assertions)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.data_quality_log (
    check_id            BIGSERIAL PRIMARY KEY,
    run_id              VARCHAR(100) NOT NULL,
    check_name          VARCHAR(100) NOT NULL,        -- e.g. 'check_no_null_prices'
    table_name          VARCHAR(100) NOT NULL,
    column_name         VARCHAR(100),
    assertion_rule      TEXT NOT NULL,
    status              VARCHAR(20) NOT NULL,         -- 'PASSED' or 'FAILED'
    failed_rows_count   INT DEFAULT 0,
    checked_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
