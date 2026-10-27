-- ============================================================================
-- Schema: governance
-- Description: Audit logging, metadata lineage, and automated quality testing
-- Milestone 6 Deliverable: Data Quality and Governance
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS governance;

-- ----------------------------------------------------------------------------
-- 1. SOURCE_METADATA_CATALOG (Source Metadata Tagging & Governance Registry)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.source_metadata_catalog (
    source_id               SERIAL PRIMARY KEY,
    source_name             VARCHAR(100) UNIQUE NOT NULL,
    custodian_organization  VARCHAR(150) NOT NULL,
    portal_name             VARCHAR(100) NOT NULL,
    source_url              TEXT NOT NULL,
    update_frequency        VARCHAR(50) NOT NULL,         -- e.g. 'Monthly (15th of each month)'
    licensing_terms         VARCHAR(100) NOT NULL,        -- e.g. 'CC BY-IGO (Open Data)'
    data_format             VARCHAR(20) NOT NULL,         -- e.g. 'CSV / REST API'
    validation_steps        TEXT NOT NULL,
    governance_steward      VARCHAR(100) DEFAULT 'James Kinyua (Data Engineering)',
    registered_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 2. PIPELINE_AUDIT_LOG (Operational Observability & Lineage Trace)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.pipeline_audit_log (
    log_id                  BIGSERIAL PRIMARY KEY,
    pipeline_name           VARCHAR(100) NOT NULL,
    run_id                  VARCHAR(100) NOT NULL,
    source_name             VARCHAR(100) NOT NULL,
    source_url              TEXT,
    raw_records_read        INT NOT NULL,
    clean_records_loaded    INT NOT NULL,
    execution_status        VARCHAR(20) NOT NULL,         -- 'PASSED' or 'FAILED'
    duration_seconds        NUMERIC(8, 2) NOT NULL,
    error_message           TEXT,
    executed_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ----------------------------------------------------------------------------
-- 3. DATA_QUALITY_LOG (Automated Quality Checks & Assertions)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.data_quality_log (
    check_id                BIGSERIAL PRIMARY KEY,
    run_id                  VARCHAR(100) NOT NULL,
    check_name              VARCHAR(100) NOT NULL,
    table_name              VARCHAR(100) NOT NULL,
    column_name             VARCHAR(100),
    assertion_rule          TEXT NOT NULL,
    status                  VARCHAR(20) NOT NULL,         -- 'PASSED' or 'FAILED'
    failed_rows_count       INT DEFAULT 0,
    checked_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
