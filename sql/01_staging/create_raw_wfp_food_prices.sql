-- ============================================================================
-- Schema: staging
-- Table : raw_wfp_food_prices
-- Description: Raw, immutable landing table preserving 100% of source columns
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.raw_wfp_food_prices (
    raw_id          BIGSERIAL PRIMARY KEY,
    date            DATE NOT NULL,
    admin1          VARCHAR(100),
    admin2          VARCHAR(100),
    market          VARCHAR(100),
    market_id       INT,
    latitude        NUMERIC(9, 6),
    longitude       NUMERIC(9, 6),
    category        VARCHAR(100),
    commodity       VARCHAR(100),
    commodity_id    INT,
    unit            VARCHAR(50),
    priceflag       VARCHAR(50),
    pricetype       VARCHAR(50),
    currency        VARCHAR(10),
    price           NUMERIC(12, 2),
    usdprice        NUMERIC(12, 4),
    ingested_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_date_market ON staging.raw_wfp_food_prices(date, market_id);
