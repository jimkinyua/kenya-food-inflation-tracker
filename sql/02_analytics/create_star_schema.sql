-- ============================================================================
-- Schema: analytics
-- Architecture: Kimball Star Schema
-- Description: Analytical Data Mart optimized for Grafana and economic queries
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- ----------------------------------------------------------------------------
-- 1. DIM_DATE (The "When" Pillar)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id             INT PRIMARY KEY,              -- Format: YYYYMMDD (e.g. 20240115)
    full_date           DATE UNIQUE NOT NULL,
    year                INT NOT NULL,
    month               INT NOT NULL,
    quarter             INT NOT NULL,
    month_name          VARCHAR(20) NOT NULL,
    is_harvest_season   BOOLEAN NOT NULL              -- True during Kenyan Long Rains (Aug-Oct) & Short Rains (Jan-Feb)
);

-- ----------------------------------------------------------------------------
-- 2. DIM_COMMODITY (The "What" Pillar)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_commodity (
    commodity_id        SERIAL PRIMARY KEY,           -- Surrogate Key
    natural_id          INT UNIQUE NOT NULL,          -- Source ID from KNBS/WFP
    commodity_name      VARCHAR(100) NOT NULL,
    category            VARCHAR(100) NOT NULL,
    is_cpi_staple       BOOLEAN DEFAULT FALSE         -- Flags core CPI inflation basket items (Maize, Beans, Potatoes, Milk)
);

-- ----------------------------------------------------------------------------
-- 3. DIM_LOCATION (The "Where" Pillar)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_location (
    location_id         SERIAL PRIMARY KEY,           -- Surrogate Key
    natural_market_id   INT UNIQUE NOT NULL,          -- Source Market ID
    market_name         VARCHAR(100) NOT NULL,
    county_name         VARCHAR(100) NOT NULL,
    county_code         INT NOT NULL,                 -- Official Kenya County Code (001 - 047)
    former_province     VARCHAR(100) NOT NULL,
    latitude            NUMERIC(9, 6) NOT NULL,
    longitude           NUMERIC(9, 6) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 4. FACT_FOOD_PRICES (The "How Much" Pillar)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_food_prices (
    price_fact_id           BIGSERIAL PRIMARY KEY,
    
    -- Foreign Keys to Dimensions
    date_id                 INT NOT NULL REFERENCES analytics.dim_date(date_id),
    commodity_id            INT NOT NULL REFERENCES analytics.dim_commodity(commodity_id),
    location_id             INT NOT NULL REFERENCES analytics.dim_location(location_id),
    
    -- Quantitative Measurements (Facts)
    raw_price               NUMERIC(10, 2) NOT NULL,
    normalized_price_per_kg NUMERIC(10, 2) NOT NULL,
    mom_price_change_pct    NUMERIC(6, 2),            -- Month-over-Month %
    yoy_inflation_pct       NUMERIC(6, 2),            -- Year-over-Year Inflation %
    
    -- Degenerate Dimensions (Operational Attributes & Flags)
    raw_unit                VARCHAR(50) NOT NULL,
    standard_unit           VARCHAR(20) NOT NULL,     -- 'KG', 'Litre', 'Piece'
    price_type              VARCHAR(20) NOT NULL,     -- 'Wholesale' vs. 'Retail'
    price_flag              VARCHAR(20) NOT NULL,     -- 'actual' vs. 'aggregate'
    currency                VARCHAR(5) DEFAULT 'KES',
    
    -- Audit Timestamp
    loaded_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Uniqueness constraint for Idempotent Loads
    CONSTRAINT uq_fact_observation UNIQUE (date_id, commodity_id, location_id, price_type)
);

-- Optimized Analytical Indexes for Sub-Second Grafana Querying
CREATE INDEX IF NOT EXISTS idx_fact_date ON analytics.fact_food_prices(date_id);
CREATE INDEX IF NOT EXISTS idx_fact_commodity ON analytics.fact_food_prices(commodity_id);
CREATE INDEX IF NOT EXISTS idx_fact_location ON analytics.fact_food_prices(location_id);
CREATE INDEX IF NOT EXISTS idx_fact_type ON analytics.fact_food_prices(price_type);
