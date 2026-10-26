-- ============================================================================
-- Analytical Queries: Answering Core Economic & Inflation Questions
-- Rubric Evidence for Milestone 4 (PostgreSQL Star Schema)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: National Price Trend for Core Staple (White Maize) Over Time
-- Question: How has the price of White Maize evolved across retail and wholesale?
-- ----------------------------------------------------------------------------
SELECT 
    d.year,
    d.month,
    d.month_name,
    f.price_type,
    ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_price_per_kg,
    COUNT(f.price_fact_id) AS total_market_surveys
FROM analytics.fact_food_prices f
JOIN analytics.dim_commodity c ON f.commodity_id = c.commodity_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
WHERE c.commodity_name = 'Maize (white)'
GROUP BY d.year, d.month, d.month_name, f.price_type
ORDER BY d.year DESC, d.month DESC, f.price_type;

-- ----------------------------------------------------------------------------
-- QUERY 2: County-Level Food Inflation Heatmap Ranking
-- Question: Which counties are experiencing the highest food inflation rate?
-- ----------------------------------------------------------------------------
SELECT 
    l.county_name,
    l.county_code,
    ROUND(AVG(f.yoy_inflation_pct), 2) AS avg_yoy_inflation_pct,
    ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_staple_price_per_kg,
    COUNT(f.price_fact_id) AS observations_count
FROM analytics.fact_food_prices f
JOIN analytics.dim_location l ON f.location_id = l.location_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
WHERE f.yoy_inflation_pct IS NOT NULL
  AND d.year >= 2023
GROUP BY l.county_name, l.county_code
ORDER BY avg_yoy_inflation_pct DESC
LIMIT 10;

-- ----------------------------------------------------------------------------
-- QUERY 3: Regional Price Arbitrage (Agricultural Breadbasket vs. Arid ASAL)
-- Question: Compare maize prices between surplus producers and deficit counties
-- ----------------------------------------------------------------------------
SELECT 
    d.year,
    l.county_name,
    CASE 
        WHEN l.county_name IN ('Trans Nzoia', 'Uasin Gishu', 'Nakuru') THEN 'Agricultural Surplus Zone'
        WHEN l.county_name IN ('Turkana', 'Garissa', 'Mandera', 'Wajir', 'Marsabit') THEN 'Arid / Deficit Zone (ASAL)'
        ELSE 'Other County'
    END AS zone_classification,
    c.commodity_name,
    ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_price_per_kg
FROM analytics.fact_food_prices f
JOIN analytics.dim_commodity c ON f.commodity_id = c.commodity_id
JOIN analytics.dim_location l ON f.location_id = l.location_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
WHERE c.commodity_name IN ('Maize (white)', 'Beans (dry)')
  AND l.county_name IN ('Trans Nzoia', 'Uasin Gishu', 'Turkana', 'Garissa', 'Mandera')
  AND d.year >= 2022
GROUP BY d.year, l.county_name, zone_classification, c.commodity_name
ORDER BY c.commodity_name, d.year DESC, avg_price_per_kg ASC;

-- ----------------------------------------------------------------------------
-- QUERY 4: Seasonality Impact (Harvest Season vs. Non-Harvest Price Comparison)
-- Question: Do food staple prices drop during Kenya's Long Rains harvest season?
-- ----------------------------------------------------------------------------
SELECT 
    c.commodity_name,
    CASE 
        WHEN d.is_harvest_season THEN 'Harvest Season (Aug-Oct & Jan-Feb)'
        ELSE 'Off-Season / Planting Period'
    END AS seasonal_period,
    ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_price_per_kg,
    ROUND(AVG(f.mom_price_change_pct), 2) AS avg_mom_price_change_pct
FROM analytics.fact_food_prices f
JOIN analytics.dim_commodity c ON f.commodity_id = c.commodity_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
WHERE c.is_cpi_staple = TRUE
GROUP BY c.commodity_name, seasonal_period
ORDER BY c.commodity_name, seasonal_period;
