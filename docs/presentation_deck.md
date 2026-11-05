# Presentation Deck: Kenya Food Market Analytics Pipeline

**Author:** Jim Kinyua  
**Role:** Data Engineer  
**Live Platform:** http://161.97.156.126:3000  
**Repository:** https://github.com/jimkinyua/kenya-food-market-analytics  

---

## Slide 1: Executive Summary & Project Vision

- **Context:** Food security and price inflation in Kenya are heavily affected by seasonal harvests, transport friction across 47 counties, and global commodity shocks.
- **The Challenge:** Official agricultural price data from KNBS and the Ministry of Agriculture was stored in fragmented, un-normalized CSVs with mixed packaging units (90kg bags, bunches, tins, liters), missing GPS coordinates, and no automated pipeline.
- **The Solution:** A production-grade end-to-end Data Engineering platform combining Apache Airflow, PostgreSQL 15 Kimball Star Schema, and Grafana for automated monthly ingestion, quality governance, and economic monitoring.

---

## Slide 2: Pipeline Architecture

```text
  [ Data Sources ]
  - KNBS Monthly Market Price Surveys
  - Ministry of Agriculture via UN WFP / HDX (28,463 records)
          │
          ▼
  [ Extraction & Validation ]
  - HTTP health verification (status 200)
  - MD5 payload checksum verification (4bff3219a5743fa8110d08ba15af3010)
          │
          ▼
  [ Cleaning & Transformation ]
  - Unit normalization to standard KG / Liter
  - County code mapping (47 counties) and GPS imputation
          │
          ▼
  [ Analytics Warehouse (Kimball Star Schema) ]
  - PostgreSQL 15 (analytics schema)
  - Fact table: fact_food_prices (28,463 rows, idempotent upsert)
  - Dimensions: dim_date, dim_commodity, dim_location
          │
          ▼
  [ Orchestration & Governance ]
  - Apache Airflow 2.8 (Cron: 0 6 15 * *)
  - Automated audit logging (governance.data_quality_audit_log)
          │
          ▼
  [ Business Intelligence & Analytics ]
  - Hosted Grafana 10 Dashboard (Trends, heatmaps, regional margins)
```

---

## Slide 3: Kimball Star Schema Design

The warehouse is optimized for high-performance OLAP queries and multi-dimensional analysis:

1. **`dim_date` (When):**
   - 249 monthly periods (2006–2024).
   - Rich seasonal attributes: calendar year, month, quarter, and boolean flags for **Long Rains Harvest** (Aug–Oct) and **Short Rains Harvest** (Jan–Feb).
2. **`dim_commodity` (What):**
   - 51 unique agricultural items across cereals, pulses, vegetables, livestock, and oils.
   - CPI food basket category classification.
3. **`dim_location` (Where):**
   - 226 sub-national wholesale and retail market hubs.
   - Mapped to Kenya's 47 counties, former provinces, and GPS coordinates.
4. **`fact_food_prices` (How Much):**
   - 28,463 price observation rows.
   - Metrics: standardized price per KG/L, raw recorded price, and monthly inflation metrics.
   - Idempotency enforced via UNIQUE constraint on `(date_id, commodity_id, location_id, price_type)` with `ON CONFLICT DO UPDATE`.

---

## Slide 4: Airflow Automation & Resilience

- **Cadence:** Scheduled at `0 6 15 * *` (06:00 AM on the 15th of each month, aligning with KNBS monthly release bulletins).
- **Task Pipeline:**
  `check_source_health >> extract_raw_data >> transform_clean_normalise >> load_postgres_warehouse >> validate_data_quality_and_audit`
- **Fault Tolerance:**
  - Automated retries with exponential backoff (`retries=3`, `retry_delay=15s`).
  - Tested failure recovery: Successfully caught simulated remote connection timeouts, held state in `UP_FOR_RETRY`, and succeeded on retry without data corruption.

---

## Slide 5: Data Quality & Governance Framework

- **Audit Logging:** Every DAG execution logs directly to `governance.data_quality_audit_log` with batch ID, row count, checksum, and execution duration.
- **Automated Assertions:**
  - Reconciliation check: `staging.raw_wfp_food_prices` (28,463 rows) == `analytics.fact_food_prices` (28,463 rows).
  - Business rules: Zero negative or zero prices (`price_per_kg > 0`).
  - Referential integrity: 100% of fact records match valid surrogate keys in all 3 dimension tables.

---

## Slide 6: Key Findings & Economic Insights

1. **Harvest Seasonality Dynamics:**
   - Supply increases during Kenya's dual harvest windows consistently reduce consumer food prices.
   - White Maize prices drop by 2.7% during harvest peaks; Irish Potatoes drop by 9.1%.
2. **Sub-National Price Disparities:**
   - Arid and semi-arid counties in Northern Kenya (Turkana, Mandera, Marsabit, Wajir) pay a 35% to 50% price premium on staple grains compared to Rift Valley breadbasket counties.
   - Cause: Road infrastructure deficits, transport fuel surcharges, and climate shocks.
3. **Wholesale vs. Retail Margins:**
   - Analysis revealed a consistent 40% to 65% retail markup over wholesale prices across urban centers, reflecting retail logistics, storage losses, and intermediary margins.

---

## Slide 7: Live Grafana Dashboard Walkthrough

Hosted live at: **http://161.97.156.126:3000**

- **KPI Metric Banners:** Immediate visibility into latest staple grain prices, month-over-month inflation changes, and active market coverage.
- **Time-Series Commodity Explorer:** Visualizes 18-year price historical trends with dynamic filters by market, county, and commodity.
- **47-County Spatial Heatmap:** Highlights geographic price inflation hotspots across Kenya.
- **Wholesale/Retail Price Spread Panels:** Compares regional trading margins.

---

## Slide 8: Technical Impact & Next Steps

- **Impact:** Transformed slow, manual, quarterly reporting into a fully automated, auditable monthly data lakehouse and analytics platform.
- **Potential Extensions:**
  - Integration of real-time mobile commodity price SMS feeds via Kafka.
  - Predictive ML price forecasting models based on seasonal rainfall anomalies (NDVI data).
