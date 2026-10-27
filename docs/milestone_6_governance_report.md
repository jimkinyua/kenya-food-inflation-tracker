# Milestone 6: Data Quality, Governance & Lineage Audit Report
**Individual Capstone Project**: Data Platform for Analyzing Kenya's Food Prices and Inflation Trends  
**Student**: James Kinyua  
**Repository**: [github.com/jimkinyua/kenya-food-market-analytics](https://github.com/jimkinyua/kenya-food-market-analytics)  
**Track**: Data Engineering (Cohort 7)  
**Execution Environment**: Live Production VPS (Ubuntu 24.04 LTS / PostgreSQL 16 / Apache Airflow 3.x)

---

## 1. Executive Summary & Quality Scorecard
All **28,463 records** ingested from the Kenya National Bureau of Statistics (KNBS) / UN World Food Programme (WFP) open data repository were evaluated against 6 automated deterministic quality checks.

```
======================================================================
AUTOMATED DATA QUALITY ASSERTION MATRIX (100% PASS RATE)
======================================================================
                 Check Name     Target Table                                                                   Assertion Rule Status  Failed Rows
check_reconciliation_parity fact_food_prices          COUNT(staging.raw_wfp_food_prices) == COUNT(analytics.fact_food_prices) PASSED            0
      check_positive_prices fact_food_prices                                    normalized_price_per_kg > 0 AND raw_price > 0 PASSED            0
 check_no_null_foreign_keys fact_food_prices     date_id IS NOT NULL AND commodity_id IS NOT NULL AND location_id IS NOT NULL PASSED            0
check_geographic_boundaries     dim_location latitude BETWEEN -5.0 AND 5.5 AND longitude BETWEEN 33.5 AND 42.5 (Inside Kenya) PASSED            0
check_official_county_codes     dim_location                                                     county_code BETWEEN 1 AND 47 PASSED            0
    check_temporal_validity         dim_date                                                       year BETWEEN 2006 AND 2026 PASSED            0
======================================================================
```

---

## 2. Dataset Metadata Tagging & Governance Registry
Metadata tagging was formalized into a persistent governance catalog: `governance.source_metadata_catalog`.

- **Source Name**: `Kenya Food Market Prices (KNBS/MoA)`
- **Custodian Organization**: `Kenya National Bureau of Statistics (KNBS) & Ministry of Agriculture and Livestock Development (MoA)`
- **Open Data Portal**: `Humanitarian Data Exchange (HDX / UN WFP)`
- **Update Frequency**: `Monthly (Surveyed on the 15th of each month)`
- **Licensing Terms**: `Creative Commons Attribution for Intergovernmental Organisations (CC BY-IGO)`
- **Format**: `CSV / REST API endpoint`
- **Data Steward**: `James Kinyua (Data Engineering)`
- **Catalog DDL**:
```sql
CREATE TABLE IF NOT EXISTS governance.source_metadata_catalog (
    source_id               SERIAL PRIMARY KEY,
    source_name             VARCHAR(100) UNIQUE NOT NULL,
    custodian_organization  VARCHAR(150) NOT NULL,
    portal_name             VARCHAR(100) NOT NULL,
    source_url              TEXT NOT NULL,
    update_frequency        VARCHAR(50) NOT NULL,
    licensing_terms         VARCHAR(100) NOT NULL,
    data_format             VARCHAR(20) NOT NULL,
    validation_steps        TEXT NOT NULL,
    governance_steward      VARCHAR(100) DEFAULT 'James Kinyua (Data Engineering)',
    registered_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 3. Data Lineage Trace (End-to-End Auditability)
Every grain of data in the analytical data mart is traceable to its original bronze staging row, the pipeline run that executed it, and the data catalog definition:

```
--- SAMPLE LINEAGE TRACE RECORD (Auditing White Maize in Turkana County) ---
1. CUSTODIAN METADATA : Kenya Food Market Prices (KNBS/MoA) | Portal: Humanitarian Data Exchange (HDX / UN WFP) | Cadence: Monthly (Surveyed on the 15th of each month)
2. RAW STAGING LAYER  : Staging Row ID #6603 | Date: 2020-12-15 | Raw Market: Lodwar (Turkana) | Raw Unit: KG | Raw Price: KES 65.00
3. PIPELINE LINEAGE   : Orchestrated by 'kenya_food_prices_etl_pipeline' | Status: PASSED
4. ANALYTICS MART     : Fact ID #18544 | Standardized Price: KES 65.00 / KG
5. DIMENSIONAL CONTEXT: County: Turkana (Code: 023) | Coordinates: (3.120000, 35.600000) | Season: Off-Season
```

---

## 4. Anomaly Handling & Defensive Engineering
1. **Administrative Nulls Imputation**:
   - The historical market `Hola (Tana River)` had null county, province, and coordinates.
   - *Resolution*: Imputed with Province: `Coast`, County: `Tana River`, Latitude: `-1.498`, Longitude: `40.033`.
2. **Historical Sub-District Mapping**:
   - Raw records used historical district names (`Moyale`, `Ijara`, `Meru North`, `Meru South`).
   - *Resolution*: Deterministically mapped to official 47 Kenyan County Codes: `Marsabit (10)`, `Garissa (7)`, `Meru (12)`, and `Tharaka Nithi (13)`.
3. **Packaging Disparities**:
   - Converted heterogeneous bulk agricultural packaging (e.g., `90 KG bag`, `50 KG bag`, `64 KG bag`) to standard `normalized_price_per_kg`.
4. **Automated Pipeline Retry & Idempotency**:
   - Airflow DAG enforces exponential backoff (3 retries, 15s interval) and upsert idempotency (`ON CONFLICT DO UPDATE`).
