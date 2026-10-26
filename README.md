# Kenya Food Prices & Inflation Trends Data Platform

A production-grade, end-to-end Data Engineering platform built to ingest, clean, model, and analyze retail and wholesale food prices across Kenyan counties, detect regional inflation patterns, and generate actionable insights for policymakers, farmers, and consumers.

---

## 1. Project Background & Economic Context

* **Food Inflation Dominance**: In Kenya, food and non-alcoholic beverages constitute over **32.9%** of the Consumer Price Index (CPI) basket. Volatility in staple commodities (dry maize, beans, potatoes, cooking oil, milk) directly drives headline national inflation.
* **County-Level Disparities**: Kenya's 47 counties experience divergent agricultural realities. Agricultural surplus zones (Trans-Nzoia, Uasin Gishu, Nyandarua) contrast with Arid and Semi-Arid Lands (ASAL counties such as Turkana, Mandera, Marsabit) that suffer structural supply deficits, severe drought cycles, and high transport margins.
* **The Platform Objective**: Automate the collection of official ground-surveyed market records, normalize inconsistent packaging units (e.g., 90kg bags vs. per-kg retail), model data into an analytical Star Schema, and visualize inflation heatmaps and commodity price trends.

---

## 2. System Architecture (Native ELT Pattern)

The platform follows a modern **ELT (Extract $\rightarrow$ Load $\rightarrow$ Transform)** design running natively on a Linux VPS managed via `systemd`:

```
                      ┌────────────────────────────────────────┐
                      │          External Data Source          │
                      │  WFP / HDX Kenya Food Prices (CSV/API) │
                      │  (Collected by KNBS, MoA, and NDMA)    │
                      └───────────────────┬────────────────────┘
                                          │
                                          ▼
                      ┌────────────────────────────────────────┐
                      │       Orchestration Layer              │
                      │         Apache Airflow                 │
                      │  (Scheduled Ingestion, SLAs, Lineage)  │
                      └───────────────────┬────────────────────┘
                                          │
                                          ▼
  ┌───────────────────────────────────────────────────────────────────────────────┐
  │                           PostgreSQL Database Engine                          │
  │                                                                               │
  │  ┌───────────────────────┐   SQL Transform   ┌─────────────────────────────┐  │
  │  │    staging Schema     │──────────────────▶│      analytics Schema       │  │
  │  │ (Raw, Immutable Feed) │    (In-Engine)    │     (Kimball Star Schema)   │  │
  │  │ raw_wfp_food_prices   │                   │  dim_date, dim_commodity,   │  │
  │  └───────────────────────┘                   │  dim_location, fact_prices  │  │
  │              │                               └──────────────┬──────────────┘  │
  │              │ Audit & Lineage Logs                         │                 │
  │              ▼                                              ▼                 │
  │  ┌───────────────────────┐                   ┌─────────────────────────────┐  │
  │  │   governance Schema   │                   │      Serving Layer          │  │
  │  │  pipeline_audit_log   │                   │         Grafana             │  │
  │  │  data_quality_log     │                   │  (Geomaps, Trends, Gauges)  │  │
  │  └───────────────────────┘                   └─────────────────────────────┘  │
  └───────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Conceptual Data Model (The 4 Pillars)

The analytical warehouse (`analytics` schema) implements a Kimball Star Schema organized around the 4 fundamental pillars of economic inquiry:

```
                            ┌────────────────────────┐
                            │      1. DIM_DATE       │
                            │        (When?)         │
                            │  Day, Month, Year,     │
                            │  Harvest Season Flag   │
                            └───────────┬────────────┘
                                        │
                                        ▼
┌────────────────────────┐   ┌──────────────────────┐   ┌────────────────────────┐
│   2. DIM_COMMODITY     │   │ 4. FACT_FOOD_PRICES  │   │    3. DIM_LOCATION     │
│        (What?)         │   │     (How Much?)      │   │        (Where?)        │
│  Commodity Name,       │◀──│  Raw Price,          │──▶│  Market Name, County,  │
│  Category (Cereals),   │   │  Normalized Price/KG,│   │  County Code (001-047),│
│  CPI Basket Indicator  │   │  MoM Change %,       │   │  Latitude & Longitude  │
└────────────────────────┘   │  YoY Inflation %,    │   └────────────────────────┘
                             │  Wholesale/Retail    │
                             └──────────────────────┘
```

---

## 4. Data Sources & Provenance

* **Primary Source**: [Humanitarian Data Exchange (HDX) – Kenya Food Prices](https://data.humdata.org/dataset/wfp-food-prices-for-kenya).
* **Ground Data Custodians**:
  * Kenya National Bureau of Statistics (**KNBS**)
  * Ministry of Agriculture, Livestock & Fisheries (**MoA**)
  * National Drought Management Authority (**NDMA**)
* **Format**: Continuous historical time series (2006–Present), sub-national market observations across staple commodities (maize, beans, potatoes, milk, vegetables, cooking oil).

---

## 5. Repository Directory Layout

```text
├── README.md                  # Project overview, architecture, and concepts
├── .gitignore                 # Production ignore rules (secrets, venv, raw data)
├── docs/                      # Capstone technical documentation & defense notes
├── sql/                       # Modular SQL definitions and transformations
│   ├── 01_staging/            # DDL for raw landing tables
│   ├── 02_analytics/          # DDL for star schema (dims and fact)
│   └── 03_governance/         # DDL for audit logs and data quality assertions
├── scripts/                   # Extraction and transformation helper scripts
├── dags/                      # Apache Airflow pipeline definitions
└── grafana/                   # Grafana dashboard definitions and queries
```

---

## 6. Engineering Principles Followed

1. **Strategic Denormalization**: Operational hierarchies (province into county, food group into commodity) are denormalized into wide, descriptive dimensions to minimize multi-table join overhead during analytical querying.
2. **Idempotency**: All ingestion and transformation tasks are designed to be rerun safely for any time partition without creating duplicate records.
3. **Auditability & Lineage**: The raw data is landed untouched into `staging`, while metadata tracking (execution time, row counts, data quality assertions) is logged into `governance`.
