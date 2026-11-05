# Data Source Profile & Provenance Specification

**Project**: Data Platform for Analyzing Kenya's Food Prices and Inflation Trends  
**Repository**: [github.com/jimkinyua/kenya-food-market-analytics](https://github.com/jimkinyua/kenya-food-market-analytics)  
**Author**: James Kinyua (Data Engineering)  
**Document**: `docs/source_profile.md`  

---

## 1. Executive Overview & Institutional Provenance

### Why the WFP/HDX Dataset Represents KNBS and Ministry of Agriculture Data
The capstone specification identifies **Kenya National Bureau of Statistics (KNBS)**, **Ministry of Agriculture (MoA)**, **World Bank**, and **Kenya Open Data Initiative (KODI)** as primary authoritative sources for food price monitoring in Kenya. 

The dataset utilized in this engineering platform—hosted on the **Humanitarian Data Exchange (HDX)** by the **United Nations World Food Programme (UN WFP) Vulnerability Analysis and Mapping (VAM)** unit—is the official, consolidated digital distribution of market price data collected by Kenyan government enumerators:

1. **Kenya National Bureau of Statistics (KNBS)**:  
   KNBS conducts regular ground price surveys across retail outlets in urban and rural markets to compile the national Consumer Price Index (CPI), where food and non-alcoholic beverages represent **32.9%** of the CPI basket.
2. **Ministry of Agriculture and Livestock Development (MoA)**:  
   The Ministry's **Kenya Agricultural Market Information System (KAMIS / AMIS)** tracks daily and weekly wholesale and retail commodity prices across agricultural surplus hubs (e.g., Eldoret, Kitale, Nakuru, Nyandarua) and major consumption terminals (e.g., Nairobi, Mombasa, Kisumu).
3. **National Drought Management Authority (NDMA)**:  
   NDMA operates monthly food security and livestock price early warning surveillance across Kenya's 23 Arid and Semi-Arid Land (ASAL) counties (e.g., Turkana, Marsabit, Mandera, Wajir, Garissa).

**Consolidation Rationale**:  
Direct portals operated by national ministries often deliver data locked in disparate PDF bulletins, intermittent Excel spreadsheets, or fragmented web forms with frequent schema drift. The WFP VAM team, operating under bilateral agreements with KNBS, MoA, and NDMA, systematically ingests, validates, and unifies these official Kenyan field surveys into a continuous, machine-readable open-data feed published on HDX. By ingesting this dataset, our platform processes the authentic ground data collected by KNBS, MoA, and NDMA with 20+ years of historical continuity.

---

## 2. Source Metadata Registry

| Attribute | Specification |
| :--- | :--- |
| **Dataset Title** | Kenya - Food Prices (WFP / KNBS / MoA Market Monitor) |
| **Data Custodians** | Kenya National Bureau of Statistics (KNBS) & Ministry of Agriculture |
| **Publishing Portal** | Humanitarian Data Exchange (HDX) / UN WFP VAM |
| **Permanent URL** | `https://data.humdata.org/dataset/wfp-food-prices-for-kenya` |
| **Download Resource** | `wfp_food_prices_ken.csv` |
| **Licensing Terms** | **Creative Commons Attribution for Intergovernmental Organisations (CC BY-IGO)** — Permits free copying, distribution, adaptation, and commercial use with attribution. |
| **Update Cadence** | **Monthly** (Surveyed mid-month on the 15th; updated between the 15th and 25th) |
| **File Format** | Comma-Separated Values (`text/csv`, UTF-8 encoded) |
| **Ingested File Size** | ~3.52 MB (3,694,182 bytes) |
| **Observed Row Count** | **28,463 data records** (+ 1 header row) |
| **Temporal Coverage** | **2006-01-15 to 2026-09-15** (249 continuous survey months / 20+ years) |
| **Spatial Coverage** | **226 unique market centers** across **27 surveyed administrative areas / 47 counties** |
| **Cryptographic Hash** | MD5: `4bff3219a5743fa8110d08ba15af3010` |

---

## 3. Schema Definition & Attribute Data Types

The raw landing file contains **16 columns**. Below is the exact observed schema, storage types, null counts, and business semantics:

| # | Column Name | Raw Data Type | PostgreSQL Target | Null Count | Description & Business Meaning |
| :-: | :--- | :--- | :--- | :-: | :--- |
| 1 | `date` | `VARCHAR(10)` | `DATE` | 0 | Survey date, recorded as the 15th of each month (`YYYY-MM-DD`). |
| 2 | `admin1` | `VARCHAR(50)` | `VARCHAR(50)` | 68 | First-level administrative unit (Historical Province, e.g., Rift Valley, Coast). |
| 3 | `admin2` | `VARCHAR(50)` | `VARCHAR(50)` | 68 | Second-level administrative unit (County or historical district). |
| 4 | `market` | `VARCHAR(100)`| `VARCHAR(100)`| 0 | Name of the surveyed physical market hub (e.g., Lodwar, Kitale, Machakos). |
| 5 | `market_id` | `INT` | `INT` | 0 | WFP unique natural surrogate identifier for the market. |
| 6 | `latitude` | `FLOAT` | `NUMERIC(9,6)` | 68 | WGS84 GPS Latitude of the market hub center. |
| 7 | `longitude` | `FLOAT` | `NUMERIC(9,6)` | 68 | WGS84 GPS Longitude of the market hub center. |
| 8 | `category` | `VARCHAR(50)` | `VARCHAR(50)` | 0 | Broad food classification (cereals and tubers, pulses and nuts, meat, etc.). |
| 9 | `commodity` | `VARCHAR(100)`| `VARCHAR(100)`| 0 | Specific surveyed staple item (e.g., `Maize (white)`, `Beans (dry)`). |
| 10 | `commodity_id` | `INT` | `INT` | 0 | Unique natural identifier for the commodity item. |
| 11 | `unit` | `VARCHAR(30)` | `VARCHAR(30)` | 0 | Original physical packaging measure (e.g., `90 KG`, `KG`, `500 ML`). |
| 12 | `priceflag` | `VARCHAR(30)` | `VARCHAR(30)` | 0 | Quality survey flag (`actual`, `aggregate`, or `actual,aggregate`). |
| 13 | `pricetype` | `VARCHAR(20)` | `VARCHAR(20)` | 0 | Transaction tier: `Retail` or `Wholesale`. |
| 14 | `currency` | `VARCHAR(10)` | `VARCHAR(10)` | 0 | Local pricing currency (always `KES` for Kenya). |
| 15 | `price` | `FLOAT` | `NUMERIC(10,2)`| 0 | Ground-surveyed price in Kenya Shillings (KES). |
| 16 | `usdprice` | `FLOAT` | `NUMERIC(10,4)`| 0 | Converted nominal price in United States Dollars (USD). |

---

## 4. Observed Data Quirks & Engineering Mitigations

During exploratory profiling of the 28,463 records, our pipeline identified five critical real-world data quirks. These required dedicated defensive engineering in the transformation pipeline (`scripts/transform_clean_food_prices.py`):

### Quirk 1: Packaging Unit Heterogeneity
* **Observation**: Staple food prices are recorded in disparate packaging standards depending on the supply chain level. Wholesale grains are recorded in **90 KG bags**, wholesale potatoes in **50 KG** or **64 KG bags**, retail milk in **500 ML sachets**, dry pulses in **KG**, and items like cabbage in **Heads** or **Bunches**.
* **Impact**: Comparing a KES 3,200 (90 KG bag) wholesale price directly against a KES 65 (per KG) retail price would distort calculations by orders of magnitude.
* **Engineering Mitigation**: Built a deterministic conversion lookup dictionary (`UNIT_CONVERSION_FACTORS`) that standardizes all weight packaging to 1 KG and all volume packaging to 1 Litre:
  $$\text{normalized\_price\_per\_kg} = \frac{\text{raw\_price}}{\text{conversion\_multiplier}}$$
  This produces a uniform `normalized_price_per_kg` column across all commodities for accurate time-series and cross-county price comparisons.

### Quirk 2: Missing Geographic Coordinates and Administrative Nulls
* **Observation**: Exactly **68 rows** in the dataset possess `NULL` values across `admin1`, `admin2`, `latitude`, and `longitude`.
* **Root Cause Investigation**: All 68 null records belong to a single physical market center: **`Hola (Tana River)`**.
* **Engineering Mitigation**: Implemented deterministic spatial imputation:
  - `admin1` $\rightarrow$ `'Coast'`
  - `admin2` $\rightarrow$ `'Tana River'`
  - `latitude` $\rightarrow$ `-1.498000`
  - `longitude` $\rightarrow$ `40.033000`  
  This preserves 100% of the historical survey records (0 records dropped) while ensuring the market plots accurately on GIS county maps.

### Quirk 3: Historical Administrative Boundaries vs. Modern 47 Counties
* **Observation**: Kenya promulgated a new Constitution in August 2010 that replaced the legacy 8 provinces and administrative districts with **47 devolved County governments**. In historical records prior to 2013, several markets recorded historical district names (`Moyale`, `Ijara`, `Meru North`, `Meru South`) rather than modern county designations.
* **Engineering Mitigation**: Implemented a canonical mapping dictionary (`DISTRICT_TO_COUNTY_NAME` and `COUNTY_CODE_MAP`) that maps every market to its modern constitutional county entity:
  - `Moyale` $\rightarrow$ Marsabit County (Code: `010`)
  - `Ijara` $\rightarrow$ Garissa County (Code: `007`)
  - `Meru North` $\rightarrow$ Meru County (Code: `012`)
  - `Meru South` $\rightarrow$ Tharaka Nithi County (Code: `013`)  
  Ensured every location record contains a valid official `county_code` strictly between 1 and 47.

### Quirk 4: Composite Price Flags
* **Observation**: The `priceflag` attribute was expected to be a discrete binary indicator (`actual` vs `aggregate`). However, 258 records contain composite strings: `'actual,aggregate'`.
* **Engineering Mitigation**: Standardized dirty price flags during data cleaning into canonical categories (`actual` = 18,737, `aggregate` = 9,726), eliminating parsing errors downstream.

### Quirk 5: Dual Currency Redundancy
* **Observation**: The dataset includes both `price` (KES) and `usdprice` (USD).
* **Engineering Mitigation**: Because `usdprice` introduces exchange rate volatility into domestic agricultural analysis, our analytical star schema standardizes on local currency (`KES`), while retaining `currency` in the metadata catalog for complete financial audibility.

---

## 5. Summary Profile Card

```text
================================================================================
KENYA FOOD MARKET DATASET SUMMARY PROFILE CARD
================================================================================
Source Name        : Kenya Food Market Prices (KNBS / MoA / NDMA)
Hosting Platform   : Humanitarian Data Exchange (HDX / UN WFP VAM)
Direct URL         : https://data.humdata.org/dataset/wfp-food-prices-for-kenya
Licence            : Creative Commons Attribution for IGOs (CC BY-IGO)
Cadence            : Monthly (Surveyed on the 15th of each month)
Raw File Size      : 3.52 MB
Total Records      : 28,463 rows (Header + 28,463 observations)
Date Range         : 2006-01-15 to 2026-09-15 (249 continuous months)
Geographic Breadth : 226 market hubs across Kenya's 47 counties
Staple Commodities : 51 commodities (Cereals, Pulses, Dairy, Meat, Vegetables)
Integrity MD5      : 4bff3219a5743fa8110d08ba15af3010
Cleaning Yield     : 28,463 clean rows (100% retention; 0 dropped rows)
================================================================================
```

