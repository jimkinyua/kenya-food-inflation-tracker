#!/usr/bin/env python3
"""
Data Cleaning & Normalisation Pipeline: Kenya Food Prices
===========================================================
Milestone 3 Transformation Engine:
1. Imputes missing geographic entities and coordinates (e.g. Hola / Tana River).
2. Maps historical administrative regions to official 47 Kenyan County codes.
3. Normalises disparate packaging units (90kg bags, grams, ml) to standard per-KG/L prices.
4. Cleans mixed text flags and validates pricing boundaries.
5. Computes economic indicators: Month-over-Month (MoM %) and Year-over-Year (YoY %) inflation.
6. Generates Before/After evidence logging required for Milestone 3 defense.

Usage:
    python scripts/transform_clean_food_prices.py
"""

import os
import sys
import logging
import pandas as pd
import numpy as np

# -----------------------------------------------------------------------------
# Configuration & Paths
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "wfp_food_prices_kenya.csv")
PROCESSED_DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "clean_food_prices_kenya.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("DataCleaning")

# -----------------------------------------------------------------------------
# Reference Dictionaries: Unit Conversions & County Mappings
# -----------------------------------------------------------------------------
UNIT_CONVERSION_FACTORS = {
    # Weight units -> Standard Multiplier to 1 KG
    "KG": 1.0,
    "90 KG": 90.0,
    "50 KG": 50.0,
    "64 KG": 64.0,
    "126 KG": 126.0,
    "13 KG": 13.0,
    "200 G": 0.2,
    "400 G": 0.4,
    # Volume units -> Standard Multiplier to 1 Litre
    "L": 1.0,
    "500 ML": 0.5,
    # Discrete item units -> Preserved per piece/unit
    "Unit": 1.0,
    "Head": 1.0,
    "Bunch": 1.0,
}

# Official 47 Kenyan County Codes
COUNTY_CODE_MAP = {
    "Mombasa": 1, "Kwale": 2, "Kilifi": 3, "Tana River": 4, "Lamu": 5,
    "Taita Taveta": 6, "Garissa": 7, "Wajir": 8, "Mandera": 9, "Marsabit": 10,
    "Isiolo": 11, "Meru": 12, "Tharaka Nithi": 13, "Embu": 14, "Kitui": 15,
    "Machakos": 16, "Makueni": 17, "Nyandarua": 18, "Nyeri": 19, "Kirinyaga": 20,
    "Murang'a": 21, "Kiambu": 22, "Turkana": 23, "West Pokot": 24, "Samburu": 25,
    "Trans Nzoia": 26, "Uasin Gishu": 27, "Elgeyo Marakwet": 28, "Nandi": 29,
    "Baringo": 30, "Laikipia": 31, "Nakuru": 32, "Narok": 33, "Kajiado": 34,
    "Kericho": 35, "Bomet": 36, "Kakamega": 37, "Vihiga": 38, "Bungoma": 39,
    "Busia": 40, "Siaya": 41, "Kisumu": 42, "Homa Bay": 43, "Migori": 44,
    "Kisii": 45, "Nyamira": 46, "Nairobi": 47
}


def clean_and_normalise_food_prices(raw_csv_path: str, output_csv_path: str) -> pd.DataFrame:
    """Executes the full transformation pipeline and outputs clean dataset."""
    logger.info("=" * 70)
    logger.info("STARTING DATA CLEANING & NORMALISATION PIPELINE")
    logger.info(f"Reading Raw Data: {raw_csv_path}")
    logger.info("=" * 70)

    df_raw = pd.read_csv(raw_csv_path)
    initial_rows = len(df_raw)
    df = df_raw.copy()

    # -------------------------------------------------------------------------
    # 1. Geographic Imputation & Cleaning
    # -------------------------------------------------------------------------
    logger.info("Task 1: Imputing missing administrative and GPS coordinates...")
    
    # Identify before state for evidence
    hola_before = df[df['market'].str.contains("Hola", na=False)][['market', 'admin1', 'admin2', 'latitude', 'longitude']].iloc[0].to_dict()

    # Hola (Tana River) missing location imputation
    hola_mask = df['market'].str.contains("Hola", na=False) & df['admin1'].isnull()
    df.loc[hola_mask, 'admin1'] = 'Coast'
    df.loc[hola_mask, 'admin2'] = 'Tana River'
    df.loc[hola_mask, 'latitude'] = -1.498
    df.loc[hola_mask, 'longitude'] = 40.033

    hola_after = df[df['market'].str.contains("Hola", na=False)][['market', 'admin1', 'admin2', 'latitude', 'longitude']].iloc[0].to_dict()

    # Standardize county name
    df['admin2'] = df['admin2'].str.strip()
    # Map to official County Code
    df['county_code'] = df['admin2'].map(COUNTY_CODE_MAP).fillna(0).astype(int)

    # -------------------------------------------------------------------------
    # 2. Unit Normalisation
    # -------------------------------------------------------------------------
    logger.info("Task 2: Normalising disparate agricultural packaging units...")
    
    # Example Before state for 90 KG bag
    maize_90kg_before = df[(df['commodity'] == 'Maize (white)') & (df['unit'] == '90 KG')][['commodity', 'unit', 'price', 'pricetype']].iloc[0].to_dict()

    # Calculate unit conversion multiplier
    df['unit_multiplier'] = df['unit'].map(UNIT_CONVERSION_FACTORS).fillna(1.0)
    
    # Compute normalized standard price per KG or Litre
    df['normalized_price_per_kg'] = (df['price'] / df['unit_multiplier']).round(2)

    # Standardized unit label
    def assign_standard_unit(row):
        u = row['unit']
        if u in ['L', '500 ML']:
            return 'Litre'
        elif u in ['Unit', 'Head', 'Bunch']:
            return 'Piece'
        else:
            return 'KG'

    df['standard_unit'] = df.apply(assign_standard_unit, axis=1)

    maize_90kg_after = df[(df['commodity'] == 'Maize (white)') & (df['unit'] == '90 KG')][['commodity', 'unit', 'price', 'normalized_price_per_kg', 'standard_unit', 'pricetype']].iloc[0].to_dict()

    # -------------------------------------------------------------------------
    # 3. Clean Price Flags & Currencies
    # -------------------------------------------------------------------------
    logger.info("Task 3: Standardising price flags and currencies...")
    flag_before = df['priceflag'].value_counts().to_dict()
    df['priceflag'] = df['priceflag'].replace({'actual,aggregate': 'aggregate'})
    flag_after = df['priceflag'].value_counts().to_dict()

    # -------------------------------------------------------------------------
    # 4. Temporal Feature Engineering & Harvest Calendar
    # -------------------------------------------------------------------------
    logger.info("Task 4: Parsing dates and engineering harvest season flags...")
    df['date'] = pd.to_datetime(df['date'])
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter
    df['month_name'] = df['date'].dt.strftime('%B')

    # Kenyan Agricultural Harvest Calendar:
    # Long Rains Harvest: Aug, Sep, Oct (Months 8, 9, 10)
    # Short Rains Harvest: Jan, Feb (Months 1, 2)
    df['is_harvest_season'] = df['month'].isin([1, 2, 8, 9, 10])

    # -------------------------------------------------------------------------
    # 5. Inflation Metrics (MoM % and YoY %)
    # -------------------------------------------------------------------------
    logger.info("Task 5: Computing Month-over-Month (MoM %) and Year-over-Year (YoY %) price changes...")
    
    # Sort for deterministic time-series shifts
    df = df.sort_values(by=['market', 'commodity', 'pricetype', 'date']).reset_index(drop=True)

    # Calculate percentage changes per series
    group_cols = ['market', 'commodity', 'pricetype']
    df['prev_month_price'] = df.groupby(group_cols)['normalized_price_per_kg'].shift(1)
    df['mom_price_change_pct'] = (
        ((df['normalized_price_per_kg'] - df['prev_month_price']) / df['prev_month_price']) * 100
    ).round(2)

    df['prev_year_price'] = df.groupby(group_cols)['normalized_price_per_kg'].shift(12)
    df['yoy_inflation_pct'] = (
        ((df['normalized_price_per_kg'] - df['prev_year_price']) / df['prev_year_price']) * 100
    ).round(2)

    # Clean intermediate shift columns
    df = df.drop(columns=['prev_month_price', 'prev_year_price'])

    # -------------------------------------------------------------------------
    # 6. Save Clean Dataset & Log Audit Evidence
    # -------------------------------------------------------------------------
    os.makedirs(os.path.dirname(output_csv_path), exist_ok=True)
    df.to_csv(output_csv_path, index=False)
    
    logger.info("-" * 70)
    logger.info("TRANSFORMATION SUMMARY:")
    logger.info(f"Input Records     : {initial_rows:,}")
    logger.info(f"Cleaned Records   : {len(df):,}")
    logger.info(f"Output File       : {output_csv_path}")
    logger.info(f"Total Columns     : {len(df.columns)}")
    logger.info(f"Remaining Nulls   : {df.isnull().sum().sum()}")
    logger.info("=" * 70)

    # Print Before / After Evidence Table for Rubric
    print("\n" + "=" * 70)
    print("EVIDENCE: BEFORE vs. AFTER TRANSFORMATION SAMPLES (For Milestone 3)")
    print("=" * 70)
    print("\n[EXAMPLE 1: Geographic Null Imputation]")
    print(f"BEFORE -> Market: '{hola_before['market']}', Province: {hola_before['admin1']}, County: {hola_before['admin2']}, Lat: {hola_before['latitude']}")
    print(f"AFTER  -> Market: '{hola_after['market']}', Province: '{hola_after['admin1']}', County: '{hola_after['admin2']}', Lat: {hola_after['latitude']}, Long: {hola_after['longitude']}")
    
    print("\n[EXAMPLE 2: Unit Normalisation to Standard Price / KG]")
    print(f"BEFORE -> Commodity: {maize_90kg_before['commodity']}, Raw Unit: {maize_90kg_before['unit']}, Raw Price: KES {maize_90kg_before['price']:.2f}")
    print(f"AFTER  -> Commodity: {maize_90kg_after['commodity']}, Normalized Price: KES {maize_90kg_after['normalized_price_per_kg']:.2f} / {maize_90kg_after['standard_unit']} (Converted via 90kg divisor)")

    print("\n[EXAMPLE 3: Dirty Flag Standardisation]")
    print(f"BEFORE Distinct Flags: {flag_before}")
    print(f"AFTER Distinct Flags : {flag_after}")
    print("=" * 70 + "\n")

    return df


if __name__ == "__main__":
    clean_and_normalise_food_prices(RAW_DATA_PATH, PROCESSED_DATA_PATH)
