#!/usr/bin/env python3
"""
PostgreSQL Star Schema Warehouse Loader
========================================
Milestone 4 Warehouse Implementation:
1. Executes DDL scripts (Staging, Star Schema, Governance).
2. Loads raw data into staging.raw_wfp_food_prices (Bronze/Audit Layer).
3. Populates Dimension Tables (dim_date, dim_commodity, dim_location) with surrogate keys.
4. Populates Fact Table (fact_food_prices) with foreign keys and normalized measures.
5. Executes core analytical queries and prints query results evidence for Milestone 4.

Usage:
    python scripts/load_to_postgres.py
"""

import os
import sys
import time
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# -----------------------------------------------------------------------------
# Configuration & Database Connection
# -----------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "kenya_food_prices")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DATA_SOURCE_URL = os.getenv("DATA_SOURCE_URL", "")

RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "wfp_food_prices_kenya.csv")
CLEAN_DATA_PATH = os.path.join(BASE_DIR, "data", "processed", "clean_food_prices_kenya.csv")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("WarehouseLoader")

CPI_BASKET_STAPLES = [
    "Maize (white)", "Maize", "Beans (dry)", "Beans",
    "Potatoes (Irish)", "Wheat flour", "Milk", "Sugar"
]


def get_db_connection():
    """Establish connection to PostgreSQL database."""
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def execute_sql_file(conn, relative_path: str):
    """Executes a SQL file containing schema definitions."""
    full_path = os.path.join(BASE_DIR, relative_path)
    logger.info(f"Executing DDL: {relative_path}")
    with open(full_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def load_staging_table(conn, raw_csv_path: str):
    """Loads raw CSV data into staging.raw_wfp_food_prices."""
    logger.info("Loading raw data into staging.raw_wfp_food_prices...")
    df = pd.read_csv(raw_csv_path)
    
    # Replace NaN with None for SQL NULL
    df = df.where(pd.notnull(df), None)

    records = [
        (
            row['date'], row['admin1'], row['admin2'], row['market'],
            row['market_id'], row['latitude'], row['longitude'], row['category'],
            row['commodity'], row['commodity_id'], row['unit'], row['priceflag'],
            row['pricetype'], row['currency'], row['price'], row['usdprice']
        )
        for _, row in df.iterrows()
    ]

    insert_sql = """
        INSERT INTO staging.raw_wfp_food_prices (
            date, admin1, admin2, market, market_id, latitude, longitude,
            category, commodity, commodity_id, unit, priceflag, pricetype,
            currency, price, usdprice
        ) VALUES %s;
    """

    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE staging.raw_wfp_food_prices RESTART IDENTITY;")
        execute_values(cur, insert_sql, records, page_size=2000)
    conn.commit()
    logger.info(f"Loaded {len(records):,} rows into staging.raw_wfp_food_prices.")


def load_dimensions(conn, df_clean: pd.DataFrame):
    """Populates dim_date, dim_commodity, and dim_location."""
    logger.info("Populating Star Schema Dimension Tables...")

    with conn.cursor() as cur:
        # 1. DIM_DATE
        unique_dates = df_clean[['date', 'year', 'month', 'quarter', 'month_name', 'is_harvest_season']].drop_duplicates()
        date_records = []
        for _, r in unique_dates.iterrows():
            d_str = str(r['date']).split(" ")[0]
            date_id = int(d_str.replace("-", ""))
            date_records.append((date_id, d_str, int(r['year']), int(r['month']), int(r['quarter']), r['month_name'], bool(r['is_harvest_season'])))

        insert_date = """
            INSERT INTO analytics.dim_date (date_id, full_date, year, month, quarter, month_name, is_harvest_season)
            VALUES %s
            ON CONFLICT (date_id) DO UPDATE SET is_harvest_season = EXCLUDED.is_harvest_season;
        """
        execute_values(cur, insert_date, date_records)
        logger.info(f"Populated analytics.dim_date ({len(date_records):,} unique dates).")

        # 2. DIM_COMMODITY
        unique_commodities = df_clean[['commodity_id', 'commodity', 'category']].drop_duplicates()
        comm_records = [
            (int(r['commodity_id']), r['commodity'], r['category'], r['commodity'] in CPI_BASKET_STAPLES)
            for _, r in unique_commodities.iterrows()
        ]

        insert_comm = """
            INSERT INTO analytics.dim_commodity (natural_id, commodity_name, category, is_cpi_staple)
            VALUES %s
            ON CONFLICT (natural_id) DO UPDATE 
            SET commodity_name = EXCLUDED.commodity_name, category = EXCLUDED.category, is_cpi_staple = EXCLUDED.is_cpi_staple;
        """
        execute_values(cur, insert_comm, comm_records)
        logger.info(f"Populated analytics.dim_commodity ({len(comm_records):,} commodities).")

        # 3. DIM_LOCATION
        unique_locations = df_clean[['market_id', 'market', 'admin2', 'county_code', 'admin1', 'latitude', 'longitude']].drop_duplicates()
        loc_records = [
            (int(r['market_id']), r['market'], r['admin2'], int(r['county_code']), r['admin1'], float(r['latitude']), float(r['longitude']))
            for _, r in unique_locations.iterrows()
        ]

        insert_loc = """
            INSERT INTO analytics.dim_location (natural_market_id, market_name, county_name, county_code, former_province, latitude, longitude)
            VALUES %s
            ON CONFLICT (natural_market_id) DO UPDATE 
            SET market_name = EXCLUDED.market_name, county_name = EXCLUDED.county_name,
                county_code = EXCLUDED.county_code, former_province = EXCLUDED.former_province,
                latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude;
        """
        execute_values(cur, insert_loc, loc_records)
        logger.info(f"Populated analytics.dim_location ({len(loc_records):,} market hubs).")

    conn.commit()


def load_fact_food_prices(conn, df_clean: pd.DataFrame):
    """Populates analytics.fact_food_prices by joining surrogate keys."""
    logger.info("Loading analytics.fact_food_prices...")

    with conn.cursor() as cur:
        # Fetch dimension lookup dictionaries for fast surrogate key mapping
        cur.execute("SELECT natural_id, commodity_id FROM analytics.dim_commodity;")
        comm_map = dict(cur.fetchall())

        cur.execute("SELECT natural_market_id, location_id FROM analytics.dim_location;")
        loc_map = dict(cur.fetchall())

    df = df_clean.copy()
    # Replace NaN with None
    df = df.where(pd.notnull(df), None)

    fact_records = []
    for _, r in df.iterrows():
        d_str = str(r['date']).split(" ")[0]
        date_id = int(d_str.replace("-", ""))
        comm_fk = comm_map[int(r['commodity_id'])]
        loc_fk = loc_map[int(r['market_id'])]

        fact_records.append((
            date_id, comm_fk, loc_fk,
            float(r['price']), float(r['normalized_price_per_kg']),
            float(r['mom_price_change_pct']) if r['mom_price_change_pct'] is not None else None,
            float(r['yoy_inflation_pct']) if r['yoy_inflation_pct'] is not None else None,
            r['unit'], r['standard_unit'], r['pricetype'], r['priceflag'], r['currency']
        ))

    insert_fact = """
        INSERT INTO analytics.fact_food_prices (
            date_id, commodity_id, location_id,
            raw_price, normalized_price_per_kg, mom_price_change_pct, yoy_inflation_pct,
            raw_unit, standard_unit, price_type, price_flag, currency
        ) VALUES %s
        ON CONFLICT (date_id, commodity_id, location_id, price_type) DO UPDATE SET
            normalized_price_per_kg = EXCLUDED.normalized_price_per_kg,
            mom_price_change_pct = EXCLUDED.mom_price_change_pct,
            yoy_inflation_pct = EXCLUDED.yoy_inflation_pct,
            price_flag = EXCLUDED.price_flag,
            loaded_at = CURRENT_TIMESTAMP;
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_fact, fact_records, page_size=2000)
    conn.commit()
    logger.info(f"Loaded {len(fact_records):,} records into analytics.fact_food_prices.")


def run_analytical_queries(conn):
    """Executes sample queries to verify analytical performance and provide rubric evidence."""
    logger.info("\n" + "=" * 70)
    logger.info("RUNNING ANALYTICAL QUERIES (Milestone 4 Rubric Evidence)")
    logger.info("=" * 70)

    queries = [
        (
            "QUERY 1: National Average Price of White Maize (KES / KG) by Year",
            """
            SELECT d.year, f.price_type, ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_price_per_kg, COUNT(*) AS observations
            FROM analytics.fact_food_prices f
            JOIN analytics.dim_commodity c ON f.commodity_id = c.commodity_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE c.commodity_name = 'Maize (white)' AND d.year >= 2020
            GROUP BY d.year, f.price_type
            ORDER BY d.year DESC, f.price_type;
            """
        ),
        (
            "QUERY 2: Top 5 Highest Inflation Counties in 2024",
            """
            SELECT l.county_name, l.county_code, ROUND(AVG(f.yoy_inflation_pct), 2) AS avg_yoy_inflation_pct
            FROM analytics.fact_food_prices f
            JOIN analytics.dim_location l ON f.location_id = l.location_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE d.year = 2024 AND f.yoy_inflation_pct IS NOT NULL
            GROUP BY l.county_name, l.county_code
            ORDER BY avg_yoy_inflation_pct DESC
            LIMIT 5;
            """
        ),
        (
            "QUERY 3: Harvest Season vs. Non-Harvest Season Price Comparison",
            """
            SELECT 
                c.commodity_name,
                CASE WHEN d.is_harvest_season THEN 'Harvest Season (Aug-Oct & Jan-Feb)' ELSE 'Off-Season / Planting' END AS season,
                ROUND(AVG(f.normalized_price_per_kg), 2) AS avg_price_per_kg
            FROM analytics.fact_food_prices f
            JOIN analytics.dim_commodity c ON f.commodity_id = c.commodity_id
            JOIN analytics.dim_date d ON f.date_id = d.date_id
            WHERE c.commodity_name IN ('Maize (white)', 'Potatoes (Irish)', 'Beans (dry)')
            GROUP BY c.commodity_name, season
            ORDER BY c.commodity_name, season;
            """
        )
    ]

    with conn.cursor() as cur:
        for title, query in queries:
            print(f"\n--- {title} ---")
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            df_res = pd.DataFrame(rows, columns=cols)
            print(df_res.to_string(index=False))
    print("=" * 70 + "\n")


def main():
    start_time = time.time()
    logger.info("Connecting to PostgreSQL warehouse...")
    conn = get_db_connection()

    try:
        # Step 1: Execute DDLs
        execute_sql_file(conn, "sql/01_staging/create_raw_wfp_food_prices.sql")
        execute_sql_file(conn, "sql/02_analytics/create_star_schema.sql")
        execute_sql_file(conn, "sql/03_governance/create_governance_schema.sql")

        # Step 2: Load Staging Table (Raw Data)
        load_staging_table(conn, RAW_DATA_PATH)

        # Step 3: Load Star Schema Dimensions & Fact (Clean Data)
        df_clean = pd.read_csv(CLEAN_DATA_PATH)
        load_dimensions(conn, df_clean)
        load_fact_food_prices(conn, df_clean)

        # Step 4: Run Analytical Queries
        run_analytical_queries(conn)

        duration = round(time.time() - start_time, 2)

        # Step 5: Log Pipeline Execution to Governance Audit Log
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO governance.pipeline_audit_log (
                    pipeline_name, run_id, source_name, source_url, raw_records_read,
                    clean_records_loaded, execution_status, duration_seconds
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                "load_to_postgres_standalone",
                f"run_{int(time.time())}",
                "Kenya Food Market Prices (KNBS/MoA)",
                DATA_SOURCE_URL,
                len(df_clean),
                len(df_clean),
                "PASSED",
                duration
            ))
        conn.commit()

        logger.info(f"Warehouse Build & Load Completed Successfully in {duration}s!")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Warehouse Load Failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
