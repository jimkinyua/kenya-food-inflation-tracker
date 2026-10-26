#!/usr/bin/env python3
"""
Batch Ingestion Script: WFP / KNBS Kenya Food Prices
=====================================================
Automated, idempotent, and re-runnable batch extraction script that downloads
official food market price observations for Kenya and atomically lands them
into the raw data storage layer (data/raw/).

Usage:
    python scripts/extract_wfp_food_prices.py [--output-dir data/raw]
"""

import os
import sys
import time
import hashlib
import logging
import argparse
import urllib.request
import urllib.error

# -----------------------------------------------------------------------------
# Configuration & Defaults
# -----------------------------------------------------------------------------
DEFAULT_URL = (
    "https://data.humdata.org/dataset/e0d3fba6-f9a2-45d7-b949-140c455197ff/"
    "resource/517ee1bf-2437-4f8c-aa1b-cb9925b9d437/download/wfp_food_prices_ken.csv"
)
DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "raw")
DEFAULT_FILENAME = "wfp_food_prices_kenya.csv"
EXPECTED_COLUMNS = [
    "date", "admin1", "admin2", "market", "market_id",
    "latitude", "longitude", "category", "commodity",
    "commodity_id", "unit", "priceflag", "pricetype",
    "currency", "price", "usdprice"
]

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BatchIngestion")


def compute_md5(filepath: str) -> str:
    """Calculate MD5 checksum of a file for data integrity verification."""
    hasher = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def count_csv_rows(filepath: str) -> int:
    """Count total non-empty data rows in the downloaded CSV."""
    count = 0
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if line.strip():
                count += 1
    # Subtract 1 for the header line if present
    return max(0, count - 1)


def validate_file_header(filepath: str) -> bool:
    """Ensure the downloaded file contains the expected schema columns."""
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        header_line = f.readline().strip()
        headers = [col.strip().lower() for col in header_line.split(",")]
        missing = [col for col in EXPECTED_COLUMNS if col.lower() not in headers]
        if missing:
            logger.error(f"Schema validation failed! Missing expected columns: {missing}")
            return False
    return True


def extract_raw_food_prices(url: str, output_dir: str, filename: str, max_retries: int = 3) -> dict:
    """
    Downloads raw food price dataset using an atomic, re-runnable pattern:
      1. Downloads chunks to a temporary file (.tmp)
      2. Validates schema headers and file size
      3. Atomically replaces target file upon validation
      4. Logs execution metrics and audit checksum
    """
    os.makedirs(output_dir, exist_ok=True)
    target_filepath = os.path.join(output_dir, filename)
    tmp_filepath = f"{target_filepath}.tmp"

    start_time = time.time()
    logger.info("=" * 65)
    logger.info("STARTING BATCH INGESTION: Kenya Food Prices (KNBS/WFP)")
    logger.info(f"Source URL       : {url}")
    logger.info(f"Target Directory : {output_dir}")
    logger.info(f"Target Filename  : {filename}")
    logger.info("=" * 65)

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DataEngineeringPipeline/1.0"}
    req = urllib.request.Request(url, headers=headers)

    attempt = 0
    success = False

    while attempt < max_retries and not success:
        attempt += 1
        logger.info(f"Connecting to source (Attempt {attempt} of {max_retries})...")
        try:
            with urllib.request.urlopen(req, timeout=45) as response, open(tmp_filepath, "wb") as out_file:
                chunk_size = 1024 * 64  # 64 KB chunks
                bytes_downloaded = 0
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_file.write(chunk)
                    bytes_downloaded += len(chunk)
                logger.info(f"Download stream completed: {bytes_downloaded:,} bytes transferred.")
            success = True
        except (urllib.error.URLError, TimeoutError) as err:
            logger.warning(f"Attempt {attempt} failed: {err}")
            if os.path.exists(tmp_filepath):
                os.remove(tmp_filepath)
            if attempt < max_retries:
                sleep_sec = 2 ** attempt
                logger.info(f"Retrying in {sleep_sec} seconds...")
                time.sleep(sleep_sec)
            else:
                logger.error("Maximum retry attempts exceeded. Batch ingestion failed.")
                raise

    # File integrity and schema validation
    logger.info("Validating downloaded payload integrity...")
    if not os.path.exists(tmp_filepath) or os.path.getsize(tmp_filepath) == 0:
        raise ValueError("Downloaded file is empty or missing.")

    if not validate_file_header(tmp_filepath):
        os.remove(tmp_filepath)
        raise ValueError("Downloaded file failed schema header validation.")

    # Atomic rename (idempotent replacement)
    if os.path.exists(target_filepath):
        os.remove(target_filepath)
    os.rename(tmp_filepath, target_filepath)

    duration = time.time() - start_time
    file_size_mb = os.path.getsize(target_filepath) / (1024 * 1024)
    total_records = count_csv_rows(target_filepath)
    md5_hash = compute_md5(target_filepath)

    audit_metrics = {
        "status": "SUCCESS",
        "target_filepath": target_filepath,
        "file_size_mb": round(file_size_mb, 2),
        "total_records": total_records,
        "md5_checksum": md5_hash,
        "duration_seconds": round(duration, 2),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    logger.info("-" * 65)
    logger.info("BATCH INGESTION SUMMARY (Audit Evidence):")
    logger.info(f"Status           : {audit_metrics['status']}")
    logger.info(f"Landed Path      : {audit_metrics['target_filepath']}")
    logger.info(f"File Size        : {audit_metrics['file_size_mb']} MB")
    logger.info(f"Total Rows       : {audit_metrics['total_records']:,} records")
    logger.info(f"MD5 Checksum     : {audit_metrics['md5_checksum']}")
    logger.info(f"Execution Time   : {audit_metrics['duration_seconds']}s")
    logger.info("=" * 65)

    return audit_metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest Kenya Food Prices from WFP/KNBS HDX source.")
    parser.add_argument("--url", default=DEFAULT_URL, help="Source CSV download URL")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Destination folder for raw file")
    parser.add_argument("--filename", default=DEFAULT_FILENAME, help="Destination filename")
    parser.add_argument("--retries", type=int, default=3, help="Max retry attempts on failure")
    args = parser.parse_args()

    try:
        metrics = extract_raw_food_prices(
            url=args.url,
            output_dir=args.output_dir,
            filename=args.filename,
            max_retries=args.retries
        )
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal Ingestion Error: {e}", exc_info=True)
        sys.exit(1)
