import logging
import random
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup


# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------

def setup_logger(log_file: str):
    logging.basicConfig(
        filename=log_file,
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )


# ---------------------------------------------------------
# Safe Parsing Helpers
# ---------------------------------------------------------

def safe_str(value) -> Optional[str]:
    if value is None:
        return None
    try:
        text = str(value).strip()
        return text if text else None
    except Exception:
        return None


def safe_int(value) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def safe_float(value) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


# ---------------------------------------------------------
# Anti-Bot Hardened HTTP Fetcher
# ---------------------------------------------------------

session = requests.Session()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def fetch_soup(url: str) -> BeautifulSoup:
    """
    Anti-bot hardened fetcher for Keg N Bottle and similar retailers.
    - Uses a persistent session
    - Sends realistic browser headers
    - Handles gzip/br compression
    - Randomized delay to reduce bot detection
    """

    # Randomized polite delay
    time.sleep(random.uniform(0.3, 0.8))

    resp = session.get(url, headers=HEADERS)
    resp.raise_for_status()

    return BeautifulSoup(resp.text, "html.parser")


# ---------------------------------------------------------
# SQL Insert Helper (Aligned with Expanded Schema)
# ---------------------------------------------------------

def save_record_to_sql(conn, record: dict):
    """
    Inserts a full alcohol record into dbo.alcohol_data.
    This matches your expanded schema exactly.
    """

    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.alcohol_data (
            Retailer_Name,
            Brand,
            Spirit_Type,
            Spirit_Style,
            Complete_Name,
            Price,
            Rating,
            Review_Count,
            Wine_Type,
            Region,
            Appellation,
            Wine_Varietal,
            Wine_Style,
            Wine_Body,
            Country,
            State,
            Food_Pairings,
            Website_Notes,
            ABV,
            Taste,
            URL,
            Scrape_Date
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        record.get("Retailer_Name"),
        record.get("Brand"),
        record.get("Spirit_Type"),
        record.get("Spirit_Style"),
        record.get("Complete_Name"),
        record.get("Price"),
        record.get("Rating"),
        record.get("Review_Count"),
        record.get("Wine_Type"),
        record.get("Region"),
        record.get("Appellation"),
        record.get("Wine_Varietal"),
        record.get("Wine_Style"),
        record.get("Wine_Body"),
        record.get("Country"),
        record.get("State"),
        record.get("Food_Pairings"),
        record.get("Website_Notes"),
        record.get("ABV"),
        record.get("Taste"),
        record.get("URL"),
        record.get("Scrape_Date"),
    ))

    conn.commit()
