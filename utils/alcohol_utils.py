import time
import logging
import requests
import pyodbc
from bs4 import BeautifulSoup

# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------

def setup_logger(log_file):
    logging.basicConfig(
        filename=log_file,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# ---------------------------------------------------------
# Safe Conversions
# ---------------------------------------------------------

def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None

def safe_int(v):
    try:
        return int(v)
    except Exception:
        return None

def safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

# ---------------------------------------------------------
# HTTP Fetching
# ---------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_SLEEP = 1.0

def fetch_soup(url, params=None):
    time.sleep(REQUEST_SLEEP)
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

# ---------------------------------------------------------
# SQL Insertion
# ---------------------------------------------------------

def save_record_to_sql(conn, record):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO Alcohol_Pricing_Rating.dbo.alcohol_data (
                Retailer_Name,
                Brand,
                Spirit_Type,
                Spirit_Style,
                Complete_Name,
                ABV,
                Taste,
                Country,
                State,
                Food_Pairings,
                Website_Notes,
                Price,
                Rating,
                Review_Count,
                Wine_Type,
                Region,
                Appellation,
                Wine_Varietal,
                Wine_Style,
                Wine_Body,
                Beer_Type,
                Beer_Style,
                Beer_Body,
                URL,
                Scrape_Date
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["Retailer_Name"],
                record["Brand"],
                record["Spirit_Type"],
                record["Spirit_Style"],
                record["Complete_Name"],
                record["ABV"],
                record["Taste"],
                record["Country"],
                record["State"],
                record["Food_Pairings"],
                record["Website_Notes"],
                record["Price"],
                record["Rating"],
                record["Review_Count"],
                record["Wine_Type"],
                record["Region"],
                record["Appellation"],
                record["Wine_Varietal"],
                record["Wine_Style"],
                record["Wine_Body"],
                record["Beer_Type"],
                record["Beer_Style"],
                record["Beer_Body"],
                record["URL"],
                record["Scrape_Date"],
            ),
        )
        conn.commit()
    except Exception as e:
        logging.error(f"SQL error for {record.get('URL')}: {e}")
        print(f"SQL ERROR for {record.get('URL')}: {e}")

# ---------------------------------------------------------
# Normalization Helpers
# ---------------------------------------------------------

def normalize_abv(text):
    """
    Convert '13.5%' → 13
    """
    if not text:
        return None
    import re
    m = re.search(r"([\d\.]+)", text)
    if m:
        return safe_int(round(float(m.group(1))))
    return None

def normalize_price(text):
    """
    Convert '$16.99' → 16.99
    """
    if not text:
        return None
    import re
    m = re.search(r"([\d,]+\.\d{2})", text)
    if m:
        return safe_float(m.group(1).replace(",", ""))
    return None
