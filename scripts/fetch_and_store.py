"""
Script to fetch stock data from Alpha Vantage and insert into PostgreSQL.
Used by Airflow DAG.
"""

import os
import time
import logging
from datetime import datetime
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
SYMBOLS = os.environ.get("STOCK_SYMBOLS", "IBM").split(",")

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
DB_USER = os.environ.get("POSTGRES_USER", "airflow")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "airflow")
DB_NAME = os.environ.get("POSTGRES_DB", "airflow")

ALPHA_URL = "https://www.alphavantage.co/query"

def fetch_daily(symbol: str, retry: int = 5):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": API_KEY,
    }
    for attempt in range(1, retry + 1):
        try:
            LOG.info(f"[{symbol}] Attempt {attempt}")
            r = requests.get(ALPHA_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "Error Message" in data:
                raise RuntimeError(data["Error Message"])
            if "Note" in data:
                LOG.warning(f"Rate limited: {data['Note']}")
                time.sleep(20)
                continue
            if "Time Series (Daily)" in data:
                return data["Time Series (Daily)"]
            LOG.error(f"Unexpected response: {data}")
        except Exception as e:
            LOG.error(f"Fetch error: {e}")
            time.sleep(5)
    return None

def parse_data(symbol: str, timeseries: dict, days: int = 3):
    if not timeseries:
        return []
    rows = []
    dates = sorted(timeseries.keys(), reverse=True)[:days]
    for d in dates:
        try:
            item = timeseries[d]
            ts = datetime.strptime(d, "%Y-%m-%d")
            rows.append((
                symbol,
                ts,
                float(item["1. open"]),
                float(item["2. high"]),
                float(item["3. low"]),
                float(item["4. close"]),
                int(item["5. volume"])
            ))
        except Exception as e:
            LOG.error(f"Parse error for {symbol} on {d}: {e}")
    return rows

def insert_rows(rows):
    if not rows:
        LOG.info("No rows to insert.")
        return
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        query = """
            INSERT INTO stock_quotes
            (symbol, timestamp, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT DO NOTHING;
        """
        execute_values(cur, query, rows)
        conn.commit()
    except Exception as e:
        LOG.error(f"DB insert error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def main():
    all_rows = []
    for sym in SYMBOLS:
        s = sym.strip().upper()
        ts = fetch_daily(s)
        rows = parse_data(s, ts)
        all_rows.extend(rows)
        time.sleep(12)
    insert_rows(all_rows)

if __name__ == "__main__":
    main()
