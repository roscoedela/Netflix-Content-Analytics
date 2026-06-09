"""
load_trends.py
--------------
Pulls Google Trends search interest and loads into RAW.GOOGLE_TRENDS
as three separate categories:

  HISTORICAL        — top films from TMDB by popularity (last 5 years)
  TRENDING_AUTO     — what Google says is trending right now
  TRENDING_STRATEGIC — manually curated watchlist (update weekly)

Rules:
  - Append only — never truncate, history is preserved for cumulative CVI
  - 10 titles per category — stays under Google rate limits
  - 10s sleep between batches, 30s sleep between categories
  - Watermark per category prevents duplicate inserts

ADR: 003_incremental_trends_append
ADR: 004_dynamic_title_pulling
"""

import os
import time
import logging
from datetime import datetime

import pandas as pd
from pytrends.request import TrendReq
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BATCH_SIZE = 5       # pytrends hard limit per request
GEO = ""             # worldwide
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "CONTENT_ANALYTICS")
SF_SCHEMA = "RAW"
SF_TABLE = "GOOGLE_TRENDS"
TITLE_LIMIT = 10     # max per category — keeps us under rate limits

# Manually curated strategic watchlist — update every Friday
STRATEGIC_TITLES = [
    "Obsession movie",
    "Devil Wears Prada 2",
    "Backrooms movie",
    "I Love Boosters movie",
    "Scary Movie 6 movie ",
    "Power Ballad movie",
    "Star Wars The Mandalorian and Grogu movie ",
    "Masters of the universe movie",
    "Michael movie",
    "Pressure movie",
]

# ---------------------------------------------------------------------------
# Connection — supports both password and private key auth
# ---------------------------------------------------------------------------
def get_conn():
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")

    if private_key_path:
        with open(private_key_path, "rb") as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,
                backend=default_backend()
            )
        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=SF_DATABASE,
            role=os.getenv("SNOWFLAKE_ROLE"),
            schema=SF_SCHEMA,
            private_key=private_key_bytes
        )
    else:
        return snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=SF_DATABASE,
            role=os.getenv("SNOWFLAKE_ROLE"),
            schema=SF_SCHEMA
        )

# ---------------------------------------------------------------------------
# Setup — create table with CATEGORY column if it doesn't exist
# ---------------------------------------------------------------------------
def create_table_if_not_exists(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
            TITLE           VARCHAR(255)  NOT NULL,
            WEEK_OF         DATE          NOT NULL,
            SEARCH_INTEREST INTEGER,
            GEO             VARCHAR(20),
            CATEGORY        VARCHAR(50)   DEFAULT 'HISTORICAL',
            LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    # Add CATEGORY column if table already exists without it
    try:
        cursor.execute(f"""
            ALTER TABLE {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
            ADD COLUMN CATEGORY VARCHAR(50) DEFAULT 'HISTORICAL'
        """)
        log.info("Added CATEGORY column to existing table.")
    except Exception:
        pass  # column already exists — fine
    log.info("Table %s.%s.%s is ready.", SF_DATABASE, SF_SCHEMA, SF_TABLE)

# ---------------------------------------------------------------------------
# Title sources — three separate lists
# ---------------------------------------------------------------------------
def get_historical_titles(cursor) -> list[str]:
    """Top films from TMDB ordered by popularity — last 5 years."""
    cursor.execute(f"""
        SELECT TITLE
        FROM {SF_DATABASE}.RAW.TMDB_METADATA
        WHERE TRY_TO_DATE(RELEASE_DATE) >= DATEADD(year, -5, CURRENT_DATE())
        AND TITLE IS NOT NULL
        AND TITLE != ''
        GROUP BY TITLE
        ORDER BY MAX(TRY_CAST(POPULARITY AS FLOAT)) DESC NULLS LAST
        LIMIT {TITLE_LIMIT}
    """)
    titles = [row[0] for row in cursor.fetchall()]
    log.info("Pulled %d historical titles from Snowflake.", len(titles))
    return titles

def get_automated_trending() -> list[str]:
    """What Google says is trending right now — fully automated."""
    try:
        pytrends = TrendReq(hl="en-US", tz=360)
        trending = pytrends.trending_searches(pn="united_states")
        film_keywords = ["movie", "film", "netflix", "trailer", "review", "series"]
        titles = [
            term for term in trending[0].tolist()
            if any(kw in term.lower() for kw in film_keywords)
        ][:TITLE_LIMIT]
        log.info("Auto-pulled %d trending titles from Google.", len(titles))
        return titles
    except Exception as e:
        log.warning("Could not pull automated trending titles: %s", e)
        return []

def get_strategic_titles() -> list[str]:
    """Manually curated watchlist — update STRATEGIC_TITLES every Friday."""
    log.info("Using %d strategic titles.", len(STRATEGIC_TITLES))
    return STRATEGIC_TITLES[:TITLE_LIMIT]

# ---------------------------------------------------------------------------
# Watermark — per category so each list tracks independently
# ---------------------------------------------------------------------------
def get_last_loaded_week(cursor, category: str):
    cursor.execute(f"""
        SELECT MAX(WEEK_OF)
        FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
        WHERE CATEGORY = '{category}'
    """)
    result = cursor.fetchone()[0]
    if result:
        log.info("[%s] Last loaded week: %s", category, result)
    else:
        log.info("[%s] No existing data — first run.", category)
    return result

# ---------------------------------------------------------------------------
# Timeframe — 90 days on first run, current week after that
# ---------------------------------------------------------------------------
def build_timeframe(last_loaded_week) -> str:
    if last_loaded_week is None:
        return "today 3-m"   # first run — seed with 90 days of history
    return "now 7-d"         # weekly run — current week only

# ---------------------------------------------------------------------------
# Fetch from pytrends
# ---------------------------------------------------------------------------
def fetch_trends_batch(titles: list[str], pytrends: TrendReq, timeframe: str) -> pd.DataFrame:
    pytrends.build_payload(titles, timeframe=timeframe, geo=GEO)
    df = pytrends.interest_over_time()

    if df.empty:
        log.warning("No data returned for batch: %s", titles)
        return pd.DataFrame()

    df = df.drop(columns=["isPartial"], errors="ignore")
    df = df.reset_index().rename(columns={"date": "WEEK_OF"})
    df = df.melt(id_vars=["WEEK_OF"], var_name="TITLE", value_name="SEARCH_INTEREST")
    df["WEEK_OF"] = df["WEEK_OF"].dt.date
    df["GEO"] = GEO if GEO else "WORLDWIDE"
    return df

def fetch_all_trends(titles: list[str], timeframe: str) -> pd.DataFrame:
    pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25), retries=2, backoff_factor=0.5)
    frames = []

    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i : i + BATCH_SIZE]
        log.info("Fetching trends for: %s", batch)
        df = fetch_trends_batch(batch, pytrends, timeframe)
        if not df.empty:
            frames.append(df)
        if i + BATCH_SIZE < len(titles):
            time.sleep(10)  # 10s between batches — avoids rate limiting

    if not frames:
        log.warning("No trend data returned for any title in this batch.")
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)

# ---------------------------------------------------------------------------
# Load — append only, never truncate, tagged by category
# ---------------------------------------------------------------------------
def load_to_snowflake(df: pd.DataFrame, conn, last_loaded_week, category: str) -> None:
    if df.empty:
        log.info("[%s] Nothing to load — empty dataframe.", category)
        return

    # Filter rows already loaded for this category
    if last_loaded_week:
        before = len(df)
        df = df[df["WEEK_OF"] > last_loaded_week]
        log.info(
            "[%s] Filtered %d already-loaded rows. %d new rows to insert.",
            category, before - len(df), len(df)
        )

    if df.empty:
        log.info("[%s] No new rows to insert — pipeline is up to date.", category)
        return

    df.columns = [c.upper() for c in df.columns]
    df["CATEGORY"] = category  # tag each row with its category

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=SF_TABLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        auto_create_table=False,
        overwrite=False,   # NEVER overwrite — append only
    )

    if success:
        log.info("[%s] Appended %d rows in %d chunk(s).", category, num_rows, num_chunks)
    else:
        raise RuntimeError(f"write_pandas failed for category {category}.")

# ---------------------------------------------------------------------------
# Main — runs all three categories independently
# ---------------------------------------------------------------------------
def main():
    log.info("=== load_trends.py starting ===")
    started_at = datetime.utcnow()

    conn = get_conn()
    cursor = conn.cursor()

    try:
        create_table_if_not_exists(cursor)

        # ----------------------------------------------------------------
        # 1. HISTORICAL — top films from TMDB
        # ----------------------------------------------------------------
        log.info("=== Category: HISTORICAL ===")
        historical_titles = get_historical_titles(cursor)
        if historical_titles:
            last_week = get_last_loaded_week(cursor, "HISTORICAL")
            timeframe = build_timeframe(last_week)
            log.info("Using timeframe: %s", timeframe)
            df = fetch_all_trends(historical_titles, timeframe)
            load_to_snowflake(df, conn, last_week, category="HISTORICAL")
        else:
            log.warning("No historical titles found — skipping.")

        log.info("Sleeping 30s before next category...")
        time.sleep(30)

        # ----------------------------------------------------------------
        # 2. TRENDING_AUTO — what Google says is hot right now
        # ----------------------------------------------------------------
        log.info("=== Category: TRENDING_AUTO ===")
        auto_titles = get_automated_trending()
        if auto_titles:
            last_week = get_last_loaded_week(cursor, "TRENDING_AUTO")
            df = fetch_all_trends(auto_titles, "now 7-d")
            load_to_snowflake(df, conn, last_week, category="TRENDING_AUTO")
        else:
            log.warning("No automated trending titles found — skipping.")

        log.info("Sleeping 30s before next category...")
        time.sleep(30)

        # ----------------------------------------------------------------
        # 3. TRENDING_STRATEGIC — manually curated watchlist
        # ----------------------------------------------------------------
        log.info("=== Category: TRENDING_STRATEGIC ===")
        strategic_titles = get_strategic_titles()
        last_week = get_last_loaded_week(cursor, "TRENDING_STRATEGIC")
        df = fetch_all_trends(strategic_titles, "now 7-d")
        load_to_snowflake(df, conn, last_week, category="TRENDING_STRATEGIC")

    finally:
        cursor.close()
        conn.close()

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    log.info("=== load_trends.py finished in %.1fs ===", elapsed)


if __name__ == "__main__":
    main()