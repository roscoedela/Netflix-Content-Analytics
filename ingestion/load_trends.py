"""
load_trends.py
--------------
Pulls Google Trends search interest for films currently in RAW.TMDB_METADATA
and appends the current week's data into RAW.GOOGLE_TRENDS.

Key changes from original:
  - NO truncate — history is preserved for cumulative CVI
  - Pulls titles dynamically from Snowflake instead of hardcoded list
  - Only fetches the current week — not 5 years every run
  - Skips rows already loaded for the current week (idempotent)
  - write_pandas replaced with direct INSERT for consistency

ADR: 003_incremental_trends_append
ADR: 004_dynamic_title_pulling
"""

import os
import time
import logging
from datetime import datetime, date

import pandas as pd
from pytrends.request import TrendReq
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BATCH_SIZE = 5       # pytrends hard limit per request
GEO = ""             # worldwide; set "US" for US only
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "NETFLIX_DB")
SF_SCHEMA = "RAW"
SF_TABLE = "GOOGLE_TRENDS"

# How many films to track — ordered by popularity so we get the most relevant
# Keeping at 50 keeps pytrends manageable (10 batches of 5)
TITLE_LIMIT = 50

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
def create_table_if_not_exists(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
            TITLE           VARCHAR(255)  NOT NULL,
            WEEK_OF         DATE          NOT NULL,
            SEARCH_INTEREST INTEGER,
            GEO             VARCHAR(20),
            LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)
    log.info("Table %s.%s.%s is ready.", SF_DATABASE, SF_SCHEMA, SF_TABLE)

# ---------------------------------------------------------------------------
# Dynamic title pulling from Snowflake
# Pulls the most popular films from the last 2 years
# Replaces the hardcoded FILM_TITLES list
# ADR: 004_dynamic_title_pulling
# ---------------------------------------------------------------------------
def get_titles_from_snowflake(cursor) -> list[str]:
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
    log.info("Pulled %d titles dynamically from Snowflake.", len(titles))
    return titles
# ---------------------------------------------------------------------------
# Watermark — what's the latest week already loaded?
# We only insert rows newer than this, making runs idempotent
# ADR: 003_incremental_trends_append
# ---------------------------------------------------------------------------
def get_last_loaded_week(cursor):
    cursor.execute(f"""
        SELECT MAX(WEEK_OF)
        FROM {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}
    """)
    result = cursor.fetchone()[0]
    if result:
        log.info("Last loaded week: %s — will only insert newer rows.", result)
    else:
        log.info("No existing data — first run, loading recent 90 days.")
    return result

# ---------------------------------------------------------------------------
# Fetch from pytrends
# First run: pulls 90 days to seed history
# Weekly runs: pulls current week only
# ---------------------------------------------------------------------------
def build_timeframe(last_loaded_week) -> str:
    if last_loaded_week is None:
        return "today 3-m"   # first run — seed with 90 days
    return "now 7-d"         # weekly run — current week only

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
            time.sleep(10)  # stay polite to the API

    if not frames:
        raise RuntimeError("No trend data fetched for any title.")

    return pd.concat(frames, ignore_index=True)

# ---------------------------------------------------------------------------
# Load — append only, never truncate
# Filter out rows already in Snowflake before inserting
# ---------------------------------------------------------------------------
def load_to_snowflake(df: pd.DataFrame, conn, last_loaded_week) -> None:
    if last_loaded_week:
        before = len(df)
        df = df[df["WEEK_OF"] > last_loaded_week]
        log.info(
            "Filtered %d already-loaded rows. %d new rows to insert.",
            before - len(df), len(df)
        )

    if df.empty:
        log.info("No new rows to insert — pipeline is up to date.")
        return

    df.columns = [c.upper() for c in df.columns]

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
        log.info("Appended %d rows in %d chunk(s) into %s.", num_rows, num_chunks, SF_TABLE)
    else:
        raise RuntimeError("write_pandas reported failure.")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=== load_trends.py starting ===")
    started_at = datetime.utcnow()

    conn = get_conn()
    cursor = conn.cursor()

    try:
        create_table_if_not_exists(cursor)

        # Pull titles dynamically from Snowflake
        titles = get_titles_from_snowflake(cursor)
        if not titles:
            raise RuntimeError("No titles found in TMDB_METADATA — run load_tmdb.py first.")

        # Check watermark — determines timeframe and what to skip
        last_loaded_week = get_last_loaded_week(cursor)
        timeframe = build_timeframe(last_loaded_week)
        log.info("Using timeframe: %s", timeframe)

        # Fetch from Google Trends
        df = fetch_all_trends(titles, timeframe)
        log.info("Fetched %d trend rows for %d titles.", len(df), df["TITLE"].nunique())

        # Append to Snowflake — never truncate
        load_to_snowflake(df, conn, last_loaded_week)

    finally:
        cursor.close()
        conn.close()

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    log.info("=== load_trends.py finished in %.1fs ===", elapsed)


if __name__ == "__main__":
    main()