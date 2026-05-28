import os
import time
import logging
from datetime import datetime
 
import pandas as pd
from pytrends.request import TrendReq
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
 
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
 

#Config

 
# Sample film titles — extend this list or pull dynamically from Snowflake
FILM_TITLES = [
    "Inception",
    "The Dark Knight",
    "Interstellar",
    "Parasite",
    "The Avengers",
    "Get Out",
    "Everything Everywhere All at Once",
    "Oppenheimer",
    "Barbie",
    "Dune",
]
 
# pytrends pulls up to 5 keywords per request
BATCH_SIZE = 5
 
#Google Trends timeframe
TIMEFRAME = "today 5-y"
GEO = ""  #worldwide
 
#Snowflake
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "NETFLIX_DB")
SF_SCHEMA = "RAW"
SF_TABLE = "GOOGLE_TRENDS"
 
 

#Helpers

 
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
    )
 
 
def create_table_if_not_exists(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE} (
        TITLE          VARCHAR(255)   NOT NULL,
        WEEK_OF        DATE           NOT NULL,
        SEARCH_INTEREST INTEGER,
        GEO            VARCHAR(10),
        LOADED_AT      TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
    );
    """
    conn.cursor().execute(ddl)
    log.info("Table %s.%s.%s is ready.", SF_DATABASE, SF_SCHEMA, SF_TABLE)
 
 
def fetch_trends_batch(titles: list[str], pytrends: TrendReq) -> pd.DataFrame:
    """Pull weekly interest over time for up to 5 titles."""
    pytrends.build_payload(titles, timeframe=TIMEFRAME, geo=GEO)
    df = pytrends.interest_over_time()
 
    if df.empty:
        log.warning("No data returned for batch: %s", titles)
        return pd.DataFrame()
 
    # Drop the 'isPartial' column if present
    df = df.drop(columns=["isPartial"], errors="ignore")
 
    # Melt from wide (one col per title) to long format
    df = df.reset_index().rename(columns={"date": "WEEK_OF"})
    df = df.melt(id_vars=["WEEK_OF"], var_name="TITLE", value_name="SEARCH_INTEREST")
    df["WEEK_OF"] = df["WEEK_OF"].dt.date
    df["GEO"] = GEO if GEO else "WORLDWIDE"
    return df
 
 
def fetch_all_trends(titles: list[str]) -> pd.DataFrame:
    """Batch titles into groups of BATCH_SIZE and concatenate results."""
    pytrends = TrendReq(hl="en-US", tz=360, timeout=(10, 25), retries=2, backoff_factor=0.5)
    frames = []
 
    for i in range(0, len(titles), BATCH_SIZE):
        batch = titles[i : i + BATCH_SIZE]
        log.info("Fetching trends for: %s", batch)
        df = fetch_trends_batch(batch, pytrends)
        if not df.empty:
            frames.append(df)
        # Be polite to the API — avoid rate limiting
        if i + BATCH_SIZE < len(titles):
            time.sleep(2)
 
    if not frames:
        raise RuntimeError("No trend data fetched for any title.")
 
    return pd.concat(frames, ignore_index=True)
 
 
def load_to_snowflake(df: pd.DataFrame, conn) -> None:
    """Truncate-reload RAW.GOOGLE_TRENDS with fresh data."""
    cursor = conn.cursor()
 
    # Truncate before reload so we don't accumulate duplicates on reruns
    cursor.execute(f"TRUNCATE TABLE {SF_DATABASE}.{SF_SCHEMA}.{SF_TABLE}")
    log.info("Truncated existing data from %s.", SF_TABLE)
 
    # write_pandas requires column names to match exactly
    df.columns = [c.upper() for c in df.columns]
 
    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=SF_TABLE,
        database=SF_DATABASE,
        schema=SF_SCHEMA,
        auto_create_table=False,
        overwrite=False,
    )
 
    if success:
        log.info("Loaded %d rows in %d chunk(s) into %s.", num_rows, num_chunks, SF_TABLE)
    else:
        raise RuntimeError("write_pandas reported failure loading into Snowflake.")
 
 
# Main
# ---------------------------------------------------------------------------
 
def main():
    log.info("=== load_trends.py starting ===")
    started_at = datetime.utcnow()
 
    df = fetch_all_trends(FILM_TITLES)
    log.info("Fetched %d trend rows for %d titles.", len(df), df["TITLE"].nunique())
 
    conn = get_snowflake_conn()
    try:
        create_table_if_not_exists(conn)
        load_to_snowflake(df, conn)
    finally:
        conn.close()
 
    elapsed = (datetime.utcnow() - started_at).total_seconds()
    log.info("=== load_trends.py finished in %.1fs ===", elapsed)
 
 
if __name__ == "__main__":
    main()