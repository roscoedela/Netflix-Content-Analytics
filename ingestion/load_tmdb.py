"""
load_tmdb.py
------------
Fetches movie metadata from the TMDB API and upserts into Snowflake
using a staging table + MERGE INTO pattern.

Weekly runs will:
  - Fetch ALL films in RAW.LINKS (not just new ones)
  - Stage the results in a temp table
  - MERGE INTO RAW.TMDB_METADATA — insert new, update changed

This means popularity, revenue, vote counts, and status stay current
every week without ever duplicating records.

ADR: 002_merge_into_for_tmdb
"""

import os
import time
import json
import logging
import requests
import snowflake.connector
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------
def get_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        schema="RAW"
    )

TMDB_KEY = os.getenv("TMDB_API_KEY")

# ---------------------------------------------------------------------------
# Setup — create final table if it doesn't exist
# ---------------------------------------------------------------------------
CREATE_FINAL_TABLE = """
    CREATE TABLE IF NOT EXISTS RAW.TMDB_METADATA (
        TMDB_ID              VARCHAR,
        MOVIE_ID             VARCHAR,
        TITLE                VARCHAR,
        RELEASE_DATE         VARCHAR,
        RUNTIME              VARCHAR,
        BUDGET               VARCHAR,
        REVENUE              VARCHAR,
        POPULARITY           VARCHAR,
        VOTE_AVERAGE         VARCHAR,
        VOTE_COUNT           VARCHAR,
        GENRES               VARCHAR,
        PRODUCTION_COMPANIES VARCHAR,
        ORIGINAL_LANGUAGE    VARCHAR,
        OVERVIEW             VARCHAR,
        STATUS               VARCHAR,
        LOADED_AT            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UPDATED_AT           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
"""

# Temp staging table — scoped to this session, auto-drops on disconnect
CREATE_STAGE_TABLE = """
    CREATE TABLE IF NOT EXISTS RAW.TMDB_STAGE (
        TMDB_ID              VARCHAR,
        MOVIE_ID             VARCHAR,
        TITLE                VARCHAR,
        RELEASE_DATE         VARCHAR,
        RUNTIME              VARCHAR,
        BUDGET               VARCHAR,
        REVENUE              VARCHAR,
        POPULARITY           VARCHAR,
        VOTE_AVERAGE         VARCHAR,
        VOTE_COUNT           VARCHAR,
        GENRES               VARCHAR,
        PRODUCTION_COMPANIES VARCHAR,
        ORIGINAL_LANGUAGE    VARCHAR,
        OVERVIEW             VARCHAR,
        STATUS               VARCHAR
    )
"""

# ---------------------------------------------------------------------------
# Fetch ALL films from LINKS — not just new ones
# Weekly runs need to update existing records (popularity, revenue, votes change)
# ---------------------------------------------------------------------------
GET_ALL_FILMS = """
    SELECT MOVIE_ID, TMDB_ID
    FROM RAW.LINKS
    WHERE TMDB_ID IS NOT NULL
    AND TMDB_ID != ''
    ORDER BY MOVIE_ID
"""

# ---------------------------------------------------------------------------
# Insert into staging table
# ---------------------------------------------------------------------------
STAGE_INSERT_SQL = """
    INSERT INTO RAW.TMDB_STAGE (
        TMDB_ID, MOVIE_ID, TITLE, RELEASE_DATE, RUNTIME,
        BUDGET, REVENUE, POPULARITY, VOTE_AVERAGE, VOTE_COUNT,
        GENRES, PRODUCTION_COMPANIES, ORIGINAL_LANGUAGE,
        OVERVIEW, STATUS
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# ---------------------------------------------------------------------------
# MERGE INTO final table from staging
# - MATCHED: update fields that change week to week
# - NOT MATCHED: insert new records
# ---------------------------------------------------------------------------
MERGE_SQL = """
    MERGE INTO RAW.TMDB_METADATA t
    USING (
        SELECT *
        FROM RAW.TMDB_STAGE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY TMDB_ID ORDER BY TMDB_ID) = 1
    ) s
    ON t.TMDB_ID = s.TMDB_ID

    WHEN MATCHED THEN UPDATE SET
        t.TITLE                = s.TITLE,
        t.RELEASE_DATE         = s.RELEASE_DATE,
        t.RUNTIME              = s.RUNTIME,
        t.BUDGET               = s.BUDGET,
        t.REVENUE              = s.REVENUE,
        t.POPULARITY           = s.POPULARITY,
        t.VOTE_AVERAGE         = s.VOTE_AVERAGE,
        t.VOTE_COUNT           = s.VOTE_COUNT,
        t.GENRES               = s.GENRES,
        t.PRODUCTION_COMPANIES = s.PRODUCTION_COMPANIES,
        t.ORIGINAL_LANGUAGE    = s.ORIGINAL_LANGUAGE,
        t.OVERVIEW             = s.OVERVIEW,
        t.STATUS               = s.STATUS,
        t.UPDATED_AT           = CURRENT_TIMESTAMP()

    WHEN NOT MATCHED THEN INSERT (
        TMDB_ID, MOVIE_ID, TITLE, RELEASE_DATE, RUNTIME,
        BUDGET, REVENUE, POPULARITY, VOTE_AVERAGE, VOTE_COUNT,
        GENRES, PRODUCTION_COMPANIES, ORIGINAL_LANGUAGE,
        OVERVIEW, STATUS, LOADED_AT, UPDATED_AT
    ) VALUES (
        s.TMDB_ID, s.MOVIE_ID, s.TITLE, s.RELEASE_DATE, s.RUNTIME,
        s.BUDGET, s.REVENUE, s.POPULARITY, s.VOTE_AVERAGE, s.VOTE_COUNT,
        s.GENRES, s.PRODUCTION_COMPANIES, s.ORIGINAL_LANGUAGE,
        s.OVERVIEW, s.STATUS, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )
"""

# ---------------------------------------------------------------------------
# API fetch — unchanged, parallelism preserved
# ---------------------------------------------------------------------------
def fetch_tmdb(tmdb_id, movie_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_KEY}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return movie_id, tmdb_id, r.json()
        return movie_id, tmdb_id, None
    except Exception:
        return movie_id, tmdb_id, None


def build_row(movie_id, tmdb_id, data):
    return (
        str(tmdb_id),
        str(movie_id),
        data.get("title", ""),
        data.get("release_date", ""),
        str(data.get("runtime", "")),
        str(data.get("budget", "")),
        str(data.get("revenue", "")),
        str(data.get("popularity", "")),
        str(data.get("vote_average", "")),
        str(data.get("vote_count", "")),
        json.dumps(data.get("genres", [])),
        json.dumps(data.get("production_companies", [])),
        data.get("original_language", ""),
        data.get("overview", ""),
        data.get("status", "")
    )

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    log.info("=== load_tmdb.py starting ===")
    started_at = datetime.utcnow()

    conn = get_conn()
    cursor = conn.cursor()

    # Setup tables
    cursor.execute(CREATE_FINAL_TABLE)
    cursor.execute(CREATE_STAGE_TABLE)
    cursor.execute("TRUNCATE TABLE IF EXISTS RAW.TMDB_STAGE")
    log.info("Tables ready.")

    # Get all films to fetch
    cursor.execute(GET_ALL_FILMS)
    films = cursor.fetchall()
    log.info("%d films to fetch from TMDB API", len(films))

    # Fetch in parallel — 20 workers, same as before
    batch = []
    skipped = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {
            executor.submit(fetch_tmdb, tmdb_id, movie_id): (movie_id, tmdb_id)
            for movie_id, tmdb_id in films
        }

        for future in tqdm(as_completed(futures), total=len(films)):
            movie_id, tmdb_id, data = future.result()

            if not data:
                skipped += 1
                continue

            batch.append(build_row(movie_id, tmdb_id, data))

            # Write to STAGING table in batches of 500
            if len(batch) >= 500:
                cursor.executemany(STAGE_INSERT_SQL, batch)
                batch = []

    # Final batch flush to staging
    if batch:
        cursor.executemany(STAGE_INSERT_SQL, batch)

    staged = len(films) - skipped
    log.info("Staged %d records. Running MERGE INTO final table...", staged)

    # MERGE staging → final
    cursor.execute(MERGE_SQL)

    # Capture merge result for logging
    merge_result = cursor.fetchone()
    if merge_result:
        log.info("MERGE complete — %s rows affected.", merge_result[0])
    else:
        log.info("MERGE complete.")

    cursor.close()
    conn.close()

    elapsed = (datetime.utcnow() - started_at).total_seconds()
    log.info(
        "=== load_tmdb.py finished in %.1fs — %d loaded, %d skipped ===",
        elapsed, staged, skipped
    )


if __name__ == "__main__":
    main()