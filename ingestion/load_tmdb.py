import os
import time
import json
import requests
import snowflake.connector
from dotenv import load_dotenv
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))

# Connections
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    schema="RAW"
)
cursor = conn.cursor()
TMDB_KEY = os.getenv("TMDB_API_KEY")
print("Connected to Snowflake")

# Create table
cursor.execute("""
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
        LOADED_AT            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("Table created")

# Get TMDB IDs not yet loaded
cursor.execute("""
    SELECT l.MOVIE_ID, l.TMDB_ID
    FROM RAW.LINKS l
    LEFT JOIN RAW.TMDB_METADATA m ON l.TMDB_ID = m.TMDB_ID
    WHERE l.TMDB_ID IS NOT NULL
    AND l.TMDB_ID != ''
    AND m.TMDB_ID IS NULL
    ORDER BY l.MOVIE_ID
""")
films = cursor.fetchall()
print(f"{len(films)} films to fetch from TMDB")

# API function
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

# Insert SQL
INSERT_SQL = """
    INSERT INTO RAW.TMDB_METADATA (
        TMDB_ID, MOVIE_ID, TITLE, RELEASE_DATE, RUNTIME,
        BUDGET, REVENUE, POPULARITY, VOTE_AVERAGE, VOTE_COUNT,
        GENRES, PRODUCTION_COMPANIES, ORIGINAL_LANGUAGE,
        OVERVIEW, STATUS
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

batch = []
skipped = 0

# Fetch in parallel with 20 workers
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

        batch.append((
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
        ))

        if len(batch) >= 500:
            cursor.executemany(INSERT_SQL, batch)
            batch = []

# Insert any remaining records
if batch:
    cursor.executemany(INSERT_SQL, batch)

cursor.close()
conn.close()
print(f"TMDB load complete — {len(films) - skipped:,} loaded, {skipped:,} skipped")