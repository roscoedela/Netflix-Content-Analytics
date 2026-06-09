import os
import time
import logging
import requests
import snowflake.connector
from dotenv import load_dotenv
from tqdm import tqdm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

TMDB_KEY = os.getenv("TMDB_API_KEY")
TOP_N_CAST = 5

# Crew roles we care about — one person per role per film
CREW_ROLES = {
    "Director":                  "director",
    "Director of Photography":   "cinematographer",
    "Original Music Composer":   "composer",
    "Screenplay":                "writer",
    "Writer":                    "writer",
}

def get_conn():
    with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(), password=None, backend=default_backend()
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
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        schema="RAW",
        private_key=private_key_bytes
    )

def setup_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW.TMDB_CREDITS (
            movie_id       INTEGER,
            person_id      INTEGER,
            name           VARCHAR,
            role           VARCHAR,
            character_name VARCHAR,
            cast_order     INTEGER,
            popularity     FLOAT,
            loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (movie_id, person_id, role)
        )
    """)
    log.info("Table RAW.TMDB_CREDITS ready.")

def fetch_credits(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
    try:
        r = requests.get(url, params={"api_key": TMDB_KEY}, timeout=10)
        if r.status_code != 200:
            return []
        data = r.json()
    except Exception:
        return []

    rows = []

    # Top N cast by billing order
    for member in sorted(data.get("cast", []), key=lambda x: x["order"])[:TOP_N_CAST]:
        rows.append((
            member["id"],
            member["name"],
            "cast",
            member.get("character"),
            member["order"],
            member.get("popularity", 0)
        ))

    # Crew roles — one person per role per film
    found_roles = set()
    for member in data.get("crew", []):
        job = member.get("job", "")
        role = CREW_ROLES.get(job)
        if role and role not in found_roles:
            rows.append((
                member["id"],
                member["name"],
                role,
                None,
                None,
                member.get("popularity", 0)
            ))
            found_roles.add(role)
        if len(found_roles) == len(set(CREW_ROLES.values())):
            break

    return rows

def main():
    log.info("=== load_tmdb_credits.py starting ===")
    conn = get_conn()
    cursor = conn.cursor()
    setup_table(cursor)

    # Only fetch films not already loaded — supports safe reruns
    cursor.execute("""
        SELECT DISTINCT l.movie_id, l.tmdb_id
        FROM RAW.stg_links l
        WHERE l.tmdb_id IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM RAW.TMDB_CREDITS c
            WHERE c.movie_id = l.movie_id
        )
        ORDER BY l.movie_id
    """)
    films = cursor.fetchall()
    log.info(f"{len(films)} films to fetch credits for.")

    batch = []
    batch_size = 500
    loaded = 0
    skipped = 0

    for movie_id, tmdb_id in tqdm(films):
        rows = fetch_credits(tmdb_id)
        if not rows:
            skipped += 1
            continue
        for person_id, name, role, character, order, popularity in rows:
            batch.append((movie_id, person_id, name, role, character, order, popularity))
        if len(batch) >= batch_size:
            cursor.executemany("""
                INSERT INTO RAW.TMDB_CREDITS
                    (movie_id, person_id, name, role, character_name, cast_order, popularity)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, batch)
            loaded += len(batch)
            batch = []
        time.sleep(0.05)

    if batch:
        cursor.executemany("""
            INSERT INTO RAW.TMDB_CREDITS
                (movie_id, person_id, name, role, character_name, cast_order, popularity)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, batch)
        loaded += len(batch)

    conn.commit()
    cursor.close()
    conn.close()
    log.info(f"=== finished — {loaded} credit rows loaded, {skipped} films skipped ===")

if __name__ == "__main__":
    main()