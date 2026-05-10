import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

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
print("Connected to Snowflake")

cursor.execute("CREATE TABLE IF NOT EXISTS RAW.MOVIES (MOVIE_ID VARCHAR, TITLE VARCHAR, GENRES VARCHAR)")
cursor.execute("CREATE TABLE IF NOT EXISTS RAW.RATINGS (USER_ID VARCHAR, MOVIE_ID VARCHAR, RATING VARCHAR, TIMESTAMP VARCHAR)")
cursor.execute("CREATE TABLE IF NOT EXISTS RAW.TAGS (USER_ID VARCHAR, MOVIE_ID VARCHAR, TAG VARCHAR, TIMESTAMP VARCHAR)")
cursor.execute("CREATE TABLE IF NOT EXISTS RAW.LINKS (MOVIE_ID VARCHAR, IMDB_ID VARCHAR, TMDB_ID VARCHAR)")
print("Tables created")

def load_csv_to_snowflake(filepath, table_name, col_map):
    print(f"Loading {table_name}...")
    total_rows = 0
    for chunk in tqdm(pd.read_csv(filepath, chunksize=10000, dtype=str, keep_default_na=False)):
        chunk = chunk.rename(columns=col_map)
        columns = list(col_map.values())
        data = [tuple(row) for row in chunk[columns].values]
        col_str = ", ".join(columns)
        val_str = ", ".join(["%s"] * len(columns))
        cursor.executemany(f"INSERT INTO RAW.{table_name} ({col_str}) VALUES ({val_str})", data)
        total_rows += len(chunk)
    print(f"{table_name} loaded — {total_rows:,} rows")

load_csv_to_snowflake("data/raw/movies.csv", "MOVIES", {
    "movieId": "MOVIE_ID", "title": "TITLE", "genres": "GENRES"
})
load_csv_to_snowflake("data/raw/links.csv", "LINKS", {
    "movieId": "MOVIE_ID", "imdbId": "IMDB_ID", "tmdbId": "TMDB_ID"
})
load_csv_to_snowflake("data/raw/tags.csv", "TAGS", {
    "userId": "USER_ID", "movieId": "MOVIE_ID", "tag": "TAG", "timestamp": "TIMESTAMP"
})
load_csv_to_snowflake("data/raw/ratings.csv", "RATINGS", {
    "userId": "USER_ID", "movieId": "MOVIE_ID", "rating": "RATING", "timestamp": "TIMESTAMP"
})

cursor.close()
conn.close()
print("All data loaded successfully")
