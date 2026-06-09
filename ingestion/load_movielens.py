import os
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# Load private key for key-pair auth (bypasses MFA)
with open(os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"), "rb") as key_file:
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

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    schema="RAW",
    private_key=private_key_bytes
)
cursor = conn.cursor()
print("Connected to Snowflake")

cursor.execute("CREATE STAGE IF NOT EXISTS RAW.MOVIELENS_STAGE")
print("Stage created")

data_path = os.path.abspath("data/raw")

files = ["movies.csv", "links.csv", "tags.csv", "ratings.csv"]
for f in files:
    print(f"Staging {f}...")
    cursor.execute(f"PUT 'file://{data_path}/{f}' @RAW.MOVIELENS_STAGE AUTO_COMPRESS=FALSE")
    print(f"{f} staged")

    file_format = """
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL', 'null', 'NA')
        EMPTY_FIELD_AS_NULL = TRUE
    )
    ON_ERROR = 'CONTINUE'
"""

print("Loading MOVIES...")
cursor.execute(f"""
    COPY INTO RAW.MOVIES (MOVIE_ID, TITLE, GENRES)
    FROM @RAW.MOVIELENS_STAGE/movies.csv
    {file_format}
""")
print("MOVIES loaded")

print("Loading LINKS...")
cursor.execute(f"""
    COPY INTO RAW.LINKS (MOVIE_ID, IMDB_ID, TMDB_ID)
    FROM @RAW.MOVIELENS_STAGE/links.csv
    {file_format}
""")
print("LINKS loaded")

print("Loading TAGS...")
cursor.execute(f"""
    COPY INTO RAW.TAGS (USER_ID, MOVIE_ID, TAG, TIMESTAMP)
    FROM @RAW.MOVIELENS_STAGE/tags.csv
    {file_format}
""")
print("TAGS loaded")

print("Loading RATINGS...")
cursor.execute(f"""
    COPY INTO RAW.RATINGS (USER_ID, MOVIE_ID, RATING, TIMESTAMP)
    FROM @RAW.MOVIELENS_STAGE/ratings.csv
    {file_format}
""")
print("RATINGS loaded")

cursor.close()
conn.close()
print("Production load complete")