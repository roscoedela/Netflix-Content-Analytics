import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT = os.path.join(BASE_DIR, 'data', 'raw', 'Final_data_w_prod__genre.csv')
OUTPUT = os.path.join(BASE_DIR, 'data', 'raw', 'exploded_genre.csv')

df = pd.read_csv(INPUT)
df = df.dropna(subset=['GENRE_NAMES'])
df['GENRE'] = df['GENRE_NAMES'].str.split(', ')
df_exploded = df.explode('GENRE')
df_exploded['GENRE'] = df_exploded['GENRE'].str.strip()
df_exploded = df_exploded.drop(columns=['GENRE_NAMES'])
df_exploded.to_csv(OUTPUT, index=False)

print(f"Original rows: {len(df)}")
print(f"Exploded rows: {len(df_exploded)}")
print(f"Unique genres: {df_exploded['GENRE'].nunique()}")
print(f"\nTop 10 genres by count:")
print(df_exploded['GENRE'].value_counts().head(10))