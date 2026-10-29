import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT = os.path.join(BASE_DIR, 'data', 'raw', 'Final_data_w_prod__genre.csv')
OUTPUT = os.path.join(BASE_DIR, 'data', 'raw', 'exploded_genre_prod_comp_final.csv')

df = pd.read_csv(INPUT)

# Calculate film count per company BEFORE any exploding
df_prod = df.dropna(subset=['PRODUCTION_COMPANY_NAMES']).copy()
df_prod['PRODUCTION_COMPANY'] = df_prod['PRODUCTION_COMPANY_NAMES'].str.split(', ')
df_prod = df_prod.explode('PRODUCTION_COMPANY')
df_prod['PRODUCTION_COMPANY'] = df_prod['PRODUCTION_COMPANY'].str.strip()
film_counts = df_prod.groupby('PRODUCTION_COMPANY')['MOVIE_ID'].nunique().reset_index()
film_counts.columns = ['PRODUCTION_COMPANY', 'FILM_COUNT']

# Explode genres
df = df.dropna(subset=['GENRE_NAMES'])
df['GENRE'] = df['GENRE_NAMES'].str.split(', ')
df_exploded = df.explode('GENRE')
df_exploded['GENRE'] = df_exploded['GENRE'].str.strip()

# Explode production companies
df_exploded = df_exploded.dropna(subset=['PRODUCTION_COMPANY_NAMES'])
df_exploded['PRODUCTION_COMPANY'] = df_exploded['PRODUCTION_COMPANY_NAMES'].str.split(', ')
df_exploded = df_exploded.explode('PRODUCTION_COMPANY')
df_exploded['PRODUCTION_COMPANY'] = df_exploded['PRODUCTION_COMPANY'].str.strip()

# Drop original combined columns
df_exploded = df_exploded.drop(columns=['GENRE_NAMES', 'PRODUCTION_COMPANY_NAMES'])

# Merge correct film counts
df_exploded = df_exploded.merge(film_counts, on='PRODUCTION_COMPANY', how='left')

# Only keep companies with 10+ films — filters out single-film noise
df_exploded = df_exploded[df_exploded['FILM_COUNT'] >= 10]

df_exploded.to_csv(OUTPUT, index=False)

print(f"Original rows: {len(df)}")
print(f"Exploded rows after filter: {len(df_exploded)}")
print(f"Unique genres: {df_exploded['GENRE'].nunique()}")
print(f"Unique companies: {df_exploded['PRODUCTION_COMPANY'].nunique()}")
print(f"\nTop 10 companies by film count:")
print(film_counts[film_counts['FILM_COUNT'] >= 10].sort_values('FILM_COUNT', ascending=False).head(10))