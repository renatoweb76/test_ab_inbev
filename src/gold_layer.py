import pandas as pd

# Ler o DataFrame da camada Silver
df = pd.read_parquet('/files/silver/all_states.parquet')

# Criar tabelas de dimensão e fato
dim_location = df[['city', 'state', 'country']].drop_duplicates()
dim_location.to_parquet('/files/gold/dim_location.parquet', index=False)

dim_brewery_type = df[['brewery_type']].drop_duplicates()
dim_brewery_type.to_parquet('/files/gold/dim_brewery_type.parquet', index=False)

dim_brewery_name = df[['name']].drop_duplicates().rename(columns={'name': 'brewery_name'})
dim_brewery_name.to_parquet('/files/gold/dim_brewery_name.parquet', index=False)

df_fact = df[['id', 'city', 'state', 'country', 'brewery_type', 'name']]
df_fact.to_parquet('/files/gold/fact_breweries_raw.parquet', index=False)