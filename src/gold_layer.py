import pandas as pd
import os

def generate_gold_files():
    silver_path = '/files/silver/all_states.parquet'
    gold_path = '/files/gold/'
    os.makedirs(gold_path, exist_ok=True)

    # 1. Ler dados da camada Silver
    df = pd.read_parquet(silver_path)

    # 2. Criar dimensões
    # ----------------------------

    # Dimensão de localização
    dim_location = df[['city', 'state', 'country']].drop_duplicates()
    dim_location.to_parquet(os.path.join(gold_path, 'dim_location.parquet'), index=False)

    # Dimensão de tipo de cervejaria
    dim_brewery_type = df[['brewery_type']].drop_duplicates()
    dim_brewery_type.to_parquet(os.path.join(gold_path, 'dim_brewery_type.parquet'), index=False)

    # Dimensão de nome da cervejaria
    dim_brewery_name = df[['name']].drop_duplicates().rename(columns={'name': 'brewery_name'})
    dim_brewery_name.to_parquet(os.path.join(gold_path, 'dim_brewery_name.parquet'), index=False)

    # Dimensão de tempo
    current_date = pd.Timestamp.today().normalize()
    df_time = pd.DataFrame({
        'full_date': [current_date],
        'year': [current_date.year],
        'month': [current_date.month],
        'day': [current_date.day],
        'day_of_week': [current_date.dayofweek],
        'week_of_year': [current_date.isocalendar().week]
    })
    df_time.to_parquet(os.path.join(gold_path, 'dim_time.parquet'), index=False)

    # 3. Criar tabela fato com métricas
    # ----------------------------------

    df_fact = df[['id', 'name', 'city', 'state', 'country', 'brewery_type', 'website_url', 'latitude', 'longitude']].copy()
    df_fact['brewery_count'] = 1
    df_fact['has_website'] = df_fact['website_url'].notna().astype(int)
    df_fact['has_location'] = (df_fact['latitude'].notna() & df_fact['longitude'].notna()).astype(int)
    df_fact['full_date'] = current_date

    df_fact.rename(columns={'id': 'brewery_id', 'name': 'brewery_name'}, inplace=True)

    df_fact = df_fact[['brewery_id', 'brewery_name', 'city', 'state', 'country',
                       'brewery_type', 'full_date', 'brewery_count',
                       'has_website', 'has_location']]

    df_fact.to_parquet(os.path.join(gold_path, 'fact_breweries_raw.parquet'), index=False)

    print("[GOLD] Arquivos da camada Gold foram gerados com sucesso.")

