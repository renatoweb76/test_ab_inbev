import pandas as pd
from sqlalchemy import create_engine

# Carrega dados da tabela fato de cervejarias
def load_fact_breweries():
    path = '/tmp/gold/fact_breweries_raw.parquet'
    df = pd.read_parquet(path)

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/breweries_dw')

    # Carregar dimensões para mapeamento
    dim_location = pd.read_sql('SELECT * FROM dw.dim_location', engine)
    dim_type = pd.read_sql('SELECT * FROM dw.dim_brewery_type', engine)
    dim_name = pd.read_sql('SELECT * FROM dw.dim_brewery_name', engine)
    dim_time = pd.read_sql('SELECT * FROM dw.dim_time', engine)

    # Realizar joins para obter surrogate keys
    df = df.merge(dim_location, on=['city', 'state', 'country'], how='left')
    df = df.merge(dim_type, on='brewery_type', how='left')
    df = df.merge(dim_name, on='api_brewery_id', how='left')
    df = df.merge(dim_time, on='full_date', how='left')

    # Selecionar colunas finais para a fato
    fact_df = df[[
        'location_id',
        'brewery_type_id',
        'brewery_name_id',
        'time_id',
        'brewery_count',
        'has_website',
        'has_location'
    ]]

    fact_df.to_sql('fact_breweries', engine, schema='dw', if_exists='append', index=False)

    print("[LOAD] Carga da tabela fato 'fact_breweries' concluída.")

if __name__ == "__main__":
    load_fact_breweries()
