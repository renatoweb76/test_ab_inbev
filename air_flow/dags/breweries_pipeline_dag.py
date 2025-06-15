from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# ===== Adicionar src/ e src/load_dw/ ao sys.path =====

# Caminho absoluto para a pasta 'air_flow'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos para src/ e src/load_dw/
SRC_PATH = os.path.join(BASE_DIR, 'src')
LOAD_DW_PATH = os.path.join(SRC_PATH, 'load_dw')

# Adiciona ao sys.path
sys.path.append(SRC_PATH)
sys.path.append(LOAD_DW_PATH)

# ===== Importação dos scripts =====

from bronze_layer import fetch_raw_data
from silver_layer import transform_and_partition
from gold_layer import generate_gold_files

from load_dim_location import load_dim_location
from load_dim_brewery_type import load_dim_brewery_type
from load_dim_brewery_name import load_dim_brewery_name
from load_dim_time import load_dim_time
from load_fact_breweries import load_fact_breweries

# ===== DAG definition =====

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='breweries_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Pipeline ETL completo para dados de cervejarias usando API e carga em DW'
) as dag:

    # Bronze
    t_bronze = PythonOperator(
        task_id='fetch_raw_data',
        python_callable=fetch_raw_data
    )

    # Silver
    t_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=transform_and_partition
    )

    # Gold
    t_gold = PythonOperator(
        task_id='generate_gold',
        python_callable=generate_gold_files
    )

    # Dimensões
    t_load_dim_location = PythonOperator(
        task_id='load_dim_location',
        python_callable=load_dim_location
    )

    t_load_dim_type = PythonOperator(
        task_id='load_dim_brewery_type',
        python_callable=load_dim_brewery_type
    )

    t_load_dim_name = PythonOperator(
        task_id='load_dim_brewery_name',
        python_callable=load_dim_brewery_name
    )

    t_load_dim_time = PythonOperator(
        task_id='load_dim_time',
        python_callable=load_dim_time
    )

    # Fato
    t_load_fact = PythonOperator(
        task_id='load_fact_breweries',
        python_callable=load_fact_breweries
    )

    # ===== Dependências =====
    t_bronze >> t_silver >> t_gold

    t_gold >> [t_load_dim_location, t_load_dim_type, t_load_dim_name, t_load_dim_time]

    [t_load_dim_location, t_load_dim_type, t_load_dim_name, t_load_dim_time] >> t_load_fact
