import json
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.load_to_bronze import load_to_bronze_spark, load_to_bronze_postgreSQL
from functions.load_to_silver import load_to_silver

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
directory_path = "configurations"
config_path = "config.yaml"
config_abspath = os.path.join(file_path, directory_path, config_path)
config_dir_path = f"{file_path}{directory_path}/"
sys.path.append(file_path)
sys.path.append(config_dir_path)

from functions.config import Config

config = Config(config_abspath)

pg_conn = BaseHook.get_connection('oltp_postgres')

# tables_to_load = config.get_config('TABLES_DSHOP')
#tables_to_load = ['orders', 'products', 'departments', 'aisles', 'clients', 'stores', 'store_types', 'location_areas']
tables_to_load = json.loads(pg_conn.extra)['tables']
#tables_to_load = pg_conn.extra.split()

load_to_bronze_tasks = []
load_to_silver_tasks = []

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="extract_postgreSQL_dag",
    description="Load data from PostgreSQL data base to Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2021, 11, 16),
    default_args=default_args
)


dummy1 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_bronze',
    dag=dag
)



for table in tables_to_load:
    load_to_bronze_tasks.append(
        PythonOperator(
            task_id=f"{table}_load_to_bronze",
            python_callable=load_to_bronze_postgreSQL,
            op_kwargs={'table': table},
            provide_context=True,
            dag=dag
        )
    )



dummy1 >> load_to_bronze_tasks >> dummy2
