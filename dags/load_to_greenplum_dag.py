import json
import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.load_to_bronze import load_to_bronze_spark, load_to_bronze_postgreSQL
from functions.load_to_silver import load_to_silver, transform_and_load_to_silver
from functions.load_to_greenplum import load_to_greenplum

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
directory_path = "configurations"
config_path = "config.yaml"
config_abspath = os.path.join(file_path, directory_path, config_path)
config_dir_path = f"{file_path}{directory_path}/"
sys.path.append(file_path)
sys.path.append(config_dir_path)

from functions.config import Config

config = Config(config_abspath)

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="load_to_greenplum_dag",
    description="",
    schedule_interval="0 2 * * *",
    start_date=datetime(2021, 11, 19),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id='start_load_to_greenplum',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_greenplum',
    dag=dag
)

load_to_gp = PythonOperator(
    task_id=f"load_to_greenplum",
    python_callable=load_to_greenplum,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

dummy1 >> load_to_gp >> dummy2
