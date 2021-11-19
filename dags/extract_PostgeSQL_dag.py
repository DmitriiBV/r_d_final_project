import json
import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.load_to_bronze import  load_to_bronze_postgreSQL

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
sys.path.append(file_path)

pg_conn = BaseHook.get_connection('oltp_postgres')
tables_to_load = json.loads(pg_conn.extra)['tables']
load_to_bronze_tasks = []


default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="extract_postgreSQL_dag",
    description="Load data from PostgreSQL data base to Data Lake",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 11, 19),
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
