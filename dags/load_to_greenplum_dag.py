import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.load_to_greenplum import load_to_greenplum

file_path = f"{os.path.abspath(os.path.dirname(__file__))}/"
sys.path.append(file_path)

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
