from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from functions.load_to_bronze import load_to_bronze_API
from functions.load_to_silver import load_to_silver_API

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

dag = DAG(
    dag_id="extract_API_dag",
    description="Load data from API_url to DataLake",
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



load_to_bronze_task = PythonOperator(
    task_id="API_load_to_bronze",
    python_callable=load_to_bronze_API,
    provide_context=True,
    dag=dag
)



dummy1 >> load_to_bronze_task >> dummy2
