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
    dag_id="load_API_dag",
    description="Load data from API_url to Data Lake",
    schedule_interval="@daily",
    start_date=datetime(2021, 11, 10),
    default_args=default_args
)

dummy1 = DummyOperator(
    task_id='start_pipline',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='start_load_to_bronze',
    dag=dag
)
dummy3 = DummyOperator(
    task_id='finish_load_to_bronze',
    dag=dag
)
dummy4 = DummyOperator(
    task_id='start_load_to_silver',
    dag=dag
)
dummy5 = DummyOperator(
    task_id='finish_load_to_silver',
    dag=dag
)
dummy6 = DummyOperator(
    task_id='finish_pipline',
    dag=dag
)

load_to_bronze_task = PythonOperator(
    task_id="API_load_to_bronze",
    python_callable=load_to_bronze_API,
    provide_context=True,
    dag=dag
)

load_to_silver_task = PythonOperator(
    task_id="API_load_to_silver",
    python_callable=load_to_silver_API,
    provide_context=True,
    dag=dag
)

dummy1 >> dummy2 >> load_to_bronze_task >> dummy3 >> dummy4 >> load_to_silver_task >> dummy5 >> dummy6
