from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scrapper import run_youtube_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='A simple tutorial DAG for YouTube ETL',
)

run_etl = PythonOperator(
    task_id='run_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag,
)

run_etl
