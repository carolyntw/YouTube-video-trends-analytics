from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scrapper import run_youtube_etl
from data_processing import process_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'youtube_etl_dag',
    default_args=default_args,
    description='A DAG for YouTube ETL',
)

run_etl = PythonOperator(
    task_id='run_youtube_etl',
    python_callable=run_youtube_etl,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_youtube_data',
    python_callable=process_data,
    dag=dag,
)

run_etl >> process_data_task
