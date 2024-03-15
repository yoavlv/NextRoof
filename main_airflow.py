from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Assuming your project's functionality is encapsulated in the run_new function
from .main import run_new, params  # Adjust the import path as necessary

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 18),  # Adjust the start date accordingly
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'your_project_dag',
    default_args=default_args,
    description='A simple DAG to run your_project',
    schedule_interval=timedelta(days=1),
)

run_project_task = PythonOperator(
    task_id='run_your_project',
    python_callable=run_new,
    op_kwargs={'params': params},
    dag=dag,
)

