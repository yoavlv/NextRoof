from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'NextRoof_WorkFlow',
    default_args=default_args,
    description='NextRoof WorkFlow Daily Run',
    schedule_interval=timedelta(days=1),
)

def run_etl_script():
    script_path = "/opt/airflow/nextroof/main/main.py"
    results = subprocess.run(['python', script_path], capture_output=True, text=True)
    if results.returncode != 0:
        raise Exception(f"Script failed with error: {results.stderr}")
    else:
        print(results.stdout)

t1 = PythonOperator(
    task_id="Run_ETL_Script",
    python_callable=run_etl_script,
    dag=dag
)



t1




