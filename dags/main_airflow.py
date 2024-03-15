from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from nadlan.nadlan_main import nadlan_main
from madlan.madlan_main import madlan_main
from algorithms.model import train_model_main
from nadlan.sql_reader_nadlan import read_from_population
from dags.email import send_daily_status
from airflow.models import TaskInstance
import time

city_code_l = [5000, 8600, 6300, 3000, 7900, 6600, 6400, 8700, 6200, 6100, 6900, 2650, 8300, 4000, 70, 7400, 9000, 8300, 70, 9000]
city_code_list = list(set(city_code_l))
city_dict = read_from_population(city_id_list=city_code_list)

params = {
    'nadlan_params': {
        'num_of_pages': 2,
        'maintenance': False,
        'rank': True,
    },
    'madlan_params': {
        'maintenance': False,
    },
    'model_params': {
        'train': False,
        'find_best_params': False,
        'best_params': False,
        'lean_params': True
    },
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 2),
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
def wrapper_nadlan():
    start_time = time.time()
    result = nadlan_main(city_dict, params['nadlan_params'])
    total_time = time.time() - start_time
    result['time'] = total_time
    return result

def wrapper_madlan():
    start_time = time.time()
    result = madlan_main(city_dict, params['madlan_params'])
    total_time = time.time() - start_time
    result['time'] = total_time
    return result

def wrapper_model():
    start_time = time.time()
    result = train_model_main(city_dict, params['model_params'])
    total_time = time.time() - start_time
    result['time'] = total_time
    return result

def wrapper_email(**kwargs):
    results = {}
    ti: TaskInstance = kwargs['ti']
    results['nadlan'] = ti.xcom_pull(task_ids='task_nadlan')
    results['madlan'] = ti.xcom_pull(task_ids='task_madlan')
    results['model'] = ti.xcom_pull(task_ids='task_model')
    send_daily_status(results)

def send_email_wrapper(**kwargs):
    ti = kwargs['ti']
    nadlan_result = ti.xcom_pull(task_ids='run_nadlan_main')
    madlan_result = ti.xcom_pull(task_ids='run_madlan_main')
    model_result = ti.xcom_pull(task_ids='run_model_main')

    email_data = {
        "Nadlan Task Result": nadlan_result,
        "Madlan Task Result": madlan_result,
        "Model Task Result": model_result,
    }
    send_daily_status(email_data)


task_nadlan = PythonOperator(
    task_id='task_nadlan',
    python_callable=wrapper_nadlan,
    dag=dag,
)

task_madlan = PythonOperator(
    task_id='task_madlan',
    python_callable=wrapper_madlan,
    dag=dag,
)

task_model = PythonOperator(
    task_id='task_model',
    python_callable=wrapper_model,
    dag=dag,
)


task_email = PythonOperator(
    task_id='task_email',
    python_callable=send_email_wrapper,
    provide_context=True,
    dag=dag,
)

# Define the task sequence
task_nadlan >> task_model
# task_madlan >> task_model
# task_model >> task_email