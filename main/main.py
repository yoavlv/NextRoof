import datetime
import sys
import ssl
from dev import password
from email.message import EmailMessage
import smtplib
import json
import time
import datetime
from datetime import datetime

sys.path.append('/madlan')
from madlan.madlan_main import madlan_main
sys.path.append('/nadlan')
from nadlan.nadlan_main import nadlan_main
from nadlan.sql_reader_nadlan import read_from_population
sys.path.append('/algorithms')
from algorithms.model import train_model_main
sys.path.append('/sql')
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

city_code_l = [5000, 8600, 6300, 3000, 7900, 6600, 6400, 8700, 6200, 6100, 6900, 2650, 8300, 4000, 70, 7400, 9000,8300,70,9000]
city_code_list = list(set(city_code_l))
def send_daily_status(data):
    formatted_data = json.dumps(data, indent=4, ensure_ascii=False)
    email_sender = 'yoavlv12@gmail.com'
    email_pass = password
    email_receiver = 'yoavlv12@gmail.com'
    subject = "Daily Status NextRoof Workflow"
    em = EmailMessage()
    em.set_content(formatted_data, subtype='plain')
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
        smtp.login(email_sender, email_pass)
        smtp.sendmail(email_sender, email_receiver, em.as_string())


def run_new(params):
    city_dict = read_from_population(city_id_list=city_code_list)
    run_status = {}
    current_time = datetime.now()

    run_status['date'] = current_time.strftime("%Y-%m-%d %H:%M")

    start_time = time.time()
    # run_status['nadlan_main'] = nadlan_main(city_dict, params['nadlan_params'])
    # nadlan_main_time = time.time() - start_time
    # run_status['nadlan_main_time'] = f"{nadlan_main_time:.2f} seconds"

    if params['model_params']['train']:
        start_time = time.time()
        run_status['train_model'] = train_model_main(city_dict, params['model_params'])
        train_model_time = time.time() - start_time
        run_status['train_model_time'] = f"{train_model_time:.1f} seconds"

    start_time = time.time()
    run_status['madlan_main'] = madlan_main(city_dict, params['madlan_params'])
    madlan_main_time = time.time() - start_time
    run_status['madlan_main_time'] = f"{madlan_main_time:.1f} seconds"

    try:
        send_daily_status(run_status)
    except Exception as e:
        run_status['send_daily_status_error'] = str(e)

    return run_status


params = {
    'nadlan_params':{
        'num_of_pages': 1000,
        'maintenance': False,
        'rank': True,
    },
    'madlan_params': {
        'maintenance':False,

    },
    'model_params':{
        'train': True,
        'find_best_params': False,
        'best_params': False,
        'lean_params': True
    },

}

status = run_new(params)

print(status)


def find_errors(data, errors, path=""):
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path} -> {key}" if path else key
            if key == "error" and value is not None:
                errors[new_path] = value
                print(f"{new_path} : {value}")
            find_errors(value, errors, new_path)
