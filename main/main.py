import ssl
from dev import password
from email.message import EmailMessage
import smtplib
import json
import time
import datetime
from madlan.madlan_main import madlan_main
from nadlan.nadlan_main import nadlan_main
from nadlan.sql_reader_nadlan import read_from_population
from algorithms.model import train_model_main
from datetime import datetime


params = {
    'nadlan_params':{
        'num_of_pages': 0,
        'maintenance': False,
        'rank': True,
    },
    'madlan_params': {
        'clean':False,
    },
    'model_params':{
        'train': False,
        'find_best_params': False,
        'best_params': False,
        'lean_params': True
    },
}
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
    city_code_list = [6400, 4000,70,5000,6600,7400,9000, 8300,6300,6100, 8700, 3000, 6900, 8600, 2650, 6200, 7900]
    city_dict = read_from_population(city_id_list=city_code_list)
    run_status = {}
    current_time = datetime.now()

    run_status['date'] = current_time.strftime("%Y-%m-%d %H:%M")

    start_time = time.time()
    run_status['nadlan_main'] = nadlan_main(city_dict, params['nadlan_params'])
    nadlan_main_time = time.time() - start_time
    run_status['nadlan_main_time'] = f"{nadlan_main_time:.2f} seconds"

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


status = run_new(params)
print(status)
