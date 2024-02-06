import datetime
import sys
sys.path.append('/madlan')
from madlan.madlan_main import madlan_main
sys.path.append('/nadlan')
from nadlan.nadlan_main import nadlan_main
import ssl
sys.path.append('/algorithms')
from algorithms.model import train_model_main
sys.path.append('/sql')
from dev import password
from email.message import EmailMessage
import smtplib
import json
from handlers import city_dict
import time

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


def run_new(train=True, clean=False, maintenance=False):
    run_status = {}
    current_time = datetime.datetime.now()
    run_status['date'] = current_time.strftime("%Y-%m-%d %H:%M")

    start_time = time.time()
    run_status['nadlan_main'] = nadlan_main(city_dict, num_of_pages=200, maintenance=maintenance)
    nadlan_main_time = time.time() - start_time
    run_status['nadlan_main_time'] = f"{nadlan_main_time:.2f} seconds"

    run_status['train_model'] = False
    if train:
        start_time = time.time()
        run_status['train_model'] = train_model_main(city_dict)
        train_model_time = time.time() - start_time
        run_status['train_model_time'] = f"{train_model_time:.1f} seconds"

    start_time = time.time()
    run_status['madlan_main'] = madlan_main(city_dict, clean)
    madlan_main_time = time.time() - start_time
    run_status['madlan_main_time'] = f"{madlan_main_time:.1f} seconds"

    try:
        send_daily_status(run_status)
    except Exception as e:
        run_status['send_daily_status_error'] = str(e)

    return run_status


status = run_new(train=False, clean=False, maintenance=False)

print(status)


def find_errors(data, errors, path=""):
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path} -> {key}" if path else key
            if key == "error" and value is not None:
                errors[new_path] = value
                print(f"{new_path} : {value}")
            find_errors(value, errors, new_path)
