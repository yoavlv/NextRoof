import datetime
import sys
sys.path.append('/madlan')
from madlan.madlan_main import madlan_main
sys.path.append('/nadlan')
from nadlan.nadlan_main import nadlan_main
import ssl
sys.path.append('/Algorithms')
from Algorithms.model import train_model_main
sys.path.append('/sql')
from dev import password
from email.message import EmailMessage
import smtplib
import json
from handlers import city_dict
def send_daily_status(data):
    formatted_data = json.dumps(data, indent=4)
    email_sender = 'yoavlv12@gmail.com'
    email_pass = password
    email_receiver = 'yoavlv12@gmail.com'
    subject = "test mail"
    em = EmailMessage()
    em.set_content(formatted_data, subtype='plain')
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com',465,context=context) as smtp:
        smtp.login(email_sender,email_pass)
        smtp.sendmail(email_sender,email_receiver ,em.as_string())

def run_new():
    run_status = {}
    current_time = datetime.datetime.now()
    run_status['date'] = str(datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day,hour=current_time.hour, minute=current_time.minute))
    run_status['nadlan_main'] = nadlan_main(city_dict, num_of_pages=2)
    run_status['train_model'] = train_model_main(city_dict)
    run_status['madlan_main'] = madlan_main(city_dict)
    send_daily_status(run_status)
    return run_status


status = run_new()
print(status)



def find_errors(data, errors, path=""):
    if isinstance(data, dict):
        for key, value in data.items():
            new_path = f"{path} -> {key}" if path else key
            if key == "error" and value is not None:
                errors[new_path] = value
                print(f"{new_path} : {value}")
            find_errors(value, errors, new_path)