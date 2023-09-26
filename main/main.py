import datetime
import sys
from monitor import monitor_data , find_errors
sys.path.append('/Scraping')
sys.path.append('/Algorithms')
sys.path.append('/sql')
from dev import password
from Algorithms.model import init_model
from handlers import nadlan_handler , madlan_handler,yad2_handler
from email.message import EmailMessage
import ssl
import smtplib
import json
import traceback
import os

def daily_run(run_model = False):
    current_time = datetime.datetime.now()
    monitor_data['Date'] = str(datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day,hour=current_time.hour, minute=current_time.minute))
    try:
        nadlan_handler()
        madlan_handler()
        yad2_handler()
        if run_model == True:
            init_model()
        errors = dict()
        find_errors(monitor_data, errors)
        send_daily_status(monitor_data , errors)
    except Exception as e:
        print(e)
        error = {"error" : e}
        # send_daily_status(error)
def send_daily_status(data , errors = None):
    formatted_data = json.dumps(data, indent=4)
    formatted_errors = json.dumps(errors, indent=4)

    email_sender = 'yoavlv12@gmail.com'
    email_pass = password
    email_receiver = 'yoavlv12@gmail.com'
    subject = "test mail"
    em = EmailMessage()
    em['From'] = email_sender
    em['To'] = email_receiver
    em['Subject'] = subject
    em.set_content(f"errors: {formatted_errors}\n\n\n{formatted_data}")
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL('smtp.gmail.com',465,context=context) as smtp:
        smtp.login(email_sender,email_pass)
        smtp.sendmail(email_sender,email_receiver ,em.as_string())

daily_run()

# madlan(286)
# nadaln(53833)

def delete_files(paths):
    """
    Deletes files at the specified paths.
    :param paths: List of paths to files to be deleted.
    """
    start = 'C:/Users/yoavl/NextRoof/Data/'
    for path in paths:
        path = start +path
        try:
            if os.path.exists(path):
                os.remove(path)
                print(f"Successfully deleted {path}")
            else:
                print(f"No file found at {path}")
        except Exception as e:
            print(f"Error deleting the file at {path}: {e}")

# files = ['nadlan_p.csv','nadlan_clean_p.csv','madlan_p.csv','madlan_data_clean_p.csv','madlan_predict_p.csv']
# delete_files(files)