import datetime
from dataProcess import CleanData
import pandas as pd
import sys
from monitor import monitor_data , find_errors
sys.path.append('C:/Users/yoavl/NextRoof/Scraping')
sys.path.append('C:/Users/yoavl/NextRoof/Algorithms')
from dev import password
from Algorithms.model import init_model
from Algorithms.calc_results import calc_results
from Scraping.madlan import run_madlan
from Scraping.nadlan_scrape import run_nadlan
from Clean.data_cleaning_nadlan import run_nadlan_clean
from email.message import EmailMessage
import ssl
import smtplib
import json

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

def data_cleaning():
    try:
        df_madlan = pd.read_csv('C:/Users/yoavl/NextRoof/Data/madlan_data.csv')
        madlan = CleanData(df_madlan, 'madlan')
        madlan.saveDataFrame('C:/Users/yoavl/NextRoof/Data/madlan_data_clean.csv')
        monitor_data['Clean']['madlan']['Total_size'] = madlan.df.shape
        monitor_data['Clean']['madlan']['status'] = 'Success'

    except Exception as e:
        monitor_data['Clean']['madlan']['status'] = 'Fail'
        monitor_data['Clean']['madlan']['error'] = e

    try:
        df_yad2 = pd.read_csv('C:/Users/yoavl/NextRoof/Data/yad_2_data.csv')
        yad2 = CleanData(df_yad2, 'yad2')
        yad2.saveDataFrame('C:/Users/yoavl/NextRoof/Data/yad_2_data_clean.csv')
        monitor_data['Clean']['yad2']['Total_size'] = yad2.df.shape
        monitor_data['Clean']['yad2']['status'] = 'Success'
    except Exception as e:
        monitor_data['Clean']['yad2']['status'] = 'Fail'
        monitor_data['Clean']['yad2']['error'] = e

def daily_run():
    current_time = datetime.datetime.now()
    monitor_data['Date'] = str(datetime.datetime(year=current_time.year, month=current_time.month, day=current_time.day,hour=current_time.hour, minute=current_time.minute))
    try:
        run_nadlan(10,10)
        print(f"run_nadlan {monitor_data}")
        run_nadlan_clean()
        print(f"run_nadlan_clean {monitor_data}")
        run_madlan()
        print(f"run_madlan {monitor_data}")
        data_cleaning()
        print(f"data_cleaning {monitor_data}")
        # print("data_cleaning pass")
        init_model()
        print("init_model pass")
        print(f"init_model {monitor_data}")
        calc_results(yad2=True)
        print(f"calc_results yad2 {monitor_data}")
        calc_results(madlan=True)
        print(f"calc_results madlan {monitor_data}")
        errors = dict()
        find_errors(monitor_data, errors)

        send_daily_status(monitor_data , errors)
    except Exception as e:
        error = {"error" : e}
        send_daily_status(error)

daily_run()

