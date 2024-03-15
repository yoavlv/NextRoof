from dev import password
from email.message import EmailMessage
import smtplib
import json
import ssl

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