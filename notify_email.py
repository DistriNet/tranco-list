import smtplib
from email.message import EmailMessage
import email.utils

import requests
from rq import Queue, Connection, get_current_connection
from global_config import MAILGUN_API_KEY

def send_notification_mailgun_api(email_address, list_id, list_size):
    with Connection(get_current_connection()):
        q = Queue('generate')
        job = q.fetch_job(list_id)
        success = job.result

    if success:
        subject = 'The Tranco list: generation succeeded'
        body = "Hello,\n\nWe have successfully generated your requested Tranco list with ID {}. You may retrieve it at https://tranco-list.eu/list/{}/{}\n\nTranco\nhttps://tranco-list.eu/".format(list_id, list_id, list_size)
    else:
        subject = 'The Tranco list: generation failed'
        body = "Hello,\n\nUnfortunately, we were currently unable to generate your requested Tranco list with ID {}. Please try again later.\n\nTranco\nhttps://tranco-list.eu/".format(list_id)

    r = requests.post(
            "https://api.eu.mailgun.net/v3/mg.tranco-list.eu/messages",
            auth=("api", MAILGUN_API_KEY),
            data={"from": "Tranco <noreply@mg.tranco-list.eu>",
                  "to": [email_address],
                  "subject": subject,
                  "text": body})
    return int(r.status_code) == 200