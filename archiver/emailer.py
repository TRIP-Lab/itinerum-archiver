#!/usr/bin/env python
# Kyle Fitzsimmons, 2019
from datetime import datetime
from email.mime.text import MIMEText
from prettytable import PrettyTable
import smtplib


def send(recipient, sender_cfg, msg):
    with smtplib.SMTP(host=sender_cfg['host'],
                      port=sender_cfg['port'],
                      timeout=10) as smtp:
        smtp.ehlo()
        if sender_cfg['tls']:
            smtp.starttls()
        smtp.login(sender_cfg['address'], sender_cfg['password'])
        smtp.sendmail(sender_cfg['address'], recipient, msg.as_string())

def send_message(export_timestamp, recipient, sender_cfg, records):
    export_timestamp_UTC = datetime.utcfromtimestamp(export_timestamp).isoformat()

    table = PrettyTable()
    table.field_names = ['survey', 'start time (UTC)', 'end time (UTC)']
    for record in records:
        survey_name, start_time, end_time = record[2], record[3], record[4]
        start_time_UTC = None
        if start_time:
            start_time_UTC = datetime.utcfromtimestamp(start_time).isoformat()
        end_time_UTC = None
        if end_time:
            end_time_UTC = datetime.utcfromtimestamp(end_time).isoformat()
        table.add_row([survey_name, start_time_UTC, end_time_UTC])

    lines = [
        'Inactive survey archiver ran at: {ts}'.format(ts=export_timestamp_UTC),
        '',
        'Backups created for:',
        str(table),
    ]

    if not records:
        lines = [
            'Inactive survey archiver ran at: {ts}'.format(ts=export_timestamp_UTC),
            '',
            'No inactive surveys to backup.'
        ]

    msg = MIMEText('\n'.join(lines))
    msg['Subject'] = 'Itinerum data-archiver run: {ts}'.format(ts=export_timestamp_UTC)
    msg['From'] = sender_cfg['address']
    msg['To'] = recipient
    send(recipient, sender_cfg, msg)
