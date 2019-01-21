#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
from datetime import datetime
from jinja2 import Template

import database

WEBUI_TEMPLATE = '../www/status.html.tmpl'
WEBUI_HTML = '../www/status.html'


def write_webpage(page_html):
    with open(WEBUI_HTML, 'w') as html_f:
        html_f.write(page_html)


def generate_html():
    master_sqlite_fp = './exports.sqlite'
    export_db = database.ExportsDatabase(master_sqlite_fp)

    archived_statuses = []
    for row in export_db.fetch_archived_statuses():
        archive_time, name, start, end = row
        archived_statuses.append({
            'survey_name': name,
            'survey_start': datetime.utcfromtimestamp(start).isoformat(),
            'survey_end': datetime.utcfromtimestamp(end).isoformat(),
            'archive_status': 'backups created',
            'archive_time': datetime.utcfromtimestamp(archive_time).isoformat(),
            'link': None
        })

    active_statuses = []
    for row in export_db.fetch_active_statuses():
        name, start, end = row
        active_statuses.append({
            'survey_name': name,
            'survey_start': datetime.utcfromtimestamp(start).isoformat(),
            'survey_last_update': datetime.utcfromtimestamp(end).isoformat()
        })

    with open(WEBUI_TEMPLATE, 'r') as tmpl_f:
        template = Template(tmpl_f.read())
        rendered = template.render(archived_surveys=archived_statuses,
                                   active_surveys=active_statuses)
        write_webpage(rendered)


if __name__ == '__main__':
    generate_html()
