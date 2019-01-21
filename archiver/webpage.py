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


def generate_html(exports_sqlite_fp=None):
    if not exports_sqlite_fp:
        exports_sqlite_fp = './exports.sqlite'
    exports_db = database.ExportsDatabase(exports_sqlite_fp)

    archived_statuses = []
    for row in exports_db.fetch_archived_statuses():
        archive_time, name, _id, start, end, s3_uri = row
        start_UTC = None
        if start:
            start_UTC = datetime.utcfromtimestamp(start).isoformat()
        end_UTC = None
        if end:
            end_UTC = datetime.utcfromtimestamp(end).isoformat()

        archived_statuses.append({
            'survey_name': name,
            'survey_start': start_UTC,
            'survey_end': end_UTC,
            'archive_status': 'archived' if s3_uri else 'backups created',
            'archive_time': datetime.utcfromtimestamp(archive_time).isoformat(),
            'archive_link': s3_uri
        })

    active_statuses = []
    for row in exports_db.fetch_active_statuses():
        name, start, end = row
        start_UTC = None
        if start:
            start_UTC = datetime.utcfromtimestamp(start).isoformat()
        end_UTC = None
        if end:
            end_UTC = datetime.utcfromtimestamp(end).isoformat()

        active_statuses.append({
            'survey_name': name,
            'survey_start': start_UTC,
            'survey_last_update': end_UTC
        })

    with open(WEBUI_TEMPLATE, 'r') as tmpl_f:
        template = Template(tmpl_f.read())
        rendered = template.render(archived_surveys=archived_statuses,
                                   active_surveys=active_statuses)
        write_webpage(rendered)


if __name__ == '__main__':
    generate_html()
