#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import boto3
import logging
import os
import sqlite3
import sys
import zipfile


EXPORTS_DB = '../archiver/exports.sqlite'
EXPORTS_DATA_DIR = '../archiver/output'
WORKING_DATA_DIR = './temp'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# create output temp dir if it doesnt exist
if not os.path.exists(WORKING_DATA_DIR):
    os.mkdir(WORKING_DATA_DIR)

# get survey names without a database record of completed s3 push (url)
conn = sqlite3.connect(EXPORTS_DB)
cur = conn.cursor()
sql = '''SELECT survey_name FROM exports WHERE s3_uri IS NULL;'''
cur.execute(sql)
surveys_to_upload = [s for s, in cur.fetchall()]
conn.close()

# group completed exports into a single .zip archive in temp dir
archive_groups = {}
for filename in os.listdir(EXPORTS_DATA_DIR):
    base_name = filename.split('.')[0].split('-')[0]
    if base_name in surveys_to_upload and filename.endswith('.gz'):
        archive_groups.setdefault(base_name, []).append(filename)


archives = []
for survey_name, group in archive_groups.items():
    if len(group) != 3:
        print(group)
        continue

    fps_to_archive = []
    for export_fn in group:
        export_fp = os.path.join(EXPORTS_DATA_DIR, export_fn)
        fps_to_archive.append(export_fp)

    archive_fn = '{survey}.zip'.format(survey=survey_name)
    archive_fp = os.path.join(WORKING_DATA_DIR, archive_fn)
    with zipfile.ZipFile(archive_fp, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        for export_fp in fps_to_archive:
            export_fn = export_fp.split('/')[-1]
            zip_f.write(export_fp, arcname=export_fn)
    archives.append((survey_name, archive_fn, archive_fp))
    break

# push .zip archive to s3
conn = sqlite3.connect(EXPORTS_DB)
cur = conn.cursor()

s3 = boto3.resource('s3')
for survey_name, archive_fn, archive_fp in archives:
    s3.meta.client.upload_file(archive_fp, 'itinerum-cold-storage', archive_fn)
    logger.info('Push {fn} archive to S3 complete'.format(fn=archive_fn))

    # update exports db with link
    base_s3_uri = 'https://s3.ca-central-1.amazonaws.com/itinerum-cold-storage'
    s3_uri = '{base}/{key}'.format(base=base_s3_uri, key=archive_fn)
    sql = '''UPDATE exports
             SET s3_uri='{uri}'
             WHERE survey_name='{name}';'''.format(
        uri=s3_uri, name=survey_name)
    cur.execute(sql)
conn.commit()
conn.close()

# regenerate status page
sys.path.append('../archiver')
import webpage
webpage.generate_html('../archiver/exports.sqlite')
