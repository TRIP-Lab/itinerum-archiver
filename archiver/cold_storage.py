#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import boto3
import os
import shutil
import sqlite3
import zipfile

import database


## GLOBALS
EXPORTS_DATA_DIR = './output'
WORKING_DATA_DIR = './temp'

exports_db = database.ExportsDatabase('./exports.sqlite')


# get survey names without a database record of completed s3 push (url)
def fetch_surveys_to_push():
    sql = '''SELECT survey_name FROM exports WHERE s3_uri IS NULL;'''
    exports_db._query(sql)
    return [s for s, in exports_db._db_cur.fetchall()]


# group completed exports into a single .zip archive in temp dir
def create_archive_file_groups(survey_names):
    archive_groups = {}
    for filename in os.listdir(EXPORTS_DATA_DIR):
        base_name = filename.split('.')[0].split('-')[0]
        if base_name in survey_names and filename.endswith('.gz'):
            archive_groups.setdefault(base_name, []).append(filename)
    return archive_groups


def create_single_file_archive(file_groups):
    archives = []
    for survey_name, group in file_groups.items():
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
    return archives


def upload_s3(archives):
    s3 = boto3.resource('s3')
    for survey_name, archive_fn, archive_fp in archives:
        s3.meta.client.upload_file(archive_fp, 'itinerum-cold-storage', archive_fn)
        logger.info('Push {fn} archive to S3 complete'.format(fn=archive_fn))

        # update exports 0db with link
        base_s3_uri = 'https://s3.ca-central-1.amazonaws.com/itinerum-cold-storage'
        s3_uri = '{base}/{key}'.format(base=base_s3_uri, key=archive_fn)
        sql = '''UPDATE exports
                 SET s3_uri='{uri}'
                 WHERE survey_name='{name}';'''.format(
            uri=s3_uri, name=survey_name)
        exports_db._query(sql)
    exports_db._db_conn.commit()


def push_archives_to_s3():
    # create output temp dir if it doesnt exist
    if not os.path.exists(WORKING_DATA_DIR):
        os.mkdir(WORKING_DATA_DIR)

    survey_names = fetch_surveys_to_push()
    file_groups = create_archive_file_groups(survey_names)
    archives = create_single_file_archive(file_groups)
    upload_s3(archives)

    # clean-up temp data dir
    shutil.rmtree(WORKING_DATA_DIR)
