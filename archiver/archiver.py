#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import dateutil.parser
import json
import logging
import os
import shutil
import time
import unicodedata

import csv_formatters
import database
import emailer
import fileio


## GLOBALS
CFG_FN = './config.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(cfg_fn):
    '''Load a configuration JSON to Python dict.'''
    cfg = None
    with open(cfg_fn, 'r') as cfg_f:
        cfg = json.load(cfg_f)
        cfg['inactivity_date'] = dateutil.parser.parse(cfg['inactivity_date'])
    return cfg


def copy_psql_sqlite(source_db, dest_db, table_name, survey_id, json_cols=None, float_cols=None):
    '''Read the colums from existing PostgreSQL table, create the output SQLite 
       table, and copy all rows for a particular `survey_id` from input to output dbs.'''
    cols = source_db.table_schema(table_name)
    dest_db.generate_table(table_name, cols)
    rows = source_db.select_all(table_name, survey_id, json_cols, float_cols)
    dest_db.insert_many(table_name, cols, rows)


def create_psql_copy_table(source_db, table_name, survey_id, survey_name):
    '''Copy data from selected survey to restorable PostgreSQL to temporary table.'''
    dest_table_name = 'temp_{src}_{name}'.format(src=table_name,
                                                 name=survey_name)
    source_db.drop_table(dest_table_name)
    source_db.copy_all(table_name, dest_table_name, survey_id)


def drop_psql_copy_tables(source_db, survey_name, copy_tables):
    for table_name in copy_tables:
        temp_table_name = 'temp_{src}_{name}'.format(src=table_name,
                                                     name=survey_name)
        source_db.drop_table(temp_table_name)


def dump_csv_survey_responses(source_db, csv_dir, survey_id, survey_name):
    locations_cols = ['location_home', 'location_work', 'location_study']
    timestamp_cols = ['created_at', 'modified_at']
    exclude_cols = ['id', 'survey_id', 'mobile_id', 'response']

    mobile_users_cols = [col for col in source_db.table_cols('mobile_users')
                         if col not in exclude_cols]
    survey_questions = source_db.fetch_survey_questions(survey_id)
    survey_question_cols = []
    for q in survey_questions:
        col = q['question_label']
        if not col.lower() in locations_cols:
            survey_question_cols.append(col)

    header = csv_formatters.survey_response_header(mobile_users_cols,
                                                   survey_question_cols,
                                                   timestamp_cols,
                                                   locations_cols,
                                                   exclude_cols)
    responses = source_db.fetch_survey_responses(survey_id)
    csv_rows = []
    for user in responses:
        survey_response = user.get('response')
        # skip users who never completed a survey response
        if not survey_response:
            continue
        row = csv_formatters.survey_response_row(header, user, timestamp_cols, locations_cols)
        csv_rows.append(row)

    fp = os.path.join(csv_dir, 'survey_responses.csv')
    fileio.write_csv(fp, header, csv_rows)


def dump_csv_coordinates(source_db, csv_dir, survey_id, survey_name):
    header = ['uuid', 'latitude', 'longitude', 'altitude', 'speed', 'direction',
              'h_accuracy', 'v_accuracy', 'acceleration_x', 'acceleration_y', 'acceleration_z',
              'mode_detected', 'point_type', 'timestamp_UTC', 'timestamp_epoch']
    coordinates = source_db.fetch_coordinates(survey_id)
    csv_rows = []
    last_row = None  # filters points recorded as duplicates in database
    for point in coordinates:
        if int(point['latitude']) == 0 and int(point['longitude'] == 0):
            continue
        row = csv_formatters.coordinate_row(header, point)
        if row != last_row:
            csv_rows.append(row)
        last_row = row    

    fp = os.path.join(csv_dir, 'coordinates.csv')
    fileio.write_csv(fp, header, csv_rows)


def dump_csv_prompts(source_db, csv_dir, survey_id, survey_name):
    timestamp_cols = ['displayed_at', 'recorded_at', 'edited_at']
    header = ['uuid', 'prompt_uuid', 'prompt_num', 'response', 'displayed_at_UTC',
              'displayed_at_epoch', 'recorded_at_UTC', 'recorded_at_epoch',
              'edited_at_UTC', 'edited_at_epoch', 'latitude', 'longitude']

    # group the prompt responses by displayed_at
    prompts = source_db.fetch_prompt_responses(survey_id)
    grouped_prompts = csv_formatters.group_prompt_responses(prompts)
    
    csv_rows = []
    for prompt_response in grouped_prompts:
        row = csv_formatters.prompt_response_row(header, prompt_response)
        csv_rows.append(row)

    fp = os.path.join(csv_dir, 'prompt_responses.csv')
    fileio.write_csv(fp, header, csv_rows)


def _prompt_timestamps_by_uuid(prompts):
    answered_prompt_times = {}
    for p in prompts:
        uuid, displayed_at = p['uuid'], p['displayed_at_UTC']
        answered_prompt_times.setdefault(uuid, set()).add(displayed_at)
    return answered_prompt_times


def _duplicate_prompt_exists(cancelled, answered_prompt_times):
    uuid, displayed_at = cancelled['uuid'], cancelled['displayed_at_UTC']
    return displayed_at in answered_prompt_times.get(uuid, [])


def dump_csv_cancelled_prompts(source_db, csv_dir, survey_id, survey_name):
    header = ['uuid', 'prompt_uuid', 'latitude', 'longitude', 'displayed_at_UTC', 
              'displayed_at_epoch', 'cancelled_at_UTC', 'cancelled_at_epoch',
              'is_travelling']

    prompts = source_db.fetch_prompt_responses(survey_id)
    answered_prompt_times = _prompt_timestamps_by_uuid(prompts)

    cancelled_prompts = source_db.fetch_cancelled_prompt_responses(survey_id)
    csv_rows = []
    for cancelled in cancelled_prompts:
        if _duplicate_prompt_exists(cancelled, answered_prompt_times):
            continue
        row = csv_formatters.cancelled_prompt_row(header, cancelled)
        csv_rows.append(row)

    fp = os.path.join(csv_dir, 'cancelled_prompts.csv')
    fileio.write_csv(fp, header, csv_rows)


def main():
    run_timestamp = int(time.time())

    cfg = load_config(CFG_FN)
    source_db = database.ItinerumDatabase(**cfg['source_db'])

    # create output directory
    if not os.path.exists(cfg['output_dir']):
        logger.info('Creating output directory: %s' % cfg['output_dir'])
        os.mkdir(cfg['output_dir'])

    # step 1: fetch latest users for each survey and write to a timestamped
    #         .csv file
    logger.info('Finding most recent user by survey: %s' % cfg['output_dir'])
    surveys_latest_activity = source_db.latest_signups_by_survey()
    latest_signups_fn = 'surveys-latest_users.csv'
    latest_signups_fp = os.path.join(cfg['output_dir'], latest_signups_fn)
    header = ['survey id', 'survey name', 'last sign-up']
    fileio.write_csv(latest_signups_fp, header, surveys_latest_activity)

    # step 2: filter for surveys that have not been updated since config
    #         inactivity date
    inactive_surveys = filter(lambda row: row['last_created_at'] < cfg['inactivity_date'],
                              surveys_latest_activity)

    copy_tables = ['mobile_users', 'mobile_survey_responses', 'mobile_coordinates',
                   'mobile_prompt_responses', 'mobile_cancelled_prompt_responses']
    email_records = []
    for survey_id, survey_name, survey_last_user in inactive_surveys:
        # coerce accented survey_name to pure ASCII version appending
        # an underscore after any previously accented characters
        nfkd_form = unicodedata.normalize('NFKD', survey_name)
        survey_name = u''.join([c if not unicodedata.combining(c)
                                else '_' for c in nfkd_form])
        survey_name = survey_name.replace(' ', '_').replace('\'', '')

        # step 3: archive inactive surveys to .sqlite
        dest_sqlite_fn = '{}.sqlite'.format(survey_name)
        dest_sqlite_fp = os.path.join(cfg['output_dir'], dest_sqlite_fn)
        if os.path.exists(dest_sqlite_fp):
            os.remove(dest_sqlite_fp)
        logger.info('Export {survey} to {fn}'.format(survey=survey_name,
                                                     fn=dest_sqlite_fp))
        dest_db = fileio.SQLiteDatabase(dest_sqlite_fp)
        copy_psql_sqlite(source_db, dest_db, 'mobile_users', survey_id)
        copy_psql_sqlite(source_db, dest_db, 'mobile_survey_responses', survey_id,
            json_cols=['response'])
        copy_psql_sqlite(source_db, dest_db, 'mobile_coordinates', survey_id,
            float_cols=[
                'latitude', 'longitude', 'altitude', 'speed', 'direction', 'h_accuracy',
                'v_accuracy', 'acceleration_x', 'acceleration_y', 'acceleration_z']
        )
        copy_psql_sqlite(source_db, dest_db, 'mobile_prompt_responses', survey_id,
            json_cols=['response'], float_cols=['latitude', 'longitude'])
        copy_psql_sqlite(source_db, dest_db, 'mobile_cancelled_prompt_responses', survey_id,
            float_cols=['latitude', 'longitude'])

        # step 4: copy inactive surveys to temp postgresql tables, dump
        #         inactive surveys to .psql files and drop temp tables
        psql_dump_fn = '{survey}.psql.gz'.format(survey=survey_name)
        psql_dump_fp = os.path.join(cfg['output_dir'], psql_dump_fn)
        logger.info('Export {survey} to {fn}'.format(survey=survey_name,
                                                     fn=psql_dump_fn))
        create_psql_copy_table(source_db, 'mobile_users', survey_id, survey_name)
        create_psql_copy_table(source_db, 'mobile_survey_responses', survey_id, survey_name)
        create_psql_copy_table(source_db, 'mobile_coordinates', survey_id, survey_name)
        create_psql_copy_table(source_db, 'mobile_prompt_responses', survey_id, survey_name)
        create_psql_copy_table(source_db, 'mobile_cancelled_prompt_responses', survey_id, survey_name)
        fileio.dump_psql_copy_tables(psql_dump_fp, survey_name, **cfg['source_db'])
        drop_psql_copy_tables(source_db, survey_name, copy_tables)

        # step 5: archive inactive surveys to .csv                 
        csv_dir_fn = '{survey}-csv'.format(survey=survey_name)
        csv_dir = os.path.join(cfg['output_dir'], csv_dir_fn)
        logger.info('Export {survey} as .csv files to {dir}'.format(survey=survey_name,
                                                                    dir=csv_dir))
        if os.path.exists(csv_dir):
            shutil.rmtree(csv_dir)
        os.mkdir(csv_dir)

        logger.info('Export survey_responses.csv')
        dump_csv_survey_responses(source_db, csv_dir, survey_id, survey_name)
        logger.info('Export coordinates.csv')
        dump_csv_coordinates(source_db, csv_dir, survey_id, survey_name)
        logger.info('Export prompt_responses.csv')
        dump_csv_prompts(source_db, csv_dir, survey_id, survey_name)
        logger.info('Export cancelled_prompts.csv')
        dump_csv_cancelled_prompts(source_db, csv_dir, survey_id, survey_name)

        # step 6: write record to data-archiver master .sqlite to track export with
        #         survey start, survey end, and total records included in export as
        #         well as datetime of completed export
        logger.info('Update master database with export record')
        master_sqlite_fp = './exports.sqlite'
        export_db = database.SQLiteDatabase(master_sqlite_fp)
        record_cols = ['timestamp', 'survey_name', 'survey_start', 'survey_end']
        record_cols += ['count_' + t for t in copy_tables]
        
        start_time = source_db.start_time(survey_id)
        if start_time:
            start_time = int(start_time.timestamp())
        end_time = source_db.end_time(survey_id)
        if end_time:
            end_time = int(end_time.timestamp())
        record = [run_timestamp, survey_name, start_time, end_time]
        record += [dest_db.count(t) for t in copy_tables]
        export_db.upsert(record_cols, record)
        email_records.append(record)

        # step 7: compress .csv dir and .sqlite database
        logger.info('Compress output files and directories')
        fileio.create_archive(dest_sqlite_fp)
        fileio.create_archive(csv_dir)

        # step 8: delete backed-up survey rows and relevant indexes from database
        logger.info('Delete archived survey records from source database')
        source_db.delete_survey(survey_id)

    # step 9: send email with successful exports details and link to status webpage
    logger.info('Send notification of {num} exported surveys to {email}'.format(
        num=len(email_records), email=cfg['receiver_email']['address']))
    emailer.send_message(recipient=cfg['receiver_email']['address'],
                         sender_cfg=cfg['sender_email'],
                         records=email_records)

    # step 10: vacuum database to reclaim disk space
    logger.info('Vacuum database to free space from deleted records')
    source_db.vacuum()

if __name__ == '__main__':
    main()