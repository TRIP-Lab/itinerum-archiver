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
    return cfg


def copy_psql_sqlite(source_db, dest_db, table_name, mobile_ids, json_cols=None, float_cols=None):
    '''Read the colums from existing PostgreSQL table, create the output SQLite 
       table, and copy all rows for variousr `mobile_id` from input to output dbs.'''
    cols = source_db.table_schema(table_name)
    dest_db.generate_table(table_name, cols)
    rows = source_db.select_all(table_name, mobile_ids, json_cols, float_cols)
    dest_db.insert_many(table_name, cols, rows)


def dump_csv_survey_responses(source_db, csv_dir, mobile_ids, survey_id):
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
    responses = source_db.fetch_survey_responses(mobile_ids=mobile_ids)
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


def dump_csv_coordinates(source_db, csv_dir, mobile_ids):
    header = ['uuid', 'latitude', 'longitude', 'altitude', 'speed', 'direction',
              'h_accuracy', 'v_accuracy', 'acceleration_x', 'acceleration_y', 'acceleration_z',
              'mode_detected', 'point_type', 'timestamp_UTC', 'timestamp_epoch']
    coordinates = source_db.fetch_coordinates(mobile_ids=mobile_ids)
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


def dump_csv_prompts(source_db, csv_dir, mobile_ids):
    timestamp_cols = ['displayed_at', 'recorded_at', 'edited_at']
    header = ['uuid', 'prompt_uuid', 'prompt_num', 'response', 'displayed_at_UTC',
              'displayed_at_epoch', 'recorded_at_UTC', 'recorded_at_epoch',
              'edited_at_UTC', 'edited_at_epoch', 'latitude', 'longitude']

    # group the prompt responses by displayed_at
    prompts = source_db.fetch_prompt_responses(mobile_ids=mobile_ids)
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


def dump_csv_cancelled_prompts(source_db, csv_dir, mobile_ids):
    header = ['uuid', 'prompt_uuid', 'latitude', 'longitude', 'displayed_at_UTC', 
              'displayed_at_epoch', 'cancelled_at_UTC', 'cancelled_at_epoch',
              'is_travelling']

    prompts = source_db.fetch_prompt_responses(mobile_ids=mobile_ids)
    answered_prompt_times = _prompt_timestamps_by_uuid(prompts)

    cancelled_prompts = source_db.fetch_cancelled_prompt_responses(mobile_ids=mobile_ids)
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
    if not os.path.exists(cfg['archive']['output_dir']):
        logger.info('Creating output directory: %s' % cfg['archive']['output_dir'])
        os.mkdir(cfg['archive']['output_dir'])

    # coerce accented survey_name to pure ASCII version appending
    # an underscore after any previously accented characters
    nfkd_form = unicodedata.normalize('NFKD', cfg['archive']['survey_name'])
    survey_name = u''.join([c if not unicodedata.combining(c)
                            else '_' for c in nfkd_form])
    survey_name = survey_name.replace(' ', '_').replace('\'', '')
    cutoff_date = dateutil.parser.parse(cfg['archive']['cutoff_date'])

    mobile_ids = source_db.latest_signups_in_survey(survey_name=cfg['archive']['survey_name'],
                                                    cutoff=cutoff_date)

    # step 3: archive inactive surveys to .sqlite
    dest_sqlite_fn = '{}_users.sqlite'.format(survey_name)
    dest_sqlite_fp = os.path.join(cfg['archive']['output_dir'], dest_sqlite_fn)
    if os.path.exists(dest_sqlite_fp):
        os.remove(dest_sqlite_fp)
    logger.info('Export {survey} to {fn}'.format(survey=survey_name,
                                                 fn=dest_sqlite_fp))
    dest_db = fileio.SQLiteDatabase(dest_sqlite_fp)
    copy_psql_sqlite(source_db, dest_db, 'mobile_users', mobile_ids)
    copy_psql_sqlite(source_db, dest_db, 'mobile_survey_responses', mobile_ids,
        json_cols=['response'])
    copy_psql_sqlite(source_db, dest_db, 'mobile_coordinates', mobile_ids,
        float_cols=[
            'latitude', 'longitude', 'altitude', 'speed', 'direction', 'h_accuracy',
            'v_accuracy', 'acceleration_x', 'acceleration_y', 'acceleration_z'])
    copy_psql_sqlite(source_db, dest_db, 'mobile_prompt_responses', mobile_ids,
        json_cols=['response'], float_cols=['latitude', 'longitude'])
    copy_psql_sqlite(source_db, dest_db, 'mobile_cancelled_prompt_responses', mobile_ids,
        float_cols=['latitude', 'longitude'])

    # step 5: archive inactive surveys to .csv                 
    csv_dir_fn = '{survey}-csv_users'.format(survey=survey_name)
    csv_dir = os.path.join(cfg['archive']['output_dir'], csv_dir_fn)
    logger.info('Export {survey} as .csv files to {dir}'.format(survey=survey_name,
                                                                dir=csv_dir))
    if os.path.exists(csv_dir):
        shutil.rmtree(csv_dir)
    os.mkdir(csv_dir)

    logger.info('Export survey_responses.csv')
    survey_id = source_db.get_survey_id(cfg['archive']['survey_name'])
    dump_csv_survey_responses(source_db, csv_dir, mobile_ids, survey_id)
    logger.info('Export coordinates.csv')
    dump_csv_coordinates(source_db, csv_dir, mobile_ids)
    logger.info('Export prompt_responses.csv')
    dump_csv_prompts(source_db, csv_dir, mobile_ids)
    logger.info('Export cancelled_prompts.csv')
    dump_csv_cancelled_prompts(source_db, csv_dir, mobile_ids)

    # step 7: compress .csv dir and .sqlite database
    logger.info('Compress output files and directories')
    fileio.create_archive(dest_sqlite_fp)
    fileio.create_archive(csv_dir)


if __name__ == '__main__':
    main()
