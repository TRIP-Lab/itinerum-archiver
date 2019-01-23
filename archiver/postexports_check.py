#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import logging
import sys

from archiver import load_config
import database


## GLOBALS
CFG_FN = './config.json'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_records(db, _id, survey_name):
    def _check(sql):
        db._query(sql)
        result, = db._db_cur.fetchone()
        try:
            assert result == 0
        except AssertionError:
            logging.info('Records exist for: {name}'.format(name=survey_name))
            logging.info(sql)
            logging.info('Exiting...')
            sys.exit(1)        

    id_tables = [
        'surveys',
        'statistics_surveys'
    ]

    survey_id_tables = [
        'mobile_cancelled_prompt_responses',
        'mobile_coordinates',
        'mobile_prompt_responses',
        'mobile_survey_responses',
        'mobile_users',
        'prompt_questions',
        'statistics_mobile_users',
        'survey_questions',
        'survey_subway_stops',
        'tokens_researcher_invite',
        'web_users'
    ]

    for table in id_tables:
        count_sql = '''SELECT COUNT(*) FROM {table} WHERE id={id};'''.format(
            table=table,
            id=_id)
        _check(count_sql)

    for table in survey_id_tables:
        count_sql = '''SELECT COUNT(*) FROM {table} WHERE survey_id={id};'''.format(
            table=table,
            id=_id)
        _check(count_sql)


    
def main():
    cfg = load_config(CFG_FN)
    exports_sqlite_fp = './exports.sqlite'
    exports_db = database.ExportsDatabase(exports_sqlite_fp)
    source_db = database.ItinerumDatabase(**cfg['source_db'])    

    for _, name, _id, start, end, _ in exports_db.fetch_archived_statuses():
        logger.info('Checking successful delete of {name} survey records...'.format(
            name=name))
        check_records(source_db, _id, name)



if __name__ == '__main__':
    main()
