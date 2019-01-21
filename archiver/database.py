#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
from datetime import datetime
import json
import logging
import psycopg2
import psycopg2.extras
import pytz
import sqlite3


logger = logging.getLogger(__name__)


HARDCODED_SERVER_START_TIME = datetime(2017, 5, 1, 0, 0, 0, tzinfo=pytz.UTC)
POSTGRES_SQLITE_TYPES = {
    'numeric': 'REAL',
    'integer': 'INTEGER',
    'double precision': 'REAL',
    'timestamp with time zone': 'DATETIME',
    'character varying': 'TEXT',
    'jsonb': 'TEXT',
    'boolean': 'INTEGER'
}


class PostgreSQLDatabase(object):

    def __init__(self, host, dbname, port, user, password):
        self._db_conn = psycopg2.connect(dbname=dbname,
                                         user=user,
                                         password=password,
                                         host=host,
                                         port=port,
                                         cursor_factory=psycopg2.extras.DictCursor)
        self._db_cur = self._db_conn.cursor()

    def __del__(self):
        self._db_conn.close()

    def _query(self, query, params=None):
        return self._db_cur.execute(query, params)

    def copy_all(self, src_table_name, dest_table_name, survey_id):
        sql = '''
            SELECT *
            INTO {dest_table}
            FROM {src_table}
            WHERE survey_id = {id};            
        '''.format(
            dest_table=dest_table_name,
            src_table=src_table_name,
            id=survey_id
        )
        self._db_cur.execute(sql)
        self._db_conn.commit()


    def drop_table(self, table_name):
        sql = '''
            DROP TABLE IF EXISTS {table};
        '''.format(
            table=table_name
        )
        self._db_cur.execute(sql)
        self._db_conn.commit()

    def table_cols(self, table_name):
        sql = '''SELECT * FROM {table} LIMIT 0;'''.format(table=table_name)
        self._query(sql)
        columns = [d[0] for d in self._db_cur.description]
        return columns

    def table_schema(self, table_name):
        sql = '''
            SELECT column_name, data_type, character_maximum_length
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = '{table}';
        '''.format(
            table=table_name
        )
        self._query(sql)
        columns = []
        for name, dtype, max_length in self._db_cur.fetchall():
            sqlite_dtype = POSTGRES_SQLITE_TYPES[dtype]
            columns.append((name, sqlite_dtype))
        return columns

    def vacuum(self):
        old_isolation_level = self._db_conn.isolation_level
        self._db_conn.set_isolation_level(0)

        logger.info('Analyze source database for vacuum')
        sql1 = '''VACUUM FULL ANALYZE;'''
        self._query(sql1)
        logger.info('Performing vacuum on source database')
        sql2 = '''VACUUM FULL;'''
        self._query(sql2)
        self._db_conn.commit()
        logger.info('Source database vacuum complete')

        self._db_conn.set_isolation_level(old_isolation_level)



class ItinerumDatabase(PostgreSQLDatabase):
    def __init__(self, host, dbname, port, user, password):
        super().__init__(host, dbname, port, user, password)

    def delete_survey(self, survey_id):
        # delete from tables progressively even though CASCADE is in place
        # to less load while dropping from each table individually
        
        # delete mobile data
        sql1 = '''DELETE FROM mobile_coordinates WHERE survey_id={id}'''.format(
            id=survey_id)
        self._query(sql1)

        sql2 = '''DELETE FROM mobile_prompt_responses WHERE survey_id={id}'''.format(
            id=survey_id)
        self._query(sql2)

        sql3 = '''DELETE FROM mobile_cancelled_prompt_responses WHERE survey_id={id}'''.format(
            id=survey_id)
        self._query(sql3)

        sql4 = '''DELETE FROM mobile_users WHERE survey_id={id}'''.format(
            id=survey_id)
        self._query(sql4)

        # delete dashboard data
        sql5 = '''
            DELETE FROM tokens_password_reset
            WHERE web_user_id IN (
                SELECT id
                FROM web_users
                WHERE survey_id={id}
            )
        '''.format(
            id=survey_id
        )
        self._query(sql5)

        sql6 = '''DELETE FROM surveys WHERE id={id};'''.format(
            id=survey_id)
        self._query(sql6)

        self._db_conn.commit()


    def end_time(self, survey_id):
        sql = '''
            SELECT timestamp
            FROM mobile_coordinates
            WHERE survey_id={survey_id}
            AND timestamp >= '{init_cutoff}'
            AND timestamp <= '{now_cutoff}'
            ORDER BY timestamp DESC
            LIMIT 1;
        '''.format(
            init_cutoff=HARDCODED_SERVER_START_TIME,
            now_cutoff=datetime.utcnow(),
            survey_id=survey_id
        )
        self._query(sql)
        try:
            end, = self._db_cur.fetchone()
        except TypeError:
            end = None
        return end

    def fetch_coordinates(self, survey_id):
        sql = '''SELECT mobile_users.uuid, mobile_coordinates.latitude, mobile_coordinates.longitude,
                        mobile_coordinates.altitude, mobile_coordinates.speed, mobile_coordinates.direction,
                        mobile_coordinates.h_accuracy, mobile_coordinates.v_accuracy, mobile_coordinates.acceleration_x,
                        mobile_coordinates.acceleration_y, mobile_coordinates.acceleration_z, mobile_coordinates.mode_detected,
                        mobile_coordinates.point_type, mobile_coordinates.timestamp AS "timestamp_UTC",
                        DATE_PART('epoch', mobile_coordinates.timestamp)::integer AS timestamp_epoch
                 FROM mobile_coordinates
                 JOIN mobile_users ON (mobile_coordinates.mobile_id=mobile_users.id)
                 WHERE mobile_coordinates.survey_id={};'''.format(survey_id)
        self._query(sql)
        return self._db_cur.fetchall()

    def fetch_cancelled_prompt_responses(self, survey_id):
        sql = '''SELECT mobile_users.uuid, mobile_cancelled_prompt_responses.prompt_uuid,
                        mobile_cancelled_prompt_responses.latitude, mobile_cancelled_prompt_responses.longitude,
                        mobile_cancelled_prompt_responses.displayed_at AS "displayed_at_UTC",
                        DATE_PART('epoch', mobile_cancelled_prompt_responses.displayed_at)::integer AS displayed_at_epoch,
                        mobile_cancelled_prompt_responses.cancelled_at AS "cancelled_at_UTC",
                        DATE_PART('epoch', mobile_cancelled_prompt_responses.cancelled_at)::integer AS cancelled_at_epoch,
                        mobile_cancelled_prompt_responses.is_travelling
                 FROM mobile_cancelled_prompt_responses
                 JOIN mobile_users ON (mobile_cancelled_prompt_responses.mobile_id=mobile_users.id)
                 WHERE mobile_cancelled_prompt_responses.survey_id={}
                 ORDER BY mobile_cancelled_prompt_responses.id;'''.format(survey_id)
        self._query(sql)
        return self._db_cur.fetchall()

    def fetch_survey_questions(self, survey_id):
        sql = '''
            SELECT *
            FROM survey_questions
            WHERE survey_id={survey_id}
            ORDER BY question_num;
        '''.format(
            survey_id=survey_id
        )
        self._query(sql)
        return self._db_cur.fetchall()

    def fetch_survey_responses(self, survey_id):
        sql = '''SELECT *
                 FROM mobile_survey_responses
                 JOIN mobile_users ON mobile_survey_responses.mobile_id=mobile_users.id
                 WHERE mobile_users.survey_id={}
                 ORDER BY mobile_users.created_at;'''.format(survey_id)
        self._query(sql)
        return self._db_cur.fetchall()

    def fetch_prompt_responses(self, survey_id):
        sql = '''SELECT mobile_users.uuid, mobile_prompt_responses.prompt_uuid, mobile_prompt_responses.response,
                        mobile_prompt_responses.latitude, mobile_prompt_responses.longitude, 
                        mobile_prompt_responses.displayed_at AS "displayed_at_UTC",
                        DATE_PART('epoch', mobile_prompt_responses.displayed_at)::integer AS displayed_at_epoch,
                        mobile_prompt_responses.recorded_at AS "recorded_at_UTC",
                        DATE_PART('epoch', mobile_prompt_responses.recorded_at)::integer AS recorded_at_epoch,
                        mobile_prompt_responses.edited_at AS "edited_at_UTC",
                        DATE_PART('epoch', mobile_prompt_responses.edited_at)::integer AS edited_at_epoch
                 FROM mobile_prompt_responses
                 JOIN mobile_users ON (mobile_prompt_responses.mobile_id=mobile_users.id)
                 WHERE mobile_prompt_responses.survey_id={}
                 ORDER BY mobile_prompt_responses.displayed_at, mobile_prompt_responses.prompt_uuid, mobile_prompt_responses.prompt_num;'''.format(survey_id)
        self._query(sql)
        return self._db_cur.fetchall()

    def latest_signups_by_survey(self):
        sql = '''
            SELECT mobile_users.survey_id AS survey_id,
                   surveys.name AS name,
                   MAX(mobile_users.created_at) AS last_created_at
            FROM mobile_users
            JOIN surveys ON mobile_users.survey_id = surveys.id
            GROUP BY mobile_users.survey_id, surveys.name
            ORDER BY last_created_at ASC; 
        '''
        self._query(sql)
        return self._db_cur.fetchall()

    def select_all(self, table_name, survey_id, json_cols=None, float_cols=None):
        sql = '''
            SELECT *
            FROM {table}
            WHERE survey_id = {id};
        '''.format(
            table=table_name,
            id=survey_id
        )
        self._query(sql)
        for row in self._db_cur.fetchall():
            if json_cols:
                for col in json_cols:
                    row[col] = json.dumps(row[col])
            if float_cols:
                for col in float_cols:
                    if row[col] is not None:
                        row[col] = float(row[col])
            yield row

    def start_time(self, survey_id):
        sql = '''
            SELECT timestamp
            FROM mobile_coordinates
            WHERE survey_id={survey_id}
            AND timestamp >= '{init_cutoff}'
            ORDER BY timestamp ASC
            LIMIT 1;
        '''.format(
            init_cutoff=HARDCODED_SERVER_START_TIME,
            survey_id=survey_id
        )
        self._query(sql)
        try:
            start, = self._db_cur.fetchone()
        except TypeError:
            start = None
        return start


class ExportsDatabase(object):

    def __init__(self, filepath):
        self._db_conn = sqlite3.connect(filepath)
        self._db_cur = self._db_conn.cursor()

    def __del__(self):
        self._db_conn.close()

    def _query(self, query, params=None):
        if not params:
            params = []
        return self._db_cur.execute(query, params)

    def create_active_table(self):
        sql = '''
            CREATE TABLE IF NOT EXISTS active (
                survey_name TEXT UNIQUE,
                survey_start INTEGER,
                survey_last_update INTEGER
            );
        '''
        self._query(sql)
        self._db_conn.commit()

    def create_exports_table(self):
        sql = '''
            CREATE TABLE IF NOT EXISTS exports (
                timestamp INTEGER,
                survey_name TEXT UNIQUE,
                survey_id INTEGER id,
                survey_start INTEGER,
                survey_end INTEGER,
                count_mobile_users INTEGER,
                count_mobile_survey_responses INTEGER,
                count_mobile_coordinates INTEGER,
                count_mobile_prompt_responses INTEGER,
                count_mobile_cancelled_prompt_responses INTEGER
            );
        '''
        self._query(sql)
        self._db_conn.commit()

    def fetch_active_statuses(self):
        sql = '''
            SELECT survey_name, survey_start, survey_last_update
            FROM active;
        '''
        self._query(sql)
        return self._db_cur.fetchall()

    def fetch_archived_statuses(self):
        sql = '''
            SELECT timestamp, survey_name, survey_id, survey_start, survey_end
            FROM exports;
        '''
        self._query(sql)
        return self._db_cur.fetchall()

    def upsert(self, table, cols, record):
        sql = '''REPLACE INTO {table} ({cols}) VALUES ({vals});'''.format(
            table=table,
            cols=', '.join(cols),
            vals=', '.join(['?'] * len(cols))
        )
        self._query(sql, record)
        self._db_conn.commit()

    def upsert_many(self, table, cols, records):
        sql = '''REPLACE INTO {table} ({cols}) VALUES ({vals});'''.format(
            table=table,
            cols=', '.join(cols),
            vals=', '.join(['?'] * len(cols))
        )
        self._db_cur.executemany(sql, records)
        self._db_conn.commit()
