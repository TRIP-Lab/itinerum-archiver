#!/usr/bin/env python3
# Kyle Fitzsimmons, 2018
import csv
import gzip
import os
import shutil
import sqlite3
import tarfile

from sh import pg_dump


def write_csv(fp, header, rows):
    with open(fp, 'w') as csv_f:
        writer = csv.writer(csv_f)
        writer.writerow(header)
        writer.writerows(rows)

    # write legacy-version encoded as Latin-1 so accents
    # display correctly on open in Excel
    if 'coordinates' not in fp and 'surveys-latest_users' not in fp:
        parts = fp.rsplit('.', 1)
        legacy_fp = parts[0] + '_latin1.csv'
        with open(legacy_fp, 'w', encoding='latin-1', errors='ignore') as csv_f:
            writer = csv.writer(csv_f)
            writer.writerow(header)
            writer.writerows(rows)


def create_archive(fp_or_dir):
    if os.path.isfile(fp_or_dir):
        fp = fp_or_dir
        archive_fp = fp + '.gz'
        with open(fp, 'rb') as f:
            with gzip.open(archive_fp, 'wb') as archive_f:
                shutil.copyfileobj(f, archive_f)
        os.remove(fp)
    else:
        _dir = fp_or_dir
        archive_fp = _dir + '.tar.gz'
        with tarfile.open(archive_fp, 'w:gz') as tar_f:
            tar_f.add(_dir, arcname=os.path.basename(_dir))
        shutil.rmtree(_dir)


class SQLiteDatabase(object):
    def __init__(self, filepath):
        self._db_conn = sqlite3.connect(filepath)
        self._db_cur = self._db_conn.cursor()

    def __del__(self):
        self._db_conn.close()

    def _query(self, query, params=None):
        if not params:
            params = []
        self._db_cur.execute(query, params)

    def count(self, table_name):
        sql = '''
            SELECT COUNT(*) FROM {table};
        '''.format(
            table=table_name
        )
        self._query(sql)
        count, = self._db_cur.fetchone()
        return count

    def generate_table(self, table_name, columns):
        col_strs = ', '.join(['{} {}'.format(*col) for col in columns])
        sql = '''CREATE TABLE {table} ({cols});'''.format(
            table=table_name,
            cols=col_strs
        )
        self._query(sql)

    def insert_many(self, table_name, columns, rows):
        sql = '''
            INSERT INTO {table} ({cols}) VALUES ({vals});
        '''.format(
            table=table_name,
            cols=', '.join([name for name, _ in columns]),
            vals=', '.join(['?'] * len(columns))
        )

        self._db_cur.executemany(sql, rows)
        self._db_conn.commit()
