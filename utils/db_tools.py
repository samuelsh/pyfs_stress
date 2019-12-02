import hashlib
import random
import sqlite3
import string
import time

from logger.server_logger import ConsoleLogger

__author__ = 'samuels'

TEST_SUITE_DB = "test.db"
TABLE_NAME = "test_data"

logger = ConsoleLogger(__name__).logger


class Record:
    def __init__(self, timestamp=None, summary=None, data=None):
        self._timestamp = timestamp or time.time()
        self._summary = summary or "DUMMY"
        self._data = data or random.choice(string.ascii_letters + string.digits).encode() * 16
        self._checksum = hashlib.md5(str(self._timestamp).encode() + self._summary.encode() + self._data).hexdigest()
        self._record = dict(timestamp=self._timestamp, summary=self._summary, data=self._data, checksum=self._checksum)

    @property
    def record_vals(self):
        return list(self._record.values())

    @property
    def record_args(self):
        return ",".join(["?"] * len(self.record_vals))


class DataBase:
    def __init__(self, db_path):
        self._db_connection = sqlite3.connect(db_path)
        self._cursor = self._db_connection.cursor()
        self._name = TEST_SUITE_DB

        # self._cursor.execute("DROP TABLE IF EXISTS %s" % TABLE_NAME)

        self._cursor.execute('''CREATE TABLE IF NOT EXISTS %s
             (
                timestamp           INTEGER,
                summary             TEXT,
                data                BLOB,
                checksum            TEXT
                )''' % TABLE_NAME)
        self._db_connection.commit()
        logger.info(f"DataBase {self._db_connection} created. Path={db_path}")

    @property
    def name(self):
        return self._name

    def insert_record(self, record):
        qry = f'insert into {TABLE_NAME} values ({record.record_args})'
        try:
            with self._db_connection:
                self._db_connection.execute(qry, record.record_vals)
        except sqlite3.DatabaseError as err:
            logger.exception(f'table={TABLE_NAME}, query={qry}, values={record.record_vals}\n Error: {err}')
            raise err

    def fetch_columns(self, columns=None):
        qry = f'select {"*" if not columns else ",".join(columns)} from {TABLE_NAME}'
        logger.info(f"SQL Query={qry}")
        try:
            return self._cursor.execute(qry)
        except sqlite3.DatabaseError as err:
            logger.exception(f'table={TABLE_NAME}, query={qry}\n Error: {err}')
            raise err

    def close(self):
        logger.info("Closing DB connection...")
        self._db_connection.close()
