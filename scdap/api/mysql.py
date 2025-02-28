"""

@create on: 2020.12.31
"""
from contextlib import contextmanager

from typing import ContextManager

from pymysql import Connection
from pymysql.cursors import Cursor

from scdap import config


@contextmanager
def get_cursor(database: str, cursor_class=None, connect_conf: dict = None) -> ContextManager[Cursor]:
    connect_conf = (connect_conf or dict()).copy()
    connect_conf.setdefault('host', config.MYSQL_HOST)
    connect_conf.setdefault('port', config.MYSQL_PORT)
    connect_conf.setdefault('user', config.MYSQL_USER)
    connect_conf.setdefault('password', config.MYSQL_PASSWORD)
    connect_conf.setdefault('charset', config.MYSQL_CHARSET)
    connect_conf.setdefault('database', database)
    connection = Connection(**connect_conf)
    try:
        cursor = connection.cursor(cursor_class)
        try:
            yield cursor
            connection.commit()
        except:
            connection.rollback()
            raise
        finally:
            cursor.close()
    finally:
        connection.close()
