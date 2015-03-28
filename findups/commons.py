# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2015"
__license__ = "MPL-2.0"

# Python standard packages
import os
import os.path
import sqlite3
import logging

_SIZE = 1
SAME_SIZE = _SIZE  # 1


class FindupsCommons(object):
    def __init__(self, db_path='', accuracy=SAME_SIZE,
                 log_level=logging.WARNING,
                 log_file=os.path.join(os.getenv('HOME'), '.findups.log')):
        """
        Establish the connection with the DB, creating it, if not existing.
        """
        logging.basicConfig(filename=log_file, level=log_level)
        logging.basicConfig(level=log_level)
        logging.warning("-------------------")

        if not db_path:
            raise RuntimeError('Path missing')

        self._db_location = os.path.expanduser(db_path)
        logging.debug("DB path: %s" % self._db_location)
        new_db = not os.path.isfile(db_path)

        self._dbconn = sqlite3.connect(self._db_location)
        self._curs = self._dbconn.cursor()
        if new_db:
            self._db_schema()
        self._accuracy = accuracy

    def _db_schema(self):
        """
        Create the DB schema.
        """
        tables = [
            '''CREATE TABLE device (
                    id TEXT PRIMARY KEY
            );''',
            '''CREATE TABLE tree (
                    id INTEGER PRIMARY KEY,
                    device TEXT REFERENCES device(id),
                    root_dir TEXT NOT NULL
            );''',
            '''CREATE TABLE size (
                    value INTEGER PRIMARY KEY
            );''',
            '''CREATE TABLE mtime (
                    time TEXT PRIMARY KEY
            );''',
            '''CREATE TABLE dir_entry (
                    tree TEXT REFERENCES tree(id),
                    path TEXT NOT NULL,
                    type TEXT CHECK(type = "dir" OR type = "file"),
                    size INTEGER NOT NULL REFERENCES size(value),
                    mtime INTEGER NOT NULL REFERENCES mtime(time),
                    PRIMARY KEY (tree, path)
            );'''
        ]
        views = [
        ]

        for table_creation in tables:
            self._curs.execute(table_creation)
        for view_creation in views:
            self._curs.execute(view_creation)
        self._dbconn.commit()
        logging.info("Database initialized in %s" % self._db_location)

    def __del__(self):
        """
        Ensure that the connection with the DB is properly closed.
        """
        self._dbconn.commit()
        self._curs.close()
        self._dbconn.close()
