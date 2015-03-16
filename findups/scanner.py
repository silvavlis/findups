# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

# Python standard packages
import os
import os.path
import sqlite3
import logging
import stat
# findups packages
import comparors.file as file_cmp
import comparors.mtime as mtime_cmp
import comparors.size as size_cmp

_SIZE = 1
SAME_SIZE = _SIZE  # 1
_FILE = 8
SAME_FILE = _FILE | SAME_SIZE  # 15


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
            '''CREATE TABLE size (
                    value INTEGER PRIMARY KEY
            );''',
            '''CREATE TABLE mtime (
                    time TEXT PRIMARY KEY
            );''',
            '''CREATE TABLE file (
                    path TEXT PRIMARY KEY,
                    size INTEGER,
                    mtime INTEGER,
                    FOREIGN KEY(size) REFERENCES size(value),
                    FOREIGN KEY(mtime) REFERENCES mtime(time)
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


class DirScanner(FindupsCommons):
    def __init__(self, db_path='', accuracy=SAME_SIZE,
                 log_level=logging.WARNING,
                 log_file=os.path.join(os.getenv('HOME'), '.findups.log')):
        """
        """
        super(DirScanner, self).__init__(db_path=db_path, accuracy=accuracy, log_level=log_level, log_file=log_file)
        self._file_cmp = file_cmp.File(self._dbconn)
        self._mtime_cmp = mtime_cmp.Mtime(self._dbconn)
        self._size_cmp = size_cmp.Size(self._dbconn)

    def scan(self, dir_path):
        """
        """
        logging.info("Scanning directory (accuracy=%s) in %s" % (self._accuracy, dir_path))
        n_files = 0
        total_size = 0
        # get the size and modification time of the files in the directory
        for root, subdirs, files in os.walk(dir_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                if file_path == self._db_location:
                    continue
                if stat.S_ISSOCK(os.stat(file_path).st_mode):
                    continue
                file_size = os.path.getsize(file_path)
                file_mtime = os.path.getmtime(file_path)
                self._size_cmp.add(file_size)
                self._mtime_cmp.add(file_mtime)
                self._file_cmp.add(file_path, file_size, file_mtime)
                n_files += 1
                total_size += file_size
        return n_files, total_size
