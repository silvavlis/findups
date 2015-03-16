# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

# Python standard packages
import logging
import sqlite3
# findups packages
import comparors.comparor


class File(comparors.comparor.Comparor):
    def __init__(self, db_conn):
        """
        """
        self._db_conn = db_conn
        self._curs = self._db_conn.cursor()

    def add(self, file_path, size, mtime):
        try:
            self._curs.execute('INSERT INTO file(path, size, mtime) '
                               'VALUES (:path, :size, :mtime);', {'path': file_path, 'size': size, 'mtime': mtime})
        except sqlite3.ProgrammingError as e:
            if str(e).startswith('You must not use 8-bit bytestrings'):
                print(str(e) + ' -> ' + file_path)
            else:
                raise
        logging.info("New file: %s" % file_path)
        self._db_conn.commit()
