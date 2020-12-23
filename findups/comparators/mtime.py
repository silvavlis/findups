# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

# Python standard packages
import sqlite3
import logging
# findups packages
import findups.comparators.comparator


class Mtime(findups.comparators.comparator.Comparator):
    def __init__(self, db_conn):
        """
        """
        self.db_conn = db_conn
        self._curs = self.db_conn.cursor()

    def add(self, mtime):
        # add the mtime to the DB
        try:
            self._curs.execute('INSERT INTO mtime(time) VALUES (:mtime);', {'mtime': mtime})
            logging.debug("New mtime: %d" % mtime)
        except sqlite3.IntegrityError as e: 
            if str(e) != "UNIQUE constraint failed: mtime.time":
                raise
            logging.debug("Mtime %d is not new" % mtime)
