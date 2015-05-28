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
import findups.comparors.comparor


class Size(findups.comparors.comparor.Comparor):
    def __init__(self, db_conn):
        """
        """
        super(self.__class__, self).__init__(db_conn, None)

    def add(self, size):
        # add the size to the DB
        try:
            self._curs.execute('INSERT INTO size(value) VALUES (:size);', {'size': size})
            logging.debug("New size: %d" % size)
        except sqlite3.IntegrityError as e: 
            if str(e) != "UNIQUE constraint failed: size.value":
                raise
            logging.debug("Size %d is not new" % size)

    def duplicates(self):
        return super(self.__class__, self)._duplicates('size')
