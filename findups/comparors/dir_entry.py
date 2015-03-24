# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2015"
__license__ = "MPL-2.0"

# Python standard packages
import os.path
import logging
import sqlite3
# findups packages
import comparors.comparor


class DirEntry(comparors.comparor.Comparor):
    def __init__(self, db_conn):
        """
        """
        self._db_conn = db_conn
        self._curs = self._db_conn.cursor()

    def set_tree(self, tree_id):
        self._tree_id = tree_id

    def add(self, path, type, size, mtime):
        try:
            self._curs.execute('INSERT INTO dir_entry(tree, path, type, size, mtime) '
                               'VALUES (:tree, :path, :type, :size, :mtime);',
                               {'tree': self._tree_id, 'path': path, 'type': type, 'size': size, 'mtime': mtime})
        except sqlite3.ProgrammingError as e:
            if str(e).startswith('You must not use 8-bit bytestrings'):
                print(str(e) + ' -> ' + name)
            else:
                raise
        if type == "dir":
            logging.info("New directory: %s" % path)
        else:
            dirs = os.path.dirname(path)
            while dirs:
                self._curs.execute('UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = :dirs;',
                    {'size': size, 'tree': self._tree_id, 'dirs': dirs})
                dirs = os.path.dirname(dirs)
            self._curs.execute('UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = "";',
                {'size': size, 'tree': self._tree_id})
            logging.info("New file: %s" % path)
        self._db_conn.commit()
        return self._curs.lastrowid
