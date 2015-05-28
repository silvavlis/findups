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
import findups.comparors.comparor
import findups.comparors.mtime as mtime_cmp
import findups.comparors.size as size_cmp


class DirEntry(findups.comparors.comparor.Comparor):
    def __init__(self, db_conn):
        """
        """
        self._db_conn = db_conn
        self._curs = self._db_conn.cursor()
        self._mtime_cmp = mtime_cmp.Mtime(self._db_conn)
        self._size_cmp = size_cmp.Size(self._db_conn)
        self._tree_id = None

    def set_tree(self, tree_id):
        self._tree_id = tree_id

    def change_parent(self, subdir):
        sql_query = 'SELECT root_dir FROM tree WHERE id = :tree_id;'
        self._curs.execute(sql_query, {'tree_id': self._tree_id})
        parent = self._curs.fetchone()[0]
        sql_query = 'SELECT id FROM tree WHERE root_dir = :subdir;'
        self._curs.execute(sql_query, {'subdir': subdir})
        subdir_id = self._curs.fetchone()[0]
        rel_path = subdir[len(parent)+1:]
        sql_query = 'SELECT size FROM dir_entry WHERE tree = :subdir AND path = "";'
        self._curs.execute(sql_query, {'subdir': subdir_id})
        subtree_size = self._curs.fetchone()[0]
        sql_query = 'UPDATE dir_entry SET tree = :parent , path = :prefix WHERE tree = :subdir AND path == "";'
        self._curs.execute(sql_query, {'parent': self._tree_id, 'prefix': rel_path, 'subdir': subdir_id})
        sql_query = 'UPDATE dir_entry SET tree = :parent , path = :prefix || path WHERE tree = :subdir AND path != "";'
        self._curs.execute(sql_query, {'parent': self._tree_id, 'prefix': rel_path + os.path.sep, 'subdir': subdir_id})
        dirs = os.path.dirname(rel_path)
        while dirs:
            sql_query = 'UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = :dirs;'
            self._curs.execute(sql_query, {'size': subtree_size, 'tree': self._tree_id, 'dirs': dirs})
            dirs = os.path.dirname(dirs)
        sql_query = 'UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = "";'
        self._curs.execute(sql_query, {'size': subtree_size, 'tree': self._tree_id})
        sql_query = 'DELETE FROM tree WHERE root_dir = :subdir;'
        self._curs.execute(sql_query, {'subdir': subdir})

    def add(self, path, type, size, mtime):
        self._size_cmp.add(size)
        self._mtime_cmp.add(mtime)
        path = path.encode("utf-8", "surrogateescape")
        if not path:
            path = u""
        if type == "dir":
            logging.info("New directory: %s" % path)
        else:
            logging.info("New file: %s" % path)
        try:
            self._curs.execute('INSERT INTO dir_entry(tree, path, type, size, mtime) '
                               'VALUES (:tree, :path, :type, :size, :mtime);',
                               {'tree': self._tree_id, 'path': path, 'type': type, 'size': size, 'mtime': mtime})
        except sqlite3.ProgrammingError as e:
            if str(e).startswith('You must not use 8-bit bytestrings'):
                print(str(e) + ' -> ' + path)
            else:
                raise
        if type == "file":
            dirs = os.path.dirname(path)
            while dirs:
                self._curs.execute('UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = :dirs;',
                    {'size': size, 'tree': self._tree_id, 'dirs': dirs})
                dirs = os.path.dirname(dirs)
            self._curs.execute('UPDATE dir_entry SET size = size + :size WHERE tree = :tree AND path = "";',
                {'size': size, 'tree': self._tree_id})
        self._db_conn.commit()
        return self._curs.lastrowid

    def duplicates(self):
        return self._size_cmp.duplicates()