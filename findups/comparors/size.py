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
        self.db_conn = db_conn
        self._curs = self.db_conn.cursor()

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
        """Get all the directories and files that appear to be duplicated only considering their size and that of their
        children (in the case of directories)."""
        # get the sizes that appear on more than one directory
        sql_query = 'SELECT size, COUNT(*) c FROM dir_entry ' + \
                    'WHERE type = "dir" AND size > 0 GROUP BY size HAVING c > 1 ' + \
                    'ORDER BY size DESC;'
        self._curs.execute(sql_query)
        dup_sizes = [size[0] for size in self._curs.fetchall()]
        grouped_duplicates = []
        # look the possible duplicates for each size
        for size in dup_sizes:
            logging.debug("Looking for possible duplicates with size %d" % size)
            # get all the directories with the same size
            sql_query = 'SELECT path FROM dir_entry ' + \
                        'WHERE type = "dir" AND size = :size ORDER BY path ASC;'
            self._curs.execute(sql_query, {'size': size})
            tmp_dirs = [dir[0] for dir in self._curs.fetchall()]
            # don't consider the parent if single child (they have the same size)
            dirs = []
            directory = tmp_dirs.pop(0)
            while tmp_dirs:
                next_dir = tmp_dirs.pop(0)
                if directory not in next_dir:
                    dirs.append(directory)
                directory = next_dir
            dirs.append(next_dir)
            # if no more potential duplicates (all dirs with same child are single children of the same dir),
            # go to next size
            if len(dirs) < 2:
                continue
            logging.debug("Directories that possibly are duplicates: %s" % str(dirs))
            # get sizes of all children for each remaining directory
            trees = {}
            for directory in dirs:
                trees[directory] = self._get_children_info('size', directory)
            grouped_duplicates.extend(self._get_duplicates(trees))
        return grouped_duplicates
