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


class DirEntry(findups.comparors.comparor.Comparor):
    def __init__(self, db_conn):
        """
        """
        self._db_conn = db_conn
        self._curs = self._db_conn.cursor()

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
            # get the number of items (files and directories) in each tree
            tree_n_items = {dir: len(trees[dir]) for dir in trees.keys()}
            # only possible duplicates if at least two trees with same number of items
            if len(set(tree_n_items.values())) != len(tree_n_items.values()):
                # get repeated number of items
                tree_n_items_values = list(tree_n_items.values())
                dup_n_items = set([x for x in tree_n_items_values if tree_n_items_values.count(x) > 1])
                # if some repeated number of items
                if dup_n_items:
                    # for each repeated number of items
                    for n_items in dup_n_items:
                        # compare all trees with that number of items
                        candidates_dup = {directory: trees[directory] for directory in trees.keys()
                                          if len(trees[directory]) == n_items}
                        directories = list(candidates_dup.keys())
                        while directories and (len(directories) > 1):
                            directory = directories.pop()
                            other_trees = {other_dir: trees[other_dir] for other_dir in trees.keys()
                                          if other_dir != directory}
                            duplicates = []
                            for other_dir in other_trees.keys():
                                if trees[directory] == other_trees[other_dir]:
                                    if not duplicates:
                                        duplicates.append(directory)
                                    duplicates.append(other_dir)
                            logging.info("Duplicates: %s" % duplicates)
                            grouped_duplicates.append(duplicates)
        return grouped_duplicates
