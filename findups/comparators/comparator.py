# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

import logging


class Comparator:
    def __init__(self, db_conn, logger):
        self.db_conn = db_conn
        self._curs = self.db_conn.cursor()

    def has(self, file_id):
        '''
        returns True if the value for the file exists already in the DB, False elsewise
        '''
        return True

    def add(self, file_path):
        '''
        adds new value to DB
        '''
        pass

    def _get_children_info(self, info_item, dir):
        sql_query = "SELECT type, {info_item} FROM dir_entry " + \
                    "WHERE path LIKE :parent ORDER BY path ASC;"
        sql_query = sql_query.format(info_item=info_item)
        self._curs.execute(sql_query, {'parent': dir + b'%'})
        return (self._curs.fetchall())[1:]

    def _get_duplicates(self, trees):
        grouped_duplicates = []
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

    def _duplicates(self, value_type):
        """Get all the directories and files that appear to be duplicated only considering 'value_type' and that of
        their children (in the case of directories)."""
        # get the 'values' that appear on more than one directory
        sql_query = 'SELECT {value_type}, COUNT(*) c FROM dir_entry ' + \
                    'WHERE type = "dir" AND {value_type} > 0 GROUP BY {value_type} HAVING c > 1 ' + \
                    'ORDER BY {value_type} DESC;'
        sql_query = sql_query.format(value_type=value_type)
        self._curs.execute(sql_query)
        dup_values = [value[0] for value in self._curs.fetchall()]
        grouped_duplicates = []
        # look the possible duplicates for each 'value'
        for value in dup_values:
            logging.debug("Looking for possible duplicates with {value_type} {value}".format(value_type=value_type,
                                                                                             value=value))
            # get all the directories with 'value' ordered to get first parents and then theirs children
            sql_query = 'SELECT path FROM dir_entry WHERE type = "dir" AND {value_type} = :value ' + \
                        'ORDER BY path ASC;'
            sql_query = sql_query.format(value_type=value_type)
            self._curs.execute(sql_query, {'value': value})
            tmp_dirs = [dir[0] for dir in self._curs.fetchall()]
            # if a parent and its child in the list (single child, therefore they have the same 'value'),
            # remove the parent from the list
            dirs = []
            directory = tmp_dirs.pop(0)
            while tmp_dirs:
                next_dir = tmp_dirs.pop(0)
                if directory not in next_dir:
                    dirs.append(directory)
                directory = next_dir
            dirs.append(next_dir)
            # if no more potential duplicates (all dirs with same child are single children of the same dir),
            # go to next 'value'
            if len(dirs) < 2:
                continue
            logging.debug("Directories that possibly are duplicates: {directories!s}".format(directories=dirs))
            # get 'values' of all children for each remaining directory
            trees = {}
            for directory in dirs:
                trees[directory] = self._get_children_info(value_type, directory)
            grouped_duplicates.extend(self._get_duplicates(trees))
            #TODO: address cornercase "I forgot it..."
        return grouped_duplicates
