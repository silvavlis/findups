# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

import logging


class Comparor:
    def __init__(self, db_conn, logger):
        pass

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
        sql_query = "SELECT type, %s FROM dir_entry " % info_item + \
                    "WHERE path LIKE :parent ORDER BY path ASC;"
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