# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2014"
__license__ = "MPL-2.0"

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
