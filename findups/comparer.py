# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2015"
__license__ = "MPL-2.0"

# Python standard packages
import os
import os.path
import logging
# findups packages
import findups.commons
import findups.comparors.dir_entry as dir_entry_cmp

_SIZE = 1
SAME_SIZE = _SIZE  # 1
_FILE = 8
SAME_FILE = _FILE | SAME_SIZE  # 15


class DirComparer(findups.commons.FindupsCommons):
    def __init__(self, db_path='', accuracy=SAME_SIZE,
                 log_level=logging.WARNING,
                 log_file=os.path.join(os.getenv('HOME'), '.findups.log')):
        """
        """
        super(DirComparer, self).__init__(db_path=db_path, accuracy=accuracy, log_level=log_level, log_file=log_file)
        self._dir_entry_cmp = dir_entry_cmp.DirEntry(self._db_conn)

    def compare(self):
        """
        """
        print("-----------------\nLooking for duplicates (accuracy=%s)" % self._accuracy)
        self._dir_entry_cmp.duplicates()