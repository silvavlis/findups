# -*- mode: python -*-
# -*- coding: utf-8 -*-

__author__ = "Silvano Cirujano Cuesta"
__email__ = "silvanociru@gmx.net"
__copyright__ = "Copyright (C) Silvano Cirujano Cuesta 2015"
__license__ = "MPL-2.0"

# Python standard packages
import os
import os.path
import sqlite3
import logging
import stat
# findups packages
import commons
import comparors.dir_entry as dir_entry_cmp
import comparors.mtime as mtime_cmp
import comparors.size as size_cmp

_SIZE = 1
SAME_SIZE = _SIZE  # 1
_FILE = 8
SAME_FILE = _FILE | SAME_SIZE  # 15


class DirScanner(commons.FindupsCommons):
    def __init__(self, device_id, db_path='', accuracy=SAME_SIZE,
                 log_level=logging.WARNING,
                 log_file=os.path.join(os.getenv('HOME'), '.findups.log')):
        """
        """
        super(DirScanner, self).__init__(db_path=db_path, accuracy=accuracy, log_level=log_level, log_file=log_file)
        self._device_id = device_id
        try:
            self._curs.execute('INSERT INTO device(id) VALUES (:id);', {'id': device_id})
            logging.debug("New device: %s" % device_id)
        except sqlite3.IntegrityError as e:
            if str(e) != "UNIQUE constraint failed: device.id":
                raise
            logging.debug("Device %s is not new" % device_id)
        self._dir_entry_cmp = dir_entry_cmp.DirEntry(self._dbconn)
        self._mtime_cmp = mtime_cmp.Mtime(self._dbconn)
        self._size_cmp = size_cmp.Size(self._dbconn)

    def scan(self, root_scan):
        """
        """
        root_scan = root_scan.rstrip("/")
        try:
            self._curs.execute('INSERT INTO tree(device, root_dir) VALUES (:device, :root);',
                               {'device': self._device_id, 'root': root_scan})
            tree_id = self._curs.lastrowid
            logging.debug("New tree: %s:%s" % (self._device_id, root_scan))
        except sqlite3.IntegrityError as e:
            if str(e) != "UNIQUE constraint failed: device.id":
                raise
            logging.debug("Device %s:%s is not new" % (self._device_id, root_scan))
        self._dir_entry_cmp.set_tree(tree_id)
        logging.info("Scanning directory (accuracy=%s) in %s" % (self._accuracy, root_scan))
        n_files = 0
        # get the size and modification time of the files in the directory
        for root, _, files in os.walk(root_scan):
            rel_root = root[len(root_scan)+1:]
            self._dir_entry_cmp.add(rel_root, type="dir", size=0, mtime=0)
            dir_size = 0
            for filename in files:
                file_path = os.path.join(root, filename)
                if file_path == self._db_location:
                    continue
                if stat.S_ISSOCK(os.stat(file_path).st_mode):
                    continue
                file_size = os.path.getsize(file_path)
                file_mtime = os.path.getmtime(file_path)
                self._size_cmp.add(file_size)
                self._mtime_cmp.add(file_mtime)
                rel_file_path = os.path.join(rel_root, filename)
                self._dir_entry_cmp.add(rel_file_path, type="file", size=file_size, mtime=file_mtime)
                n_files += 1
        self._curs.execute('SELECT size FROM dir_entry WHERE tree = :tree AND path = "";', {'tree': tree_id})
        total_size = self._curs.fetchone()[0]
        return n_files, total_size
