# -*- coding: utf-8 -*-

import os
import re
import sqlite3

WORKING_DIR = "working_dir"
DB_PATH     = "%s/mf.db" % WORKING_DIR


def is_yyyy_mm_dd (s):

    return  re.match( '^\d{4}-\d{2}-\d{2}$' , s )

