# -*- coding: utf-8 -*-

import pprint
import os

def bp( var ):
    rows, columns = os.popen('stty size', 'r').read().split()
    pp = pprint.PrettyPrinter(indent = 2, width = columns  )
    pp.pprint (var)
