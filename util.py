# -*- coding: utf-8 -*-

import pprint
import os

import collections
import json 

def bp( var ):

    if type(var) is collections.OrderedDict:
        print(json.dumps(var, indent=2))
        return

    rows, columns = os.popen('stty size', 'r').read().split()
    pp = pprint.PrettyPrinter(indent = 2, width = columns  )
    pp.pprint (var)


def bp_as_json(var):
    print(json.dumps(var, indent=2))


