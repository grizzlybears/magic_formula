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

def build_p_hint( t_day,to_hold, to_sell, to_buy):
    s = t_day 

    if len(to_hold) > 0:
        s = s +" H:"
        for i,one in enumerate( to_hold) :
            if 0 == i:
                s = s + str(one)
            else:
                s = s + "," + str(one)

     
    if len(to_sell) > 0:
        s = s +" S:"
        for i,one in enumerate( to_sell) :
            if 0 == i:
                s = s + str(one)
            else:
                s = s + "," + str(one)

    
    if len(to_buy) > 0:
        s = s + " B:"
        for i,one in enumerate( to_buy) :
            if 0 == i:
                s = s + str(one)
            else:
                s = s + "," + str(one)

    return s

def build_t_hint( t_day, to_sell, to_buy):
    s = t_day 
    
    if len(to_sell) > 0:
        s = s +" S:"
        for i,one in enumerate( to_sell) :
            if 0 == i:
                s = s + str(one)
            else:
                s = s + "," + str(one)

    
    if len(to_buy) > 0:
        s = s + " B:"
        for i,one in enumerate( to_buy) :
            if 0 == i:
                s = s + str(one)
            else:
                s = s + "," + str(one)

    return s


