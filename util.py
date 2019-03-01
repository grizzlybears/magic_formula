# -*- coding: utf-8 -*-

import pprint
import os

import collections
import json 

import  pandas as pd

def print_df_all(df):
    with pd.option_context('display.max_rows', None
            , 'display.max_columns', None
            ):
        print(df)

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

    if len(to_sell) + len(to_buy) == 0:
        return None

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

# 这样的数组 [ 
#                    [交易日，收盘价], 
#                    [交易日，收盘价], ... 
#                ]
# 求 avg(收盘价)
def avg( his  ):

    if 0 == len(his):
        return 0

    s = 0
    for entry in his:
        #print entry
        s = s  +  entry[1]

    return s / len(his)

# 这样的数组 [ 
#                    [交易日，收盘价, 前日收盘价], 
#                    [交易日，收盘价, 前日收盘价], ... 
#            ]
# 求总变动
def sum_delta( his  ):

    if 0 == len(his):
        return 0

    s = his[len(his) - 1][1] - his[0][2]

    return s 

# 这样的数组 [ 
#                    [交易日，收盘价, 前日收盘价], 
#                    [交易日，收盘价, 前日收盘价], ... 
#            ]
# 求总涨幅
def sum_delta_r( his  ):

    if 0 == len(his):
        return 0

    s = (his[len(his) - 1][1] - his[0][2]) / his[0][2]

    return s 

# 取2D数组'a2d'中的第'i'列 ('i' start from 0)
def column_of_a2d(a2d, i ):
    return [row[i] for row in a2d]


