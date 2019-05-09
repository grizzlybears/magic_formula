# -*- coding: utf-8 -*-

import pprint
import os
import re
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


def uni_2_float(a):
    if a is None:
        return None

    return float(str(a))

# 返回三元组 送，转，派    (每10股)
def parse_xrxd_note(note):
    
    #print note 

    if note is None:
        return None

    no_space =""
    for c in note:
        if c != ' ':
            no_space = no_space + c
    
    # 10[送x股][转增y股][派z元(含税)]
    pattern = re.compile(u"^10股?(送([0-9\.]+)股?)?(转[增|赠|増]?([0-9\.]+)股?)?(派([0-9\.]+)[港|美]?元?)?")
    
    matched = 0
    m = pattern.match( unicode(no_space))
    if m:
        s = u""
        s = s + m.group(0)
        for g in m.groups():
            if g:
                s = s + u", " + g
            else:
                s = s + u", None"
        
        if len(m.groups()) == 6 and \
            (m.group(1) is not None or m.group(3) is not None or m.group(5) is not None  ):
            matched = 1


            return ( uni_2_float(m.group(2)), uni_2_float(m.group(4)), uni_2_float(m.group(6))  )
            # print s

    if not matched:
        #print "无法解析分红文本!"
        return None 
        
def nullable_float(f):
    if f:
        return "%f" % float(f)
    else:
        return ''

def nullable_float2(f):
    if f:
        return "%.2f" % float(f)
    else:
        return ''


def null_or_0(f):
    if f is None:
        return 0
    else:
        return f

