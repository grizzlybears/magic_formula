## -*- coding: utf-8 -*-

# py系统包
import sys
import site
import traceback
from datetime import date,datetime,timedelta
import codecs
import csv
import collections

# 著名扩展包
import  pandas as pd
import  numpy

# 其他第三方包
import  jqdatasdk as jq

# 我们的代码
import data_struct 
import db_operator
import data_fetcher
import util


# 输入,历史行情数组 his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     ...
# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’数组:  []      
def add_blank_indices( conn,  his_md):

    for md_that_day  in his_md:
        
        indices = collections.OrderedDict ()

        for code,md_set in md_that_day[1].iteritems():

            indices[code] = []

        md_that_day.append( indices)

    return his_md

# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# 返回时，各‘指标’数组的末尾加上 ‘可买标志’       
def extend_indices_add_buyable( conn,  his_md):

    for md_that_day  in his_md:
        
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
            
            if md_of_the_code[3] or md_of_the_code[4] :
                # 涨停或者停牌的不能买
                can_buy = 0
            else:
                can_buy = 1

            indi_of_the_code.append(can_buy) 

    return his_md

# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# 返回时，各‘指标’数组的末尾加上 ‘N日累计涨幅’ 
def extend_indices_add_delta( conn,  his_md, howlong):
    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价, 前日收盘价], 
                   #                    [交易日，收盘价, 前日收盘价], ... 
                   #                ]
    md_prev_day = None

    for md_that_day  in his_md:
        
        t_day   =  md_that_day[0]
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
            
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                # 需要从外部获取本code最后N日记录
                recent_memo = data_fetcher.get_his_until( code, t_day, howlong)
                recent_mds[code] = recent_memo 
                #print recent_mds['601398.XSHG']
            else:
                # 停牌的行情不加入 recent_mds
                if not  md_of_the_code[4] : 
                    recent_mds[code].append( [t_day, md_of_the_code[0], md_of_the_code[1] ] )
 
            if len(recent_mds[code]) > howlong:
                del recent_mds[code][0]

            delta  = util.sum_delta_r( recent_mds[code])
            # 至此累计涨幅有了

            indi_of_the_code.append(delta) 
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md



# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# 返回时，各‘指标’数组的末尾加上 ‘MA’       
def extend_indices_add_ma( conn,  his_md, MA_Size1 =5):

    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价], 
                   #                    [交易日，收盘价], ... 
                   #                ]
    md_prev_day = None

    for md_that_day  in his_md:
        
        t_day   =  md_that_day[0]
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
            
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                # 需要从外部获取本code最后N日记录
                recent_memo = data_fetcher.get_his_until( code, t_day, MA_Size1)
                recent_mds[code] = recent_memo 
            else:
                # 停牌的行情不加入 recent_mds
                if not  md_of_the_code[4] : 
                    recent_mds[code].append( [t_day, md_of_the_code[0]  ]  )
 
            if len(recent_mds[code]) > MA_Size1:
                del recent_mds[code][0]

            MA1 = util.avg( recent_mds[code])
            # 至此MA有了

            indi_of_the_code.append(MA1) 
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md


# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志, PB, 换手率]
# 返回时，各‘指标’数组的末尾加上 [PB偏离度, PB, MA of PB, standard deviation of PB ]      
# PB偏离度 = ( PB - MA_of_PB ) / STD_of_PB
def extend_indices_add_pb_std( conn,  his_md, MA_Size1 = 5):

    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，PB], 
                   #                    [交易日，PB], ... 
                   #                ]
    md_prev_day = None

    for md_that_day  in his_md:
        
        t_day   =  md_that_day[0]
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            PB  = md_of_the_code[5]
            indi_of_the_code = indices[code]
            
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                recent_memo =   [
                                    [ t_day , PB ]
                                ]
                recent_mds[code] = recent_memo 
            else:
                # 停牌的行情不加入 recent_mds
                if not  md_of_the_code[4] : 
                    recent_mds[code].append( [t_day, PB  ]  )
 
            if len(recent_mds[code]) > MA_Size1:
                del recent_mds[code][0]

            #对于PB策略，样本不足则 平均值和标准差都意义不足，所以
            if len(recent_mds[code]) < MA_Size1:
                MA1 = 0
                std = 0
                bias = 0
            else:
                #print recent_mds[code]
                MA1 = util.avg( recent_mds[code])
                std = numpy.std( util.column_of_a2d( recent_mds[code],1))
                bias = (PB - MA1) / std 

            indi_of_the_code.append(bias) 
            indi_of_the_code.append(PB) 
            indi_of_the_code.append(MA1) 
            indi_of_the_code.append(std) 
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md

