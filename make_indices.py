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

    #MA_Size2 = 60
    #MA_Size3 = 20

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
                #MA2=0
                #MA3=0
            else:
                #print recent_mds[code]
                MA1 = util.avg( recent_mds[code])
                std = numpy.std( util.column_of_a2d( recent_mds[code],1))
                bias = (PB - MA1) / std 
                
                #MA2 = util.avg( recent_mds[code][ - MA_Size2 :])
                #MA3 = util.avg( recent_mds[code][ - MA_Size3 :])

            indi_of_the_code.append(bias) 
            indi_of_the_code.append(PB) 
            indi_of_the_code.append(MA1) 
            indi_of_the_code.append(std) 
            #indi_of_the_code.append(MA2) 
            #indi_of_the_code.append(MA3) 
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md


# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# 返回时，各‘指标’数组的末尾加上 ‘RSI’       
def extend_indices_add_rsi( conn,  his_md,  MA_Size1 =5):

    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价，前日收盘价, 涨幅], 
                   #                    [交易日，收盘价，前日收盘价, 涨幅], ... 
                   #                ]
    md_prev_day = None

    code_2_last_sma_up        = {}  # 代码  ==> 该代码前日SMA(上涨)
    code_2_last_sma_amplitude = {}  # 代码  ==> 该代码前日SMA(波动)

    window_size = MA_Size1 * 8    #  *8 是经验参数，这样与通达信一致. 提前窗口太小会导致前几项有误差

    for md_that_day  in his_md:
        
        t_day   =  md_that_day[0]
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
            
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                # 需要从外部获取本code最后N日记录
                recent_memo = data_fetcher.get_his_until( code, t_day, window_size )
                recent_mds[code] = recent_memo 
            else:
                # 停牌的行情不加入 recent_mds
                if not  md_of_the_code[4] : 
                    delta = md_of_the_code[0] - md_of_the_code[1] 
                    recent_mds[code].append( [t_day, md_of_the_code[0], md_of_the_code[1], delta  ] )
 
            if len(recent_mds[code]) > window_size:
                del recent_mds[code][0]

            # 至此N日行情有了，开始计算RSI
            md_window  = recent_mds[code]
            if code not in code_2_last_sma_up:
                # 本code还没算过SMA, 需要先填补过往, 把md_window中的记录先算完
                code_2_last_sma_up[code] = 0
                code_2_last_sma_amplitude[code] = 0

                for i,m in enumerate( md_window):
                    
                    #   [交易日，收盘价，前日收盘价, 涨幅]

                    delta = m[3]
                    if delta >= 0:
                        up        = delta
                        amplitude = delta 
                    else:
                        up = 0
                        amplitude = - delta 

                    sma_up = (up + code_2_last_sma_up[code] * (MA_Size1 -1) ) / MA_Size1 

                    sma_amplitude = (amplitude  + code_2_last_sma_amplitude[code] * (MA_Size1 -1) ) / MA_Size1 

                    # 更新‘前日SMA’
                    code_2_last_sma_up[code] = sma_up
                    code_2_last_sma_amplitude[code] = sma_amplitude 

                    print m, sma_up, sma_amplitude,  sma_up /  sma_amplitude * 100

                #过往记录走完，做一条rsi
                rsi = code_2_last_sma_up[code] / code_2_last_sma_amplitude[code] * 100
                indi_of_the_code.append(rsi) 

                print 

            else:
                # 直接算当日RSI
                assert(   t_day  == md_window[-1][0])

                delta = md_of_the_code[0] - md_of_the_code[1]
                
                if delta >= 0:
                    up        = delta
                    amplitude = delta 
                else:
                    up = 0
                    amplitude = - delta 

                sma_up = (up + code_2_last_sma_up[code] * (MA_Size1 -1) ) / MA_Size1 
                sma_amplitude = (amplitude + code_2_last_sma_amplitude[code] * (MA_Size1 -1) ) / MA_Size1

                # 更新‘前日SMA’
                code_2_last_sma_up[code] = sma_up
                code_2_last_sma_amplitude[code] = sma_amplitude 
        
                #过往记录走完，做一条rsi
                rsi = sma_up / sma_amplitude * 100
                indi_of_the_code.append(rsi) 
                print t_day, md_of_the_code,  sma_up, sma_amplitude,  rsi
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md


# 输入数组his_md
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志，最高，最低]
# 返回时，各‘指标’数组的末尾加上 ‘KDJ’       
def extend_indices_add_kdj( conn,  his_md,  MA_Size1 =5):

    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价，前日收盘价, 涨幅, 最高, 最低], 
                   #                    [交易日，收盘价，前日收盘价, 涨幅，最高, 最低] , ... 
                   #                ]
    
    recent_kdj = {}  # 代码==> 该代码最后几交易日的‘KDJ’  
                   # 其中‘KDJ’ 是  [ 
                   #                    [交易日，RSV, K, D, J ], 
                   #                    [交易日，RSV, K, D, J ], ... 
                   #                ]
    code_2_last_K = {} # 代码 ==> 该代码前一日的K
    code_2_last_D = {} # 代码 ==> 该代码前一日的D

    md_prev_day = None

    window_size = MA_Size1 + 36    #  (2/3) ^ 18 < 0.001

    for md_that_day  in his_md:
        
        t_day   =  md_that_day[0]
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
            
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                # 需要从外部获取本code最后N日记录
                recent_memo = data_fetcher.get_his_until( code, t_day, window_size)
                recent_mds[code] = recent_memo 

            else:
                # 停牌的行情不加入 recent_mds
                if not  md_of_the_code[4] : 
                    delta = md_of_the_code[0] - md_of_the_code[1] 
                    high  = md_of_the_code[5]
                    low   = md_of_the_code[6]
                    recent_mds[code].append( [t_day, md_of_the_code[0], md_of_the_code[1], delta , high, low ] )
 
            if len(recent_mds[code]) > window_size:
                del recent_mds[code][0]

            # 至此window_size个行情有了，开始计算KDJ 
            
            
            md_window = recent_mds[code]
            
            if code not in recent_kdj:
                # 本code还没算过KDJ, 需要先填补过往的 RSV值
                last_K   = 50
                last_D   = 50
                recent_kdj[code] = []

                # 计算RSV值，记入recent_kdj
                for i,m in enumerate( md_window):
                    if ( i < MA_Size1):
                        # 不足N日
                        his_window = md_window[0: i +1]
                    else:
                        # 够N日，滑动窗口
                        his_window = md_window[ i+ 1 - MA_Size1 : i+1 ]
                    
                    #   [交易日，收盘价，前日收盘价, 涨幅, 最高, 最低], 
                    #                                      4     5
                    #print m[0]
                    #util.bp(his_window)

                    high_in_window = max( util.column_of_a2d( his_window, 4 ))
                    low_in_window  = min( util.column_of_a2d( his_window, 5 ))

                    # 当日RSV
                    rsv =  (m[1] - low_in_window) / (high_in_window - low_in_window) * 100
                    #print m[0], m[1], low_in_window, high_in_window , rsv


                    K = last_K * 2 /3 + rsv / 3
                    D = last_D * 2 / 3 + K /3
                    J = K * 3 - D *2

                    last_K = K
                    last_D = D

                    recent_kdj[code].append( [m[0], rsv, K,D,J  ]  ) 

                    #print [m[0], rsv, K,D,J  ]  
                    #print

                last_kdj = recent_kdj[code][-1][2:5] # [2:5] ：去掉开头的" 日期,RSV"
                indi_of_the_code.extend(last_kdj )
                code_2_last_K[code] = last_kdj[0]
                code_2_last_D[code] = last_kdj[1]

            else:
                #已经算过KDJ
                last_K   = code_2_last_K[code]
                last_D   = code_2_last_D[code]
            
                # 够N日，滑动窗口
                his_window = md_window[ - MA_Size1 :]
                
                #                    [交易日，收盘价，前日收盘价, 涨幅, 最高, 最低], 
                #                                                       4     5

                high_in_window = max( util.column_of_a2d( his_window, 4 ))
                low_in_window  = min( util.column_of_a2d( his_window, 5 ))

                assert(   t_day  == his_window[-1][0])

                # 当日RSV
                rsv =  ( md_of_the_code[0] - low_in_window) / (high_in_window - low_in_window) * 100

                K = last_K * 2 /3 + rsv / 3
                D = last_D * 2 / 3 + K / 3
                J = K * 3 - D *2
                
                code_2_last_K[code] = K
                code_2_last_D[code] = D

                #print [t_day, rsv, K,D,J]
 
                indi_of_the_code.extend( [ K,D,J  ]  ) 
            
        # 准备走向下一天
        md_prev_day = md_that_day


    return his_md


