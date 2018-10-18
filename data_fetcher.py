# -*- coding: utf-8 -*-


import  jqdatasdk as jq
import  pandas as pd
import  numpy  as np
from datetime import date,datetime,timedelta

import db_operator
import data_struct 
import jq_acc

# 获得指定股票指定年度的负债表
def get_annual_balancesheet(sec_code , statYYYY):
    q = jq.query(
          jq.balance
          ).filter(
                  jq.balance.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的资产表没查到 " % (sec_code , statYYYY  )
    return ret

# 获得指定股票指定年度的利润表
def get_annual_income(sec_code , statYYYY):
    q = jq.query(
          jq.income
          ).filter(
                  jq.income.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的利润没查到 " % (sec_code , statYYYY  )
    return ret

valuation_fetched = {}

# 获得指定股票指定日期的市值数据
def get_valuation(sec_code , yyyy_mm_dd):
    k = ( sec_code, yyyy_mm_dd)
    if k in valuation_fetched: 
        print "    skip fetching valuation %s, %s " %  k 
        return None
 
    valuation_fetched[k] = 1 
    #print "    fetch valuation of %s, %s" % k 

    q = jq.query(
          jq.valuation
          ).filter(
                  jq.valuation.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, date = yyyy_mm_dd)
    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的市值数据资产表没查到 " % (sec_code , yyyy_mm_dd  )
    return ret


daily_line_fetched = {}

# 获得指定股票指定时间段的日线
def get_daily_line(sec_code , t_start, t_end ):

    k = (sec_code , t_start, t_end )

    if k in daily_line_fetched:
        #print "    skip fetching daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
        return None

    daily_line_fetched[k] = 1 

    #print "    fetch daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
    df = jq.get_price(sec_code
            , start_date= t_start, end_date=t_end
            , frequency='daily'
            , fields=None
            , skip_paused=False
            , fq='pre'
            )

    return df


# 确定某个月的第几个交易日是哪一天
# 返回: 'datetime' or None  
def get_t_day_in_mon( y ,m , tday_no ):
    t_start = "%d-%02d-01" % (y,m)
    
    
    if m == 12:
        y = y +1
        m = 1
    else:
        m = m+1

    t_end = "%d-%02d-01" % (y, m)

    df = jq.get_price(
             '000300.XSHG'
            , start_date= t_start, end_date=t_end
            , frequency='daily'
            , fields=None
            , skip_paused=True
            , fq='pre'
            )

    if len(df.index) < tday_no:
        return None

    #jqdata 返回的行的索引类型是  numpy.datetime64[ns]  (相当于time_t，以ns为单位)
    tt =  df[tday_no -1: tday_no ].index.values[0]

    #print tt
    #print tt.dtype

    ts = (tt - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    
    d = datetime.utcfromtimestamp( ts)

    #print d

    return d.date()

# 查询某股票某天是否停牌
def check_if_paused( code, t_day ):
    df = jq.get_price(
             code 
            , start_date= t_day, end_date=t_day
            , frequency='daily'
            , fields=None
            , skip_paused=True
            , fq='pre'
            )
    
    if len(df.index) < 1:
        return 1

    return 0

def fill_stock_name(l):
    for stock in l:
        si = jq.get_security_info(stock.code)
        stock.name = si.display_name

