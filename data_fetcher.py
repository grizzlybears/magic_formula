# -*- coding: utf-8 -*-


import  jqdatasdk as jq
import  pandas as pd

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

    return ret

# 获得指定股票指定年度的利润表
def get_annual_income(sec_code , statYYYY):
    q = jq.query(
          jq.income
          ).filter(
                  jq.income.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    return ret

valuation_fetched = {}

# 获得指定股票指定日期的市值数据
def get_valuation(sec_code , yyyy_mm_dd):
    k = ( sec_code, yyyy_mm_dd)
    if k in valuation_fetched: 
        print "    skip fetching valuation %s, %s " %  k 
        return None
 
    valuation_fetched[k] = 1 

    print "    fetch valuation of %s, %s" % k 

    q = jq.query(
          jq.valuation
          ).filter(
                  jq.valuation.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, date = yyyy_mm_dd)

    return ret


daily_line_fetched = {}

# 获得指定股票指定时间段的日线
def get_daily_line(sec_code , t_start, t_end ):

    k = (sec_code , t_start, t_end )

    if k in daily_line_fetched:
        print "    skip fetching daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
        return None

    daily_line_fetched[k] = 1 

    print "    fetch daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
    df = jq.get_price(sec_code
            , start_date= t_start, end_date=t_end
            , frequency='daily'
            , fields=None
            , skip_paused=False
            , fq='pre'
            )

    return df





