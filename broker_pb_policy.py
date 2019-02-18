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
from   sqlalchemy.sql  import select as alch_select
from   sqlalchemy.sql  import text   as alch_text
import  pandas as pd
import  math

# 其他第三方包
import  jqdatasdk as jq

# 我们的代码
import data_struct 
import db_operator
import data_fetcher
import util
import plotter
import make_indices 

def fetch_brk_candidators(engine,t_day):

    brokers    = jq.get_industry_stocks( 'J67' , date= t_day )

    filtered_1 =  brokers  

    # 排除 ST
    st = jq.get_extras('is_st', filtered_1 , start_date= t_day, end_date= t_day, df=False)

 
    filtered_2 = []
    for code in filtered_1:
        if st[code][0]:
            print "  ...  %s 是ST，排除" % code
            continue

        filtered_2.append(code)


    filtered_3 = []
    # 排除停牌 
    for code in filtered_2:
        if  data_fetcher.is_paused(engine, code, t_day):
            print "  ...  %s 停牌，排除" % code
            continue

        filtered_3.append(code)

    return filtered_3 


#收集该年券商的市值 数据 (含pb)
def fetch_brk_fundamentals_1_year(engine, the_year):

    #1. 获取当年度所有交易日
    alltday = data_fetcher.get_trade_days( the_year)

    for t_day in alltday: 

        #2. 获取候选 -- 券商股，非ST
        candidators = fetch_brk_candidators( engine,  t_day)
        print t_day, candidators 
    

        #3. 获取每个候选每个交易日的市值数据和日线

   
    return 


def fetch_brk_fundamentals_until_now(engine, start_year):
    
    now = datetime.now()

    for y in range( start_year, now.year + 1):
        fetch_brk_fundamentals_1_year( engine, y)



# 处理 'fetch_brk' 子命令
def handle_fetch_brk( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2003  

        i = len(argv)
        if ( 1 == i  ):
            start_year = int(argv[0])
        else:
            now = datetime.now()
            start_year = now.year - 1

        if start_year < 2003:
            print "开始年份必须不小于2003"
            return 1

        fetch_brk_fundamentals_until_now(engine, start_year)


        #fetch_target_stock_fundamentals(engine, '000651.XSHE', '2017' )
        
        # real stuff

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0


