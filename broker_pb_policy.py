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

    return brokers 

 
#收集该年券商的市值 数据 (含pb)
def fetch_brk_fundamentals_1_year(engine, the_year):
 
    print "fetching year of %d" % the_year 

    #1. 获取当年度所有交易日
    alltday = data_fetcher.get_trade_days( the_year)

    first_t_day = alltday[ 0]
    last_t_day  = alltday[-1]
    
    #2. 获取候选 -- 券商股
    candidators = fetch_brk_candidators( engine,  last_t_day)
    print candidators 
        
    #3. 获取每个候选每个交易日的市值数据
    print "fetching valuations..." 
    for t_day in alltday: 
        for sec_code in candidators:
            df =  data_fetcher.get_valuation(sec_code , t_day )
            db_operator. save_valuation_df_to_db (engine, df)
    

    #4. 获取每个候选当年的日线
    print "fetching daily lines..." 

    for sec_code in candidators:
        print sec_code 
        df =  data_fetcher.get_daily_line(sec_code , first_t_day, last_t_day )
        db_operator. save_daily_line_to_db (engine, sec_code , df)

    print "%d year over." % the_year 
   
    return 


def fetch_brk_fundamentals_until_now(engine, start_year, end_year ):

    for y in range( start_year, end_year  + 1):
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
        now = datetime.now()
        end_year = now.year 

        if ( 0== i  ):
            start_year = now.year - 1
        else:
            start_year = int(argv[0])
            
            if ( i >= 2 ):
                end_year  = argv[1]

        if start_year < 2003:
            print "开始年份必须不小于2003"
            return 1

        fetch_brk_fundamentals_until_now(engine, start_year, end_year )


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


