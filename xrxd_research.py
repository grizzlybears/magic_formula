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

BASE_CODE = '000300.XSHG'    
BASE_NAME = '沪深300'

HOWLONG_FROM_PUB_DAY = 3


def fetch_md_of_register_day(engine, code, register_day, memo):
    print "下载%s的%s(%s)行情" % (code, memo, register_day)

    if register_day is None:
        print "%s的%s为空，略过" % (code, memo)
        return 

    df = data_fetcher.get_daily_line_n(code , register_day, HOWLONG_FROM_PUB_DAY )
    if len(df.index) == 0 : 
        print "%s的%s行情未获得，略过" % (code, memo)
        return 

    db_operator.save_daily_line_to_db(engine, code, df)

    #df_va = data_fetcher.get_valuation( code , register_day)
    #db_operator.save_valuation_df_to_db (engine, df_va)

def fetch_1_year_base(engine, year ):
    print "下载%d年基准的日线" % year
    start_day = "%d-01-01" % year 
    end_day   = "%d-12-31" % year 


    base_df = data_fetcher.get_daily_line (
            BASE_CODE  
            , start_day 
            , end_day 
            )
    db_operator.save_daily_line_to_db (engine, BASE_CODE , base_df)


def fetch_1_year_xrxd(engine, year ):

    print "下载%d年所有股票的除权除息数据" % year
    # 抓除权除息数据
    df_xrxd = data_fetcher.get_XrXd_by_year( year)  
    db_operator.save_XrXd_df_to_db( engine, df_xrxd)

    # 逐条除权除息数据去抓目标股票的登记日(以及次日)市值/行情
    row_num = len(df_xrxd.index)
    for i in range(row_num):

        code          = df_xrxd.iloc[i]['code']
        register_day  = df_xrxd.iloc[i]['a_registration_date']
        
        if register_day is None:
            print "%s的A股登记日为空，略过" % code
            continue
        fetch_md_of_register_day(engine, code, register_day, '登记日')
        
        board_plan_pub_date = df_xrxd.iloc[i]['board_plan_pub_date']
        fetch_md_of_register_day(engine, code, board_plan_pub_date , '董事会公告日')

        shareholders_plan_pub_date = df_xrxd.iloc[i]['shareholders_plan_pub_date']
        fetch_md_of_register_day(engine, code, shareholders_plan_pub_date , '股东大会公告日')

        implementation_pub_date = df_xrxd.iloc[i]['implementation_pub_date']
        fetch_md_of_register_day(engine, code, implementation_pub_date , '实施公告日')


    # 比较基准 
    fetch_1_year_base(engine, year)
    
    
def fetch_xrxd(engine, start_year, end_year ):

    # 逐年抓除权除息数据
    for y in range( start_year, end_year  + 1):
         fetch_1_year_xrxd( engine, y)

    # 比较基准行情酌情最后补一年
    now = datetime.now()
    if end_year < now.year :
        #年度分配是在次年实施的
        last_year = end_year + 1
        fetch_1_year_base(engine, last_year)

# 处理 'fetch_xrxd' 子命令
def handle_fetch_xrxd( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2005

        i = len(argv)
        now = datetime.now()
        end_year = now.year 

        if ( 0== i  ):
            start_year = now.year - 1
        else:
            start_year = int(argv[0])
            
            if ( i >= 2 ):
                end_year  = int(argv[1])

        # JQ的数据从2005开始
        if start_year < 2005:
            print "开始年份必须不小于2005"
            return 1

        fetch_xrxd(engine, start_year, end_year )
        

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0

def sum_xrxd(engine, start_year, end_year ):
    pass

def handle_sum_xrxd( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2005

        i = len(argv)
        now = datetime.now()
        end_year = now.year 

        if ( 0== i  ):
            start_year = now.year - 1
        else:
            start_year = int(argv[0])
            
            if ( i >= 2 ):
                end_year  = int(argv[1])

        # JQ的数据从2005开始
        if start_year < 2005:
            print "开始年份必须不小于2005"
            return 1

        sum_xrxd(engine, start_year, end_year )

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0

