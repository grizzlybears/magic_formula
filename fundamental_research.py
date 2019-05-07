## -*- coding: utf-8 -*-

# py系统包
import sys
import site
import traceback
from datetime import date,datetime,timedelta
import codecs
import csv
import collections
import io
import os

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

#需要从05年开始，每年的净利润，净资产，市值，自由现金流，毛利率
#经营活动现金流量净额 - 投资活动现金流量净额 = 自由现金流
#能把大行业也标出来么？ 比如，制造，金融，服务，采选什么的
#如果能列出每年四月三十号的复权价格就更好了
#输出是一个大表格，每个证券一行，然后一年N列， 一行包括n年

# 处理 'fetch_funda' 子命令
def handle_fetch_funda( argv, argv0 ): 
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

        # 逐年抓基本面数据
        for y in range( start_year, end_year  + 1):
            fetch_1_year_funda( engine, y)

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print e
        return 1 
    finally:
        pass

    return 0

def fetch_1_year_funda(engine, year ):

    print "下载%d年所有股票的基本面数据" % year
    # 抓基本面数据
    df =  data_fetcher.get_annual_value_indicator2( year ) 
    
    # 存入DB
    stat_date =  '%d-12-31' % year
    db_operator.db_save_annual_funda(engine,  stat_date, df )


