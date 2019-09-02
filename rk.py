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



# 处理 'rk' 子命令 -- 对指定代码的日线进行rsi_kdj分析  
def handle_rk( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()


        i = len(argv)
        if ( 0 == i  ):
            start_day = '2019-01-01'  
        else:
            start_day  = argv[0]

        
        now = datetime.now()
    
        rk_ana(engine, start_day, now )   # N日涨幅最遭

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0

# rsi + kdj 分析
def rk_ana(engine, start_day, end_day):    
    
    #从DB抓日线数据
    conn = engine.connect()

    # 获取日线数据
    # 返回数组
    #     T_day1,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
    #     T_day2,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
    #     T_day3,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
    #     ...
    # 其中‘行情’ 是  [收盘价，前日收盘，涨幅， 涨停标志，停牌标志]
    his_md = db_operator.db_fetch_dailyline(conn, start_day )

    # ‘指标’ 是  [RSK(2), RSK(4), RSK(8) ]
    make_indices_by_rk( conn,  his_md )


    util.bp( his_md)



# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’数组:  [RSK(2), RSK(4), RSK(8)  ]      
def make_indices_by_rk( conn,  his_md,MA_Size1 = 5):
    make_indices.add_blank_indices( conn,  his_md)

 
