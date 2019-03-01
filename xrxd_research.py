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

def fetch_1_year_xrxd(engine, year ):

    df = data_fetcher.get_XrXd_by_year( year)  
    db_operator.save_XrXd_df_to_db( engine, df)



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

        fetch_xrxd_until_now(engine, start_year, end_year )
        

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0


