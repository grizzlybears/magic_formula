## -*- coding: utf-8 -*-
from datetime import date
from datetime import datetime
import site
import traceback
import codecs

import data_struct 

import  jqdatasdk as jq
import  pandas as pd

import db_operator
import data_fetcher


def list_all_sec():
    r = jq.get_all_securities()
    pd.set_option('display.max_rows', len(r))
    print r
    pd.reset_option('display.max_rows')


def fetch_target_stock_fundamentals(engine, sec_code , YYYY ):
    
    df =  data_fetcher.get_annual_balancesheet( sec_code , YYYY)
    #print df

    db_operator. save_balance_df_to_db(engine, df)
    
    t_day = YYYY + "-12-31"
    df =  data_fetcher.get_valuation(sec_code , t_day )
    db_operator. save_valuation_df_to_db (engine, df)

    df =  data_fetcher.get_annual_income( sec_code , YYYY )
    db_operator.save_income_df_to_db (engine, df)

    

# 处理 'fetch' 子命令
def handle_fetch( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()

        fetch_target_stock_fundamentals(engine, '000651.XSHE', '2017' )
        
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



# 处理 'list' 子命令
def handle_list( argv, argv0  ): 
    try:
        # connect to DB 
        conn = db_operator.get_db_conn()
        dbcur = conn.cursor()

        # real stuff
        list_all_sec()
        
        # DB clean up
        conn.commit()

    except  Exception as e:
        
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        dbcur.close()
        conn.close()

    return 0


