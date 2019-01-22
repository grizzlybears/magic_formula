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


def get_halfhour_line(engine, start_d, code ):

    today  = datetime.now().date()
    
    df = jq.get_price( code 
            , start_date= start_d, end_date= today
            , frequency='30m'
            , fields=['open', 'close', 'high', 'low', 'volume', 'money']
            , skip_paused=True
            , fq='pre'
            )

    #print df

    #row_num = len(df.index)
    last_t_day = ''
    seqno = 0
    for index, row in df.iterrows():
        print("%s %s  open=%f close=%f " %( index, code, row['open'], row['close']))
        t_day =  str(index)[:10]

        if ( t_day == last_t_day ):
            seqno = seqno +1
        else:
            last_t_day = t_day
            seqno = 0
        
        db_operator.db_save_sub_line(engine
                , code
                , t_day
                , 30 #interval
                , seqno
                , row['open']
                , row['close']
                , row['high']
                , row['low']
                , row['volume']
                , row['money']
                )


# 处理 'hh' 子命令 -- 下载指定代码的半小时线  
def handle_hh( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()

        code = '000300.XSHG'

        i = len(argv)
        if ( 0 == i  ):
            start_day = '2008-01-01'  
        else:
            start_day  = argv[0]

            if ( i >= 2 ):
                code  = argv[1]
        
        get_halfhour_line(engine, start_day, code )   

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0

def get_spec_halfhour_line(engine, start_d, end_d, code , which_seqno):

    
    df = jq.get_price( code 
            , start_date= start_d, end_date= end_d
            , frequency='30m'
            , fields=['open', 'close', 'high', 'low', 'volume', 'money']
            , skip_paused=True
            , fq='pre'
            )

    #print df

    #row_num = len(df.index)
    last_t_day = ''
    seqno = 0
    for index, row in df.iterrows():
        t_day =  str(index)[:10]

        if ( t_day == last_t_day ):
            seqno = seqno +1
        else:
            last_t_day = t_day
            seqno = 0

        if seqno not in  which_seqno:
            continue
        
        #print("%s %s  open=%f close=%f " %( index, code, row['open'], row['close']))
        db_operator.db_save_sub_line_fast(engine
                , code
                , t_day
                , 30 #interval
                , seqno
                , row['open']
                , row['close']
                , row['high']
                , row['low']
                , row['volume']
                , row['money']
                )


def get_halfhour_line_all(engine, start_d, end_d ):
    

    all_stocks = list(jq.get_all_securities(['stock']).index)
    
    for one_code in all_stocks:
        print "%s, fetching %s (%s ~ %s)" % (datetime.now(), one_code, start_d, end_d  )
        get_spec_halfhour_line(engine, start_d, end_d, one_code, [0,7]) #要第一个‘半小时’，和最后一个‘半小时’


# 处理 'hha' 子命令 -- 下载所有代码的半小时线  
def handle_hha( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()


        i = len(argv)
        if ( 0 == i  ):
            start_day = '2008-01-01'  
        else:
            start_day  = argv[0]

            if ( i >= 2 ):
                end_day  = argv[1]
            else:
                today  = datetime.now().date()
                end_day = today 
                
        
        get_halfhour_line_all(engine, start_day, end_day )   

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0

