## -*- coding: utf-8 -*-
import sys

import site
import traceback

from datetime import date,datetime,timedelta

import codecs
import csv

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


def fetch_target_stock_fundamentals(engine, sec_code , the_year ):

    YYYY = str(the_year)
    
    #抓取资产负债表
    df =  data_fetcher.get_annual_balancesheet( sec_code , YYYY)
    db_operator. save_balance_df_to_db(engine, df)
    
    #抓取利润表
    df =  data_fetcher.get_annual_income( sec_code , YYYY )
    db_operator.save_income_df_to_db (engine, df)

    now = datetime.now()
    #抓取当年05/01日的市值
    t_day = str(the_year )  + "-05-01"
    df =  data_fetcher.get_valuation(sec_code , t_day )
    db_operator. save_valuation_df_to_db (engine, df)

    #抓取第二年05/01日的市值
    t_day = str(the_year + 1)  + "-05-01"
    df =  data_fetcher.get_valuation(sec_code , t_day )
    db_operator. save_valuation_df_to_db (engine, df)

    if the_year < (now.year - 1):
        #前年或更久以前

        #抓取当年05/01 ~ 05/10的行情，计算收益用
        t_day = str(the_year)   + "-05-01"
        t_day_end = str(the_year )  + "-05-10"
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)

        #抓取第二年05/01 ~ 05/10的行情，换仓用
        t_day = str(the_year + 1)  + "-05-01"
        t_day_end = str(the_year + 1)  + "-05-10"
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)

        #抓取第三年05/01 ~ 05/10的行情，计算收益用
        t_day = str(the_year + 2)  + "-05-01"
        t_day_end = str(the_year + 2)  + "-05-10"
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)
    elif the_year < now.year:
        # the_year是 去年

        #抓取当年05/01 ~ 05/10的行情，计算收益用
        t_day = str(the_year )  + "-05-01"
        t_day_end = str(the_year )  + "-05-10"
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)


        if now.month >=5 :
            #抓取第二年05/01 ~ 05/10的行情，换仓用
            t_day = str(the_year + 1)  + "-05-01"
            t_day_end = str(the_year + 1)  + "-05-10"
            df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
            db_operator. save_daily_line_to_db (engine, sec_code , df)

        #抓取最后十天行情，计算收益用
        td = timedelta( days = 10)
        t_day = (now - td).strftime("%Y-%m-%d")
        t_day_end = now.strftime("%Y-%m-%d")
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)
    else:
        # the_year就是今年
        if now.month >=5 :
            #抓取今年05/01 ~ 05/10的行情，换仓用
            t_day = str(the_year )  + "-05-01"
            t_day_end = str(the_year )  + "-05-10"
            df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
            db_operator. save_daily_line_to_db (engine, sec_code , df)

        #抓取最后十天行情，计算收益用
        td = timedelta( days = 10)
        t_day = (now - td).strftime("%Y-%m-%d")
        t_day_end = now.strftime("%Y-%m-%d")
        df =  data_fetcher.get_daily_line(sec_code , t_day, t_day_end  )
        db_operator. save_daily_line_to_db (engine, sec_code , df)


def list_index_1_year(code, the_year):
    #
    yyyymmdd= "%d-05-01" % the_year

    # 取当年该指数成份
    members = jq.get_index_stocks( code, date = yyyymmdd )

    filename = "%s/%s.%s.csv" % ( data_struct.WORKING_DIR,  code, the_year )
    with open(filename, "w") as the_file:
        writer = csv.writer( the_file , lineterminator='\n')
        for val in members:
            writer.writerow([val])

 

def fetch_fundamentals_1_year(engine, the_year):
    #
    # 沪深300 样本股调整实施时间分别是每年 6 月和 12 月的第二个星期五的下一交易日
    # 我们假设每年五月(年报出来)换仓
    print "fetch fetch_fundamentals for %d" % the_year

    yyyymmdd= "%d-05-01" % the_year

    # 取当年沪深300成份
    hs300 = jq.get_index_stocks( '000300.XSHG', date = yyyymmdd )
    #print hs300
    
    #J66 货币金融服务    1991-04-03
    #J67 资本市场服务    1994-01-10
    #J68 保险业  2007-01-09
    #J69 其他金融业

    # 排除不适用魔法公式的行业
    banks      = jq.get_industry_stocks( 'J66' , date= yyyymmdd)
    brokers    = jq.get_industry_stocks( 'J67' , date= yyyymmdd)
    insurances = jq.get_industry_stocks( 'J68' , date= yyyymmdd)
    others     = jq.get_industry_stocks( 'J69' , date= yyyymmdd)
    exclude = banks + brokers + insurances + others

    #print banks
    #print insurances

    for code in hs300:
        if (code in exclude) :
            print "  ... skip %s ..." % code 
            continue
     
        print "  fetching %s" % code
        fetch_target_stock_fundamentals(engine, code , the_year )
        #break


    print "finished fetching fetch_fundamentals for %d" % the_year
    print 




def fetch_fundamentals_until_now(engine, start_year):
    
    now = datetime.now()

    for y in range( start_year, now.year + 1):
        fetch_fundamentals_1_year( engine, y)


def list_index_until_now(code, start_year):
    now = datetime.now()

    for y in range( start_year, now.year):
        list_index_1_year( code, y)



# 处理 'fetch' 子命令
def handle_fetch( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2005  # 沪深300从 2004年才开始有

        i = len(argv)
        if ( 1 == i  ):
            start_year = int(argv[0])
        else:
            now = datetime.now()
            start_year = now.year - 1

        if start_year < 2005:
            print "开始年份必须不小于2005"
            return 1

        fetch_fundamentals_until_now(engine, start_year)


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



# 处理 'list' 子命令
def handle_list( argv, argv0  ): 
    try:
        i = len(argv)
        if ( 1 == i  ):
            start_year = int(argv[0])
        else:
            now = datetime.now()
            start_year = now.year - 1

        if start_year < 2005:
            print "开始年份必须不小于2005"
            return 1

        list_index_until_now('000300.XSHG', start_year)

    except  Exception as e:
        
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0


