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

    #抓取第二年05/01日的市值
    t_day = str(the_year + 1)  + "-05-01"
    df =  data_fetcher.get_valuation(sec_code , t_day )
    db_operator. save_valuation_df_to_db (engine, df)

    if the_year < (now.year - 1):
        #前年或更久以前

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




def is_paused(engine, code, t_day ):

    q = db_operator.query_paused( engine, code, t_day)

    if q is not None:
        return q == 1

    print '%s, %s DB里没查到是否停牌' % (code,t_day)

    i = data_fetcher.check_if_paused(code, t_day)
    
    db_operator.record_paused(engine , code , t_day, i )
    

    return i != 0


def fetch_magic_candidators(engine,t_day):
    # 中证价值回报量化策略指数的样本空间由满足以下条件的沪深 A 股构成： 
    # （1）非 ST、*ST 股票，非暂停上市股票； （2）非金融类股票。
    
    all_stocks = list( jq.get_all_securities(types=['stock'], date= t_day ).index)
 
    # 排除金融类的股票
    banks      = jq.get_industry_stocks( 'J66' , date= t_day )
    brokers    = jq.get_industry_stocks( 'J67' , date= t_day )
    insurances = jq.get_industry_stocks( 'J68' , date= t_day )
    others     = jq.get_industry_stocks( 'J69' , date= t_day)
    exclude = banks + brokers + insurances + others

    filtered_1 = []

    for code in all_stocks:
        if (code in exclude) :
            print "  ... %s 是金融类, 排除..." % code 
            continue

        filtered_1.append (code)

    # 排除 ST
    st = jq.get_extras('is_st', filtered_1, start_date= t_day, end_date= t_day, df=False)

 
    filtered_2 = []
    for code in filtered_1:
        if st[code][0]:
            print "  ...  %s 是ST，排除" % code
            continue

        filtered_2.append(code)


    filtered_3 = []
    # 排除停牌 
    for code in filtered_2:
        if is_paused(engine, code, t_day):
            print "  ...  %s 停牌，排除" % code
            continue

        filtered_3.append(code)


    return filtered_3 


def fetch_fundamentals_1_year_may(engine, the_year, t_day):
    #根据去年年报，准备每年5月的样本列表。
    print "make list of %s" % t_day

    candidators = fetch_magic_candidators( engine,  t_day)
    print candidators 


def fetch_fundamentals_1_year_nov(engine, the_year, t_day):
    #根据当年的半年报，准备每年11月的样本列表。
    print "make list of %s" % t_day
    pass

#为了进行'the_year'的调仓，收集基本数据
def fetch_fundamentals_1_year(engine, the_year):
    #中证价值回报量化策略指数的样本股每半年调整一次
    #样本股调整实施日为每年5月和11月的第六个交易日。

    today  = datetime.now()

    if today.year < the_year:
        t_day = data_fetcher.get_t_day_in_mon( the_year, 5 , 6)
        fetch_fundamentals_1_year_may(engine, the_year, t_day)
        
        t_day = data_fetcher.get_t_day_in_mon( the_year, 11 , 6)
        fetch_fundamentals_1_year_nov(engine, the_year, t_day)
        return

    # 检查是否可以制作今年5月的样本列表
    t_day = data_fetcher.get_t_day_in_mon( the_year, 5 , 6)
    if t_day is None or today <= t_day:
        return
    
    fetch_fundamentals_1_year_may(engine, the_year, t_day)

      
    # 检查是否可以制作今年11月的样本列表
    t_day = data_fetcher.get_t_day_in_mon( the_year, 11, 6)
 
    if t_day is None or today <= t_day:
        return
  
    fetch_fundamentals_1_year_nov(engine, the_year,t_day)
    
    return 



def fetch_fundamentals_1_year_300(engine, the_year):
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


