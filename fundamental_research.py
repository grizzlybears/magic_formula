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

def handle_sum_funda( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2005

        i = len(argv)
        now = datetime.now()
        end_year = now.year - 1

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

        sum_funda(engine, start_year, end_year )

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        return 1 
    finally:
        pass

    return 0


def sum_funda(engine, start_year, end_year ):

    start_d = "%s-01-01" % start_year
    end_d   = "%s-12-31" % end_year 

    print "汇总基本面数据 %s ~ %s" %(start_year, end_year)
    conn = engine.connect()
    records = db_operator.db_fetch_funda( conn, start_d, end_d)

    for r in records:
        check_1_funda(conn, r)

    fullname = "%s/fundamentals.csv" % (data_struct.WORKING_DIR,)

    s2i = data_fetcher.get_industry_stocks()

    year_num = end_year - start_year + 1

    with open( fullname, "w")  as f:
        # 1. 打印header行

        f.write("代码,名称,行业"  )
        y = start_year
        while y <= end_year:
            f.write(",扣非净利润(万)%d,净资产(万)%d,自由现金流(万)%d,毛利率%d,参考交易日%d,市值(亿)%d,收盘价%d" 
                    % ( y, y, y, y
                        , y, y, y )
                    )
            y = y + 1

        f.write("\n")

        last_code = ''
        last_code_year_printed = year_num
        for r in records:

            if r.code != last_code:
                # 一个新的code

                # 首先结束上一行(如果中途退市，或者某些时候年报延期，那么会不满一行)
                if last_code != '':
                    for dummy in range(year_num - last_code_year_printed):
                        f.write(",,,,,,,") # 一年七个列
                
                    f.write("\n")
                    f.flush()
                
                # 新行开始
                last_code_year_printed = 0
                last_code = r.code


                # 2. 打印行首固定列
                print_fixxed_cols_in_row_begin(f, r, s2i)

                # 如果该code第一年不是 start_year，需要酌情偏移
                y = int(r.stat_date[0:4])
                for dummy in range( y - start_year):
                    f.write(",,,,,,,") # 一年七个列
                    last_code_year_printed = last_code_year_printed  + 1
            

            # 3. 循环打印每年的数据列
            #f.write("扣非净利润(万)%d,净资产(万)%d,自由现金流(万)%d,毛利率%d,参考交易日%d,市值(亿)%d,收盘价%d\n" 
            #print r.stat_date, r.net_operate_cash_flow , r.net_invest_cash_flow
            f.write(",%s,%s,%s,%s,%s,%s,%s" % (
                     util.nullable_float2(r.adjusted_profit / 1000)
                    , util.nullable_float2((r.total_assets - r.total_liability ) / 10000)
                    , util.nullable_float2((r.net_operate_cash_flow - r.net_invest_cash_flow) / 10000)
                    , util.nullable_float2(r.gross_profit_margin)
                    , r.ref_t_day 
                    , util.nullable_float2(r.market_cap )
                    , util.nullable_float2(r.close_price)
                    )
                    )
            last_code_year_printed = last_code_year_printed  + 1

        # 补完最后一行    
        for dummy in range(year_num - last_code_year_printed):
            f.write(",,,,,,,") # 一年七个列
                
        f.write("\n")

#  打印行首固定列
#  三列两个‘,’  以后的列都以‘,’开头
def    print_fixxed_cols_in_row_begin(f, r, s2i ):
    
    if r.code in s2i:
        ind = ""
        first = 1
        for i in s2i[r.code]:
            if first:
                ind = i
                first = 0
            else:
                ind = ind + "|%s" % i
    else:
        ind = '未知'

    cols = "%s,%s,%s" % (r.code, data_fetcher.get_code_name(r.code), ind )

    f.write(cols)
    print cols

def check_1_funda(conn, r):
    code      = r.code
    stat_date = r.stat_date 

    next_year = int(stat_date[0:4]) + 1
    target_date = '%s-4-30' % next_year

    dt_start = datetime.strptime(target_date ,'%Y-%m-%d').date()

    md_df = data_fetcher.get_daily_line_n(code , dt_start, 5)

    l = len(md_df.index)
    if 0 == l:
        print "WARN! %s 于%s 行情无法获取，只抓取该日可见的市值" % (code, target_date)
        r.ref_t_day = target_date
        r.close_price = None 
        
        val = data_fetcher.get_valuation(code , target_date )
        if  val is None:
            r.market_cap = None
        else:
            r.market_cap  = val['market_cap'][0]

        #print code, val['market_cap'][0]
        return 

    t_day = md_df.index[0]
    close_price = md_df['close'][0]

    r.ref_t_day   = str(t_day)[0:10]
    r.close_price = close_price 

    val = data_fetcher.get_valuation(code , t_day)
    if  val is None:
        r.market_cap = None
    else:
        r.market_cap  = val['market_cap'][0]

       #print code, r.ref_t_day, r.close_price, r.market_cap 
