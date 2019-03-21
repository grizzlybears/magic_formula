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

HOWLONG_FROM_PUB_DAY = 3


def fetch_md_of_register_day(engine, code, register_day, memo):
    print "下载%s的%s(%s)行情" % (code, memo, register_day)

    if register_day is None:
        print "%s的%s为空，略过" % (code, memo)
        return 

    df = data_fetcher.get_daily_line_n(code , register_day, HOWLONG_FROM_PUB_DAY )
    if df is None or len(df.index) == 0 : 
        print "%s的%s行情未获得，略过" % (code, memo)
        return 

    db_operator.save_daily_line_to_db(engine, code, df)

    df_va = data_fetcher.get_valuation( code , register_day)
    db_operator.save_valuation_df_to_db (engine, df_va)

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

# 获取自day日(含)起第一天的行情
def fetch_md_of_spec_day(conn, code , from_day ):
    
    # 返回  [交易日, 收盘价，开盘价，前日收盘价,  前复权因子]
    target_md = db_operator.query_first_dailyline(conn, from_day, code)
    if target_md is None:
        return None

    t_day = target_md[0]
    dt_t_day = datetime.strptime(t_day,'%Y-%m-%d').date()
    dt_from_day = datetime.strptime(from_day,'%Y-%m-%d').date()

    day_delta = dt_t_day.toordinal() -  dt_from_day.toordinal()
    if day_delta > 5:
        print "WARN! %s自%s始第一个交易日是%s，相距过大，不能采纳。" % (code, from_day, t_day  )
        return None

    return target_md

def check_md_of_spec_day(conn, code, spec_day, day_type, xrxd_note):
    
    checkinfo = data_struct.XrXdCheckInfo()
    checkinfo.check_date   = spec_day 
    checkinfo.check_reason = day_type

    # 解析公告文本
    p = util.parse_xrxd_note(xrxd_note)

    if p is not None:
        checkinfo.dividend_ratio  = p[0]
        checkinfo.transfer_ratio  = p[1]
        checkinfo.bonus_ratio_rmb = p[2]


    # 抓取标的公告后行情
    # [交易日, 收盘价，开盘价，前日收盘价,  前复权因子]
    md  = fetch_md_of_spec_day( conn, code, spec_day)
    if md is None:
        print "WARN! 无法获得%s 于%s(%s)的行情" % (code, spec_day, day_type  )
        return checkinfo
   
    checkinfo.t_day = md[0]
    checkinfo.p_close =  md[1] / md[4]
    checkinfo.p_open  =  md[2] / md[4]
    checkinfo.p_pre_close   =  md[3] / md[4]
 
    # 抓取基准公告后行情
    # [交易日, 收盘价，开盘价，前日收盘价,  前复权因子]
    md  = fetch_md_of_spec_day( conn, BASE_CODE, checkinfo.t_day )
    if md is None:
        print "WARN! 无法获得%s 于%s(%s %s)的行情" % (BASE_NAME , checkinfo.t_day, code, day_type  )
        return checkinfo
  
    checkinfo.b_close =  md[1]  # 基准是指数，无关复权
    checkinfo.b_open  =  md[2] 
    checkinfo.b_pre_close   =  md[3] 
 
    return checkinfo

def check_md_of_spec_day_noparse(conn, code, spec_day, day_type,div,trans, bonus):
    
    checkinfo = data_struct.XrXdCheckInfo()
    checkinfo.check_date   = spec_day 
    checkinfo.check_reason = day_type

    checkinfo.dividend_ratio  = div 
    checkinfo.transfer_ratio  = trans 
    checkinfo.bonus_ratio_rmb = bonus

    # 抓取标的公告后行情
    # [交易日, 收盘价，开盘价，前日收盘价,  前复权因子]
    md  = fetch_md_of_spec_day( conn, code, spec_day)
    if md is None:
        print "WARN! 无法获得%s 于%s(%s)的行情" % (code, spec_day, day_type  )
        return checkinfo
   
    checkinfo.t_day = md[0]
    checkinfo.p_close =  md[1] / md[4]
    checkinfo.p_open  =  md[2] / md[4]
    checkinfo.p_pre_close   =  md[3] / md[4]
 
    # 抓取基准公告后行情
    # [交易日, 收盘价，开盘价，前日收盘价,  前复权因子]
    md  = fetch_md_of_spec_day( conn, BASE_CODE, checkinfo.t_day )
    if md is None:
        print "WARN! 无法获得%s 于%s(%s %s)的行情" % (BASE_NAME , checkinfo.t_day, code, day_type  )
        return checkinfo
  
    checkinfo.b_close =  md[1]  # 基准是指数，无关复权
    checkinfo.b_open  =  md[2] 
    checkinfo.b_pre_close   =  md[3] 
 
    return checkinfo



def check_1_xrxd(conn, xrxd):
    
    # 董事会公告日
    xrxd.md_of_board = check_md_of_spec_day( conn, 
            xrxd.code, 
            xrxd.board_plan_pub_date,
            '董事会公告',
            xrxd.board_plan_bonusnote
            )
    
    # 股东大会公告日
    xrxd.md_of_shareholders = check_md_of_spec_day( conn, 
            xrxd.code, 
            xrxd.shareholders_plan_pub_date,
            '股东大会公告',
            xrxd.shareholders_plan_bonusnote 
            )
 
    # 股东实施公告日
    xrxd.md_of_implementation  = check_md_of_spec_day( conn, 
            xrxd.code, 
            xrxd.implementation_pub_date ,
            '实施公告',
            xrxd.implementation_bonusnote
            )
  
    # A股登记日
    xrxd.md_of_registration  = check_md_of_spec_day_noparse( conn, 
            xrxd.code, 
            xrxd.a_registration_date,
            'A股登记日',
            xrxd.dividend_ratio ,
            xrxd.transfer_ratio ,
            xrxd.bonus_ratio_rmb 
            )

def generate_xrxd_csv( records, filename ):
    fullname = "%s/%s.csv" % (data_struct.WORKING_DIR, filename)
    #the_file = io.open( filename, "w", encoding='utf-8')
    #the_file.close()
    
    # 代码，报告期，董事会公告日，董事会方案，董事会公告日行情，
    #               股东大会公告日，股东大会方案，股东大会公告日行情，
    #               实施公告日，实施公告方案，实施公告日行情
    #               A股登记日，登记日行情
    #分配基盘(万股)，送股数(万股)，转股数(万股)，分红金额(万股)，登记日股息率，登记日总市值
    #
    header = "代码,报告期"
    header = header + ",董事会公告日,董事会方案,董送股率,董转股率,董分红率,董第一交易日,董开盘,董收盘,董昨收,董基准开盘,董基准收盘,董基准昨收"
    header = header + ",股东大会公告日,股东大会方案,股送股率,股转股率,股分红率,股第一交易日,股开盘,股收盘,股昨收,股基准开盘,股基准收盘,股基准昨收"

    header = header + ",实施公告日,实施方案,实送股率,实转股率,实分红率,实第一交易日,实开盘,实收盘,实昨收,实基准开盘,实基准收盘,实基准昨收"
    
    header = header + ",A股登记日,送股率,转股率,分红率,登第一交易日,登开盘,登收盘,登昨收,登基准开盘,登基准收盘,登基准昨收"

    header = header + ",分配基盘(万股),送股数(万股),转股数(万股),分红金额(万),登记日股息率,登记日总市值"

    with open( fullname, "w")  as f:
        f.write("%s\n" %  header  )

        for r in records:
            f.write("%s\n" % r.to_csv_str() )
 
def sum_xrxd(engine, start_year, end_year ):
    start_d = "%s-01-01" % start_year
    end_d   = "%s-12-31" % end_year 

    conn = engine.connect()
    xrxd_records = db_operator.db_fetch_xrxd( conn, start_d, end_d)

    for r in xrxd_records:
        check_1_xrxd( conn, r)

    generate_xrxd_csv( xrxd_records, "xrxd_research" )

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


# 处理 'fetch_forcast' 子命令
def handle_fetch_forcast( argv, argv0 ): 
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

        fetch_forcast(engine, start_year, end_year )
        

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0

def fetch_forcast(engine, start_year, end_year ):

    # 逐年抓业绩预告数据
    for y in range( start_year, end_year  + 1):
         fetch_1_year_forcast( engine, y)

    # 比较基准行情酌情最后补一年
    now = datetime.now()
    if end_year < now.year :
        #自然年末的预告，可能会影响次年的行情
        last_year = end_year + 1
        fetch_1_year_base(engine, last_year)

def fetch_1_year_forcast(engine, year ):

    print "下载%d年所有股票的业绩预告数据" % year
    # 抓业绩预告数据
    df = data_fetcher.get_forcast_by_year( year )  
    db_operator.save_forcast_df_to_db( engine, df)

    # 逐条除权除息数据去抓目标股票的登记日(以及次日)市值/行情
    row_num = len(df.index)
    #n = 0
    for i in range(row_num):

        code     = df.iloc[i]['code']
        pub_date = df.iloc[i]['pub_date']
        
        if pub_date is None:
            print "%s的业绩预告公布日为空，略过" % code
            continue
        #if code != '002952.XSHE':
        #    continue

        fetch_md_of_register_day(engine, code, pub_date, '业绩预告公布日')
        #n = n +1
        #if n == 100:
        #    break

    # 比较基准 
    fetch_1_year_base(engine, year)
    

