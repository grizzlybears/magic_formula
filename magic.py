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

from   sqlalchemy.sql  import select as alch_select
from   sqlalchemy.sql  import text   as alch_text

import util

s_nofetch = False 

MF_MODE = 0   # 0: classic magic formula, 1: 'MF2'

MF_NetValue = 1

COMPO_NUMBER = 80

MAGIC_VOLUMN = 100 * 10000


MF2_COMPO_NUMBER2    = 80  # 成份数量
MF2_BACK_TRACE_YEARS = 2   # 追溯以往多少年的财报
MF2_CANDI_THRESHOLD  = 80  # 进入TOP多少才是候选
MF2_COMPO_THRESHOLD  = 2   # 追溯期进入多少次候选才是成份
MF2_Q=[]

def list_all_sec():
    r = jq.get_all_securities()
    pd.set_option('display.max_rows', len(r))
    print r
    pd.reset_option('display.max_rows')


def fetch_target_stock_fundamentals_and_some_md(engine, sec_code , the_year ):

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

    #print '%s, %s DB里没查到是否停牌' % (code,t_day)

    i = data_fetcher.check_if_paused(code, t_day)
    
    db_operator.record_paused(engine , code , t_day, i )
    

    return i != 0


def fetch_magic_candidators(engine,t_day):
    # 中证价值回报量化策略指数的样本空间由满足以下条件的沪深 A 股构成： 
    # （1）非 ST、*ST 股票，非暂停上市股票； （2）非金融类股票。
    
    all_stocks = list( jq.get_all_securities(types=['stock'], date= t_day ).index)
    #all_stocks = all_stocks[:100]


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

def cmp_roc( a, b):
    #先对 ROC 的倒数进行升序排列
    #再对其中为负的部分进行降序排列，得到每只股票的 ROC 排名

    if a.ROC == 0:
        ra = 0
    else:
        ra = 1 / a.ROC

    if b.ROC == 0:
        rb = 0
    else:
        rb = 1 / b.ROC

    if ra < 0:
        if rb < 0:
            if ra < rb:
                return 1
            elif ra == rb:
                return 0
            else:
                return -1
        else:
            return -1
    elif ra == 0:
        if rb < 0:
            return 1
        elif rb == 0:
            return 0
        else:
            return -1
    else:
        if rb < 0:
            return 1
        elif rb == 0:
            return 1
        else:
            if ra < rb:
                return -1
            elif ra == rb:
                return 0
            else:
                return 1

def cmp_ey( a, b):
    #对 EY 进行降序排列，得到每只股票的 EY 排名

    if a.EY < b.EY:
        return 1
    elif a.EY == b.EY:
        return 0
    else:
        return -1

def cmp_magic( a, b):
    return a.rank_roc + a.rank_ey - b.rank_roc - b.rank_ey 

def check_back( q, code):
    count = 0
    for l in q:
        for entry in l:
            if entry.code == code:
                count = count +1
                break

    return count



# 编制成份列表
def build_composition_list(engine, y, compo_m, stat_m, t_day):

    s,q = get_stat_and_query_date(y,stat_m)
    stat_end = q

    s,q = get_stat_and_query_date_3q_before(y,stat_m)
    stat_start = q


#一起:
#select m.*,e.EBIT,v.market_cap
#from tmpEBIT e
#join vMagicBalance m on ( e.code = m.code and m.statDate='2018-03-31')
#join Valuation v on ( e.code = v.code and v.day = '2018-05-09')
 
    conn = engine.connect()
    print "利润表统计区间 [ %s ~ %s ]" % (stat_start , stat_end )
    db_operator.create_tmp_EBIT( conn, stat_start , stat_end )

    stock_list = db_operator. db_fetch_stock_statements(conn, stat_end, t_day, y, stat_m)

    #util.bp( stock_list)

    sorted_by_roc = sorted( stock_list, cmp= cmp_roc)
    for i, sci in  enumerate( sorted_by_roc):
        sci.rank_roc = i
    #util.bp( sorted_by_roc)


    sorted_by_ey = sorted( sorted_by_roc, cmp= cmp_ey) 
    for i, sci in  enumerate( sorted_by_ey):
        sci.rank_ey = i
    #util.bp( sorted_by_ey)

    
    sorted_by_magic = sorted( sorted_by_ey, cmp= cmp_magic)  
    for i, sci in  enumerate( sorted_by_magic):
        sci.rank_final  = i

    global MF_MODE

    if MF_MODE == 0:

        composition_list = sorted_by_magic[ :COMPO_NUMBER ]
        data_fetcher.fill_stock_name(  composition_list )
        util.bp( composition_list )

        db_operator.db_save_composition_list(conn, y, compo_m , t_day, composition_list)
    elif MF_MODE == 1:
        global MF2_Q

        if len(MF2_Q) < 2 * MF2_BACK_TRACE_YEARS:
            # 还在追溯财报阶段
            candi_list = sorted_by_magic[ :MF2_CANDI_THRESHOLD  ]
            MF2_Q.append( candi_list )
        else:
            # 编制当期成份列表
            compo_number = 0
            
            composition_list = []

            for entry in sorted_by_magic [ :MF2_CANDI_THRESHOLD  ]:
                c = check_back( MF2_Q, entry.code)
                if c >= MF2_COMPO_THRESHOLD :
                    #经过历史考验，纳入本期成份
                    composition_list.append(entry)

                    #compo_number = compo_number + 1
                    #if compo_number == MF2_COMPO_NUMBER2:
                    #    break 

            del MF2_Q[0]
            candi_list = sorted_by_magic[ :MF2_CANDI_THRESHOLD  ]
            MF2_Q.append( candi_list )
            
            data_fetcher.fill_stock_name(  composition_list )
            util.bp( composition_list )
            #print "%d-%d:  %d compo" % ( y, compo_m, len(composition_list))
            
            db_operator.db_save_composition_list(conn, y, compo_m , t_day, composition_list)
            #raise Exception("aa")
    
    else:
        raise Exception( "Invalid 'MF_MODE'! ")


def get_1q_before (y,m):
    m = m - 3
    if m <= 0:
        m = m + 12
        y = y -1

    return (y,m)

def get_3q_before(y,m):
    m = m - 9
    if m <= 0:
        m = m + 12
        y = y -1
    return (y,m)

def get_stat_and_query_date_3q_before (y,m):
    y,m = get_3q_before(y,m)
    return get_stat_and_query_date(y,m)


def get_stat_and_query_date(y,m):
    s = ""
    q = ""
    if 3 == m:
        s = str(y) + "q1"
        q = str(y)+ "-03-31"
    elif 6==m:
        s = str(y) + "q2"
        q = str(y) + "-06-30"
    elif 9 == m:
        s = str(y) + "q3"
        q = str(y) + "-09-30"
    elif 12 == m :
        s = str(y) + "q4"
        q = str(y) + "-12-31"
    else:
        raise Exception("月%d非法，必须是[3,6,9,12]之一。" % m)

    return (s , q)

# 从指定年月开始(含)，抓过去howmany季的季度利润表
def fetch_season_income_sheet(engine,code, y,m , howmany):
    while howmany>0: 
        
        statDate, queryDate = get_stat_and_query_date( y,m)

        r = db_operator.query_income( engine, code, queryDate)
        if r is None:
            print "    需要抓利润表%s " % statDate 
            df =  data_fetcher.get_annual_income( code , statDate )
            db_operator.record_income_df_to_db(engine, df)
 
        y,m = get_1q_before( y, m )
        howmany = howmany - 1


def sum_buy_list( buy_list):
    s = 0.0
    for tr in buy_list:
        s = s + tr.amount + tr.fee 

    return s 

def sum_sell_list( sell_list):
    s = 0.0
    for tr in sell_list:
        s = s + tr.amount - tr.fee 

    return s 


def simu_buy( tr ):
    df = data_fetcher.get_daily_line( tr.code , tr.t_day , tr.t_day )
    
    #print df
    #print "%s %s at %f\n" % (tr.t_day, tr.code,  df.iloc[0]['close']) 
    trade_price = df.iloc[0]['close']
    tr.direction = 1

    v = int( MAGIC_VOLUMN / COMPO_NUMBER / trade_price )
    tr.volumn    = v  # TODO: 真正的模拟指数需要‘等权’。然而如果要等权的买100股茅台，那么总体一手ETF的价值就是 80*100 股茅台，买不起啊



    tr.price = trade_price 
    tr.amount =tr.volumn * tr.price 

def get_cur_md(buy_list):
    codes = []
    for tr in buy_list:
        codes.append( tr.code)

    
    today  = datetime.now().date()

    start_d = today - timedelta(days = 15 )

    p = jq.get_price(codes 
            , start_date= start_d , end_date= today
            , frequency='daily'
            , fields='close'
            , skip_paused=False
            , fq='pre'
            )

    df = p['close']

    row_num = len(df.index)

    t_day = df.index[row_num -1].date()
    # print t_day

    position_list = []

    for b in buy_list:
        close_price = df[ b.code ][row_num -1 ]
        #print "%s %f" % ( b.code , close_price  )

        tr = data_struct.TradeRecord()

        tr.code  = b.code 
        tr.name  = b.name 
        tr.t_day = t_day 
        tr.direction = 0
        tr.price  = close_price 
        tr.volumn = b.volumn 
        tr.amount = tr.volumn * tr.price 

        position_list.append( tr )

    return position_list 

def simu_sell( tr, ideal_sell_day):
    today  = datetime.now().date()
    
    start_d = ideal_sell_day
    end_d = start_d + timedelta( days = 30 )
    
    while start_d < today:
        df = jq.get_price( tr.code
            , start_date= start_d ,  end_date= end_d
            , frequency='daily'
            , fields='close'
            , skip_paused=True
            , fq='pre'
            )

        row_num = len(df.index)

        if row_num > 0:
            #print df 
            # yes, we found , just get 1st day
            close_price = df.iloc[ 0]['close'] 

            tr_s  = data_struct.TradeRecord()

            tr_s.code  = tr.code 
            tr_s.name  = tr.name 
            tr_s.t_day = df.index[0].date() 
            tr_s.direction = -1 
            tr_s.price  = close_price 
            tr_s.volumn = tr.volumn 
            tr_s.amount = tr_s.volumn * tr_s.price

            return tr_s
        
        start_d = end_d
        end_d = start_d + timedelta( days = 30)
 
    print "WARN!!! %s(%s) dosn't open to trade frpm %s" % ( tr.code, tr.name, ideal_sell_day  )
 
    #试图获取停牌前最后交易日的价格
    df = jq.get_price( tr.code
            , start_date= tr.t_day,  end_date= ideal_sell_day
            , frequency='daily'
            , fields='close'
            , skip_paused=True
            , fq='pre'
            )
    row_num = len(df.index)

    if row_num ==0 :

        print "WARN!!! %s(%s) even dosn't open to trade frpm %s" % ( 
            tr.code
            , tr.name
            , tr.t_day
            )

        tr_s  = data_struct.TradeRecord()

        tr_s.code  = tr.code 
        tr_s.name  = tr.name 
        tr_s.t_day = today 
        tr_s.direction = 0
        tr_s.price  = tr.price
        tr_s.volumn = tr.volumn 
        tr_s.amount = tr_s.volumn * tr_s.price

        return tr_s

    close_price = df.iloc[ row_num - 1]['close'] 

    tr_s  = data_struct.TradeRecord()
    tr_s.code  = tr.code 
    tr_s.name  = tr.name 
    tr_s.t_day = df.index[ row_num -1].date() 
    tr_s.direction = 0 
    tr_s.price  = close_price 
    tr_s.volumn = tr.volumn 
    tr_s.amount = tr_s.volumn * tr_s.price

    return tr_s


def get_sell_simu( buy_list , ideal_sell_day):

    r = []
    for b in buy_list:
        tr = simu_sell( b ,  ideal_sell_day)
        r.append( tr)

    return r

def backtest_1_year_nov(engine, the_year): 
    print "回测 %d年十一月 的成份列表" % the_year

    conn = engine.connect()

    # 1. 先买入
    buy_list = db_operator.db_fetch_composition_list(conn, the_year, 11 )
    for tr in buy_list:
        simu_buy(tr)

    util.bp( buy_list)
    
    # 2. 再卖出
    ideal_sell_day =  data_fetcher.get_t_day_in_mon( the_year + 1, 5 , 6)
    if ideal_sell_day is None:
        # 说明次年五月第六交易日还没到，则显示当前行情
        result = get_cur_md(buy_list)

    else:
        # 试图在 ideal_sell_day 卖出
        
        result = get_sell_simu( buy_list , ideal_sell_day)

    util.bp(result ) 
    
    
    total_b = sum_buy_list(buy_list)
    total_s = sum_sell_list(result)
    profit = total_s - total_b 

    print "== %d年十一月 的成份列表，总买 %.2f, 总卖 %.2f, 盈亏 %.2f (%.2f%%) ==" % ( 
            the_year
            , total_b 
            , total_s 
            , profit 
            , profit / total_b * 100
            )
    print

    db_operator.db_save_simu_trade_list( conn, the_year, 11 , buy_list, result  )


def backtest_1_year_may(engine, the_year): 

    print "回测 %d年五月 的成份列表" % the_year

    conn = engine.connect()

    # 1. 先买入
    buy_list = db_operator.db_fetch_composition_list(conn, the_year, 5 )
    for tr in buy_list:
        simu_buy(tr)

    util.bp( buy_list)
    
    # 2. 再卖出
    ideal_sell_day =  data_fetcher.get_t_day_in_mon( the_year, 11 , 6)

    if ideal_sell_day is None:
        # 说明当年11月第六交易日还没到，则显示当前行情
        result = get_cur_md(buy_list)

    else:
        # 试图在 ideal_sell_day 卖出
        
        result = get_sell_simu( buy_list , ideal_sell_day)

    util.bp(result )
    
    total_b = sum_buy_list(buy_list)
    total_s = sum_sell_list(result)
    profit = total_s - total_b 

    global MF_NetValue
    MF_NetValue = MF_NetValue * ( 1 + profit  / total_b )


    print "== %d年五月 的成份列表，总买 %.2f, 总卖 %.2f, 盈亏 %.2f (%.2f%%) ==" % ( 
            the_year
            , total_b 
            , total_s 
            , profit 
            , profit / total_b * 100
            )
    
    print 

    db_operator.db_save_simu_trade_list( conn, the_year, 5 , buy_list, result  )




def fetch_fundamentals_1_year_may(engine, the_year, t_day): 
    
    global s_nofetch 
    if s_nofetch:
        print " make list of %s without fetch data from jq" % t_day
        build_composition_list(engine, the_year ,5,  3 , t_day)
        return

    #准备每年5月的样本列表。
    print "make list of %s" % t_day

    candidators = fetch_magic_candidators( engine,  t_day)
    #print candidators 


    for code in candidators:
        #3月,一季报
        statDate, query_statDate  =  get_stat_and_query_date(the_year , 3 ) 

        print "酌情抓 %s 于%s的财报，以及%s的市值" % ( code, statDate, t_day)
        
        #酌情抓取资产负债表
        r = db_operator.query_balancesheet( engine, code, query_statDate)
        if r is None:
            print "    需要抓资产负债表%s" % statDate 
            df =  data_fetcher.get_annual_balancesheet( code , statDate )
            db_operator.record_balance_df_to_db(engine, df)
        
        #酌情抓取利润表
        fetch_season_income_sheet(engine,code, the_year , 3 , 4) #从今年1季开始，抓以往4季

        #酌情抓取t_day的市值 
        ymd = "%d-%02d-%02d" % ( t_day.year, t_day.month, t_day.day )
        r = db_operator.query_valuation( engine, code, ymd )
        if r is None:
            print "    需要抓市值数据%s" % t_day 
            df =  data_fetcher.get_valuation( code , t_day )
            db_operator.record_valuation_df_to_db(engine, df)


    build_composition_list(engine, the_year,5,  3 , t_day)


def fetch_fundamentals_1_year_nov(engine, the_year, t_day): 
    
    global s_nofetch 
    if s_nofetch:
        print " make list of %s without fetch data from jq" % t_day
        build_composition_list(engine, the_year, 11,  9 , t_day)
        return


    #准备每年11月的样本列表。
    print "make list of %s" % t_day
    
    # FIXME: 法克，jqdata的利润表不支持半年报，只有季报和年报，
    #        我们如果需要半年的数据，只有把先搞两个季报，然后自行叠加！
    
    candidators = fetch_magic_candidators( engine,  t_day) 
   
    for code in candidators:

        statDate, query_statDate  =  get_stat_and_query_date(the_year , 9 ) #9月，3季报 
        
        print "酌情抓 %s 于%s的财报，以及%s的市值" % ( code, statDate, t_day)

        #酌情抓取资产负债表
        r = db_operator.query_balancesheet( engine, code, query_statDate)
        if r is None:
            print "    需要抓资产负债表%s"  % statDate
            df =  data_fetcher.get_annual_balancesheet( code , statDate )
            db_operator.record_balance_df_to_db(engine, df)
     
        #酌情抓取利润表
        fetch_season_income_sheet(engine,code, the_year , 9 , 4) #从今年3季开始，抓以往4季
        
        #酌情抓取t_day的市值 
        ymd = "%d-%02d-%02d" % ( t_day.year, t_day.month, t_day.day )
        r = db_operator.query_valuation( engine, code, ymd )
        if r is None:
            print "    需要抓市值数据%s" % t_day
            df =  data_fetcher.get_valuation( code , t_day )
            db_operator.record_valuation_df_to_db(engine, df)

    build_composition_list(engine, the_year, 11, 9 , t_day)

#为了进行'the_year'的调仓，收集基本数据
def fetch_fundamentals_1_year(engine, the_year):
    #中证价值回报量化策略指数的样本股每半年调整一次
    #样本股调整实施日为每年5月和11月的第六个交易日。

    today  = datetime.now().date()

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


def backtest_1_year(engine, the_year):
    #中证价值回报量化策略指数的样本股每半年调整一次
    #样本股调整实施日为每年5月和11月的第六个交易日。

    today  = datetime.now().date()

    if today.year < the_year:
        backtest_1_year_may(engine, the_year, t_day)
        
        backtest_1_year_nov(engine, the_year, t_day)
        return

    # 检查是否需要回测今年5月的样本列表
    t_day = data_fetcher.get_t_day_in_mon( the_year, 5 , 6)
    if t_day is None or today <= t_day:
        return
    
    backtest_1_year_may(engine, the_year )
      
    # 检查是否需要回测今年11月的样本列表
    t_day = data_fetcher.get_t_day_in_mon( the_year, 11, 6)
 
    if t_day is None or today <= t_day:
        return
  
    backtest_1_year_nov(engine, the_year)
    
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

def backtest_until_now(engine, start_year):
    
    now = datetime.now()

    for y in range( start_year, now.year + 1):
        backtest_1_year( engine, y)



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

# 处理 'nofetch' 子命令 -- 不抓财报，只从DB数据生成成份列表
def handle_nofetch( argv, argv0 ): 
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

        global s_nofetch 
        s_nofetch = True

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


# 处理 'nofetch2' 子命令 -- 不抓财报，只从DB数据生成成份列表
# 考察前两年的财报，要求近五届(前两年四届，当期一届)内有三次入top80才进入成份, 成份取50个
def handle_nofetch2( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        

        start_year = 2008   # 沪深300从 2004年才开始有

        i = len(argv)
        if ( 1 == i  ):
            start_year = int(argv[0])
        else:
            now = datetime.now()
            start_year = now.year - 1

        if start_year < 2008:
            print "开始年份必须不小于2008"
            return 1

        global s_nofetch 
        s_nofetch = True

        global MF_MODE 
        MF_MODE = 1

        fetch_fundamentals_until_now(engine, start_year - MF2_BACK_TRACE_YEARS  )


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


# 处理 'backtest' 子命令 -- 根据DB中的成份列表回测
def handle_backtest( argv, argv0 ): 
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

        backtest_until_now(engine, start_year)

        global MF_NetValue 
        print "== 最终净值 %f ==" % MF_NetValue

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
def handle_list_index( argv, argv0  ): 
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


def show_wanke_2017_income():
    code = "000002.XSHE"  # 万科A
    #a = data_fetcher.get_annual_income(code , "2017q1")
    #print a.loc[0, ['statDate', 'net_profit' ] ]
    #print 

    #a = data_fetcher.get_annual_income(code , "2017q2")
    #print a.loc[0, ['statDate', 'net_profit' ] ]
    #print 

    q3 = data_fetcher.get_annual_income(code , "2017q3")
    #print q3.loc[0, ['statDate', 'net_profit' ] ]
    print q3
    print 

    q4 = data_fetcher.get_annual_income(code , "2017q4")
    #print q4.loc[0, ['statDate', 'net_profit' ] ]
    print q4
    print 

    half2 = q3.append(q4,ignore_index=True)
    half2.loc['sum'] = half2.sum( min_count=1)
    print half2 
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print half2
    print 


    print


    #with pd.option_context('display.max_rows', None, 'display.max_columns', 8):
    #    print q4
    #print 

    #y = data_fetcher.get_annual_income(code , "2017")
    #print y.loc[0, ['statDate', 'net_profit' ] ]
    
    #with pd.option_context('display.max_rows', None, 'display.max_columns', 8):
    #    print y
    #print 



def do_some_experiment(engine):
    show_wanke_2017_income()

# 试验场
def handle_exper( argv, argv0  ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
     
        do_some_experiment(engine)

    except  Exception as e:
        
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0


