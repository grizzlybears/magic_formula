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

# 其他第三方包
import  jqdatasdk as jq

# 我们的代码
import data_struct 
import db_operator
import data_fetcher
import util
import plotter


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

FH_BASE_CODE  = '000016.XSHG'
FH_BASE_NAME  = '上证50'


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

    global MF_NetValue
    MF_NetValue = MF_NetValue * ( total_s  / total_b )


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
    MF_NetValue = MF_NetValue * ( total_s  / total_b )


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


def fetch_index_compo_dailyline_1_day(engine, index_code,t_day):
    # 取该日指数成份
    compo_list = jq.get_index_stocks( index_code, date = t_day)

    # 抓行情
    pn = jq.get_price(compo_list 
            , start_date= t_day, end_date=t_day
            , frequency='daily'
            , fields=['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused']
            , skip_paused=False
            , fq='pre'
            )

    #print pn

    df_open  = pn['open']
    df_close = pn['close']
    df_high  = pn['high']
    df_low   = pn['low']
    df_volume = pn['volume']
    df_money  = pn['money']
    df_high_limit = pn['high_limit']
    df_low_limit  = pn['low_limit']
    df_pre_close  = pn['pre_close']
    df_paused     = pn['paused']

    for one_compo in compo_list:
        print "%s, %s : open=%f close=%f paused=%d" % (t_day, one_compo, df_open[one_compo].iloc[0],df_close[one_compo].iloc[0] ,  df_paused[one_compo].iloc[0]   )

        db_operator.db_save_dailyline(engine
                , one_compo 
                , t_day
                , df_open[one_compo].iloc[0]
                , df_close[one_compo].iloc[0]
                , df_high[one_compo].iloc[0]
                , df_low[one_compo].iloc[0]
                , df_volume[one_compo].iloc[0]
                , df_money[one_compo].iloc[0]
                , df_high_limit[one_compo].iloc[0]
                , df_low_limit[one_compo].iloc[0]
                , df_pre_close[one_compo].iloc[0]
                , df_paused[one_compo].iloc[0]
                )

    print

def fetch_index_dailyline_until_now(engine, index_code ,start_year):
    now = datetime.now()

    
    #1. 抓指数自身的日线
    t_start  = "%d-01-01" % start_year 
    df_50_his = data_fetcher.get_daily_line( index_code , t_start, now)
    db_operator.save_daily_line_to_db( engine, index_code , df_50_his) 

    #2. 抓指数成份股的日线
    all_t_day = df_50_his['t_day']
    for one_day in all_t_day:
        # 成份列表有可能发生临时调整，只能每天都抓
        fetch_index_compo_dailyline_1_day(engine, index_code, one_day)
        


def backtest_until_now(engine, start_year):
    
    now = datetime.now()

    df_50_his = data_fetcher.get_daily_line()

    for y in range( start_year, now.year + 1):
        backtest_1_year( engine, y)

# 获得‘可并列’的名次。
#   indices:  [ (第1名code, 第1名指标数组) , (第2名code, 第2名指标数组), ....  ]
#
#   WHICH_INDI: 取指标数组里哪一个指标?
#
#  返回 (可并列的名次,  sorted_indices 中的下标 )
def get_rank(code, sorted_indices, WHICH_INDI):
    pos = 0  #可‘并列’的名次

    last_indi = 10000
    for i,walker in enumerate(sorted_indices, start = 1):
        if walker[1][WHICH_INDI] != last_indi:
            pos = i
            last_indi = walker[1][WHICH_INDI]

        if walker[0] == code:
            return  pos, i-1

    raise Exception("%s不在昨日行情中。" % code );


# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# 其中‘指标’ 是  [可买标志，三日累计涨幅]

def make_indices_by_delta( conn, his_md ):

    prev_md = collections.OrderedDict ()

    for i, md_that_day  in enumerate(his_md):
        indices = collections.OrderedDict ()

        for code,md_set in md_that_day[1].iteritems():
            #if 0 == i:
            #    print "%s " % code 
            indices_for_1_sec = []

            can_buy = 1
            if i<=1:
                # 前两行之内肯定凑不满三日涨幅，只能看着
                can_buy = 0
            elif md_set[3] or md_set[4] :
                # 涨停或者停牌的不能买
                can_buy = 0 
            
            if not can_buy:
                # 不必做其他指标了
                indices_for_1_sec= [ 0 , 0 ]
                indices[code] = indices_for_1_sec

                continue

            #前天的行情
            md_2days_ago_allmd = his_md[i - 2][1]

            if code in md_2days_ago_allmd:
                # 可以计算三日涨幅
                md_this_code_2days_ago = md_2days_ago_allmd[code]
                
                #前天 的 前日收盘
                close_3d_ago = md_this_code_2days_ago[1]
                if close_3d_ago  is not None and close_3d_ago  != 0:

                    delta = md_set[0] - close_3d_ago  
                    #       本日收盘    #前天 的 前日收盘

                    delta_r = delta / close_3d_ago 
                    
                    indices_for_1_sec= [1 ,  delta_r  ]
                else:
                    indices_for_1_sec= [0 , 0  ]

            else:
                # 三日前该code还未纳入指数，不能买
                indices_for_1_sec= [0, 0 ]

            indices[code] = indices_for_1_sec
        
        #if 0 == i:
        #    print " 以上做指标 \n" 

        md_that_day.append( indices)

    return his_md


# ‘指标’数组:  [可买标志, MA(涨幅, $MA_Size1) ]      
def make_indices_by_MA_delta( conn,  his_md,MA_Size1 = 5):
    
    last_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价，前日收盘，涨幅， 涨停标志], 
                   #                    [交易日，收盘价，前日收盘，涨幅， 涨停标志], ... 
                   #                ]




INITIAL_BALANCE = 10000.0  # 策略期初金额
TRADE_COST      = 0.0003   # 手续费万三 
TRADE_TAX       = 0.0001   # 印花税千1单向

# 简单的轮换策略：
#     根据指标(目前是3日涨幅)从高到底排名。
#     前M名如果>0，则各给1/M的仓位。
#     卖出标准是指标名次低于sell_threshold 或者 指标<0.
#     有空仓则依据建仓标准补进仓位。
# Input:  2-D array 'md_his'
#         日期   各脚行情  各脚指标
#         ...
#
# Output: 2-D array , 交易数， 交易成本
#         日期  基准收盘价   策略净值 交易次数  换仓详细  
#         ...
#
def sim_rotate( his_data,  max_hold, base_code, start_day = "", end_day = ""):    
    sell_threshold = max_hold + 2

    if len(his_data) == 0:
        raise Exception("没有行情历史数据。"  );

    result = []
    trans_num = 0 
    trans_cost = 0.0
    
    sec_num = len( his_data[0][1])

    hold_num = 0

    we_hold =  data_struct.make_init_shares(INITIAL_BALANCE, max_hold)  # 我们的持仓

    for i, row in enumerate(his_data):

        t_day = row[0]
        #print "T_Day %s,  we hold %s" % (row[0], we_hold)

        md_that_day      = row[1]   #当日行情    
        indices_that_day = row[2]   #当日指标   

        if i == 0 :
            # 第一天，没有操作 ，也没有损益
            r_that_day = []
            r_that_day.append( t_day )
            
            md_of_base = md_that_day[base_code]
            
            r_that_day = [t_day, md_of_base[0],  INITIAL_BALANCE, None      ,None ]
            #                    基准收盘价      策略净值         换仓提示   换仓明细
 
            result.append( r_that_day )

            continue 
        
        if "" != start_day and t_day < start_day:
            # 略过
            continue

        if "" != end_day and t_day >= end_day:
            break
     
        
        # 昨日本策略的收盘价
        if len(result) > 0:
            y_policy  = result[ len(result) - 1 ][2]    
        else:
            y_policy = INITIAL_BALANCE 

        # 这里有一个近似的假设：
        # 我们可以基于昨日的指标，按照昨日的收盘价，进行操作(记作今日操作)，并把操作的损益反映于今日。
        
        #昨日行情
        y_md      =  his_data[i - 1][1]

        #昨日指标  {code1:指标数组1, code2:指标数组2, ... }
        y_indices =  his_data[i - 1][2]

# 简单的轮换策略：
#     根据昨日涨幅从从高到底排名。
#     前M名如果>0，则各给1/M的仓位。
        # 指标数组:  [可买标志，三日累计涨幅] 
        WHICH_INDI = 1 # 我们取指标数组里哪一个指标?
        sorted_y_indices = sorted ( y_indices.items(), key=lambda sec:sec[1][WHICH_INDI], reverse=True)

        to_hold = []   # 继续持仓的份额编号
        to_sell = []   # 要卖出的份额编号
        to_buy  = []   # 要买进的代码

        #print "%s:" % t_day 
        #print we_hold 

        # 撸一遍我们的持仓，看看有哪些要持有，哪些要卖
        for one_hold in we_hold.pos_entries:
            
            if one_hold.is_blank():
                continue

            if one_hold.code not in md_that_day:
                # 当日该code已经不在指数成份里
                to_sell.append( one_hold.seq)
                continue
                

            rank, pos = get_rank(one_hold.code, sorted_y_indices, WHICH_INDI)   #可‘并列’的名次
            
            y_indices_of_we_hold = sorted_y_indices[pos][1]  #该持仓代码的昨日指标

            if rank <= sell_threshold and y_indices_of_we_hold[WHICH_INDI] > 0:
                to_hold.append( one_hold.seq)
            else:
                to_sell.append( one_hold.seq)

        to_hold_codes = we_hold.get_codes_from_holds(  to_hold)

        # 撸一遍昨日M强，看看有哪些要买进
        max_buy = max_hold - len(to_hold) 
        for code,indi  in sorted_y_indices:
            if max_buy <=0 :
                break
            
            if indi[WHICH_INDI] <= 0 :
                break
            
            if code in to_hold_codes  or not indi[0]:
            #                         可买标志  
                continue
            
            if code not in md_that_day:
                # 当日该code已经不在指数成份里
                continue

            to_buy.append(code)
            max_buy = max_buy - 1

        to_sell_codes = we_hold.get_codes_from_holds( to_sell)
        
        op_num = len(to_buy) + len(to_sell)
        blank_num = max_hold - len(to_hold) - len(to_buy)
        
        hold_num = hold_num + len(to_hold)

        #print util.build_p_hint( t_day,  to_hold_codes ,  to_sell_codes,  to_buy  )
        assert blank_num >= 0 

        # 开始调整we_hold  估算当日的净值
        
        for one_pos in we_hold.pos_entries:
            if one_pos.seq in to_sell:
                # 要卖掉
                
                # ‘行情’ 是  [收盘价，前日收盘，涨幅， 涨停标志，停牌标志]
                trade_price = y_md[one_pos.code ][0]  #  FIXME: 如果该code停牌，这里需要寻找到其复牌价
                
                trade_amount = one_pos.volumn * trade_price
                trade_loss   = trade_amount *  ( TRADE_COST + TRADE_TAX) 

                one_pos.code = ""
                one_pos.volumn = 0
                one_pos.cost_price = 0
                one_pos.now_price  = 0
                
                we_hold.remaining = we_hold.remaining + trade_amount - trade_loss 
                trans_cost  = trans_cost + trade_loss

                continue
            elif one_pos.seq in to_hold:
                # 更新一下价格
                one_pos.now_price = md_that_day[one_pos.code ][0] 


        # 买进操作
        if len(to_buy):
            each = we_hold.remaining / we_hold.get_blank_num()

        for one_buy in to_buy:
            pos = we_hold.find_first_blank_pos()
            assert  pos

            trade_price = y_md[one_buy][0]

            trade_volumn =  int( each  / ( trade_price * (1 + TRADE_COST )))
            trade_amount =  trade_volumn * trade_price 
            trade_loss   =  trade_amount *  TRADE_COST 
            
            pos.code   = one_buy
            pos.volumn = trade_volumn  
            pos.cost_price = trade_price 
            pos.now_price  = md_that_day[one_buy ][0]
                
            we_hold.remaining = we_hold.remaining - trade_amount - trade_loss 
            trans_cost  = trans_cost + trade_loss

#       日期  基准收盘价   策略净值 交易次数  换仓详细  
  
        base_price = md_that_day[base_code][0]
        t_policy = we_hold.get_value()  
        op_num_text = "%d" % op_num
        t_hint = util.build_t_hint(t_day
                , to_sell_codes  
                , to_buy )

        r_that_day= [ t_day, base_price, t_policy ,op_num_text, t_hint ]
        result.append( r_that_day )

        #print r_that_day
        #print we_hold
        #print 

        trans_num = trans_num + op_num 

    print "持有次数 %d" % hold_num

    return (result ,  trans_num , trans_cost )


def fh50_until_now(engine, start_year):
    
    now = datetime.now()

    #从DB抓日线数据
    start_day = "%d-01-01" % start_year

    conn = engine.connect()

    # 获取日线数据
# 返回数组
#     T_day1,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day2,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day3,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘，涨幅， 涨停标志，停牌标志]

    his_md = db_operator.db_fetch_dailyline(conn, start_day )

    # 在日线数据中，扩充加入指标数据
# 返回时，数组md_his_data扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的行情, 证券2:证券2的行情, ... }
#     ...

    # ‘指标’ 是  [可买标志，三日累计涨幅]
    make_indices_by_delta( conn,  his_md )
    
    #util.bp_as_json( his_md)
    #util.bp( his_md)

    result, trans_num, trans_cost  = sim_rotate( his_md, 3 , FH_BASE_CODE )    
# Output: 2-D array , 交易数， 交易成本
#         日期  基准收盘价   策略净值 交易次数  换仓详细  
#         ...
#

    #util.bp( result)
    #准备画图
    base_info = data_struct.SecurityInfo()
    base_info.code = FH_BASE_CODE
    base_info.name = FH_BASE_NAME

    secs = [ base_info ]

    suffix = ".from_%d" % start_year 

    plotter.generate_htm_chart_for_faster_horse2( secs, result , suffix)
 
    #show summary
    t_day_num = len(result)
    base_delta   = result[ t_day_num - 1][1] / result[ 0][1]
    policy_delta = result[ t_day_num - 1][2] / result[ 0][2]

    print "%s ~ %s, %d个交易日，交易%d笔，交易成本%f，基准表现%f，策略表现%f" % (
            result[0][0], result[ t_day_num - 1][0], t_day_num
            , trans_num,  trans_cost
            , base_delta, policy_delta 
            )


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
    #show_wanke_2017_income()
    
    a = data_fetcher.get_his_until( FH_BASE_CODE   , '2015-08-12', 5)
    util.bp(a)


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

# 处理 'fetch50' 子命令
def handle_fetch50( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2005  # 上证50从 2004年才开始有

        i = len(argv)
        if ( 1 == i  ):
            start_year = int(argv[0])
        else:
            now = datetime.now()
            start_year = now.year - 1

        if start_year < 2005:
            print "开始年份必须不小于2005"
            return 1

        fetch_index_dailyline_until_now(engine,FH_BASE_CODE , start_year)


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

# 处理 'fh50' 子命令 -- 回测50指数成份骑快马策略 
def handle_fh50( argv, argv0 ): 
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

        fh50_until_now(engine, start_year)


    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0


