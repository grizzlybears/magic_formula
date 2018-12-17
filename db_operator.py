## -*- coding: utf-8 -*-

# py系统包
from datetime import date
from datetime import datetime

import sqlite3
import subprocess
import collections
import math

# 著名扩展包
import  pandas as pd
from   sqlalchemy import create_engine
from   sqlalchemy.sql  import select as alch_select
from   sqlalchemy.sql  import text   as alch_text
from   sqlalchemy    import  MetaData 

# 其他第三方包
import to_sql_newrows as nr

# 我们的代码
import data_struct 
import util

s_metadata = None

# 打开DB，返回 sqlalchemy 的 db engine 
def get_db_engine():
    #engine = create_engine( "sqlite:///%s" % data_struct.DB_PATH, echo=True)
    engine = create_engine( "sqlite:///%s" % data_struct.DB_PATH)
    
    # Create a MetaData instance
    global s_metadata 
    s_metadata = MetaData( engine, reflect=True)

    # reflect db schema to MetaData
    # s_metadata.reflect(bind=engine)

    #print s_metadata.tables

    return engine 

def create_valuation_table(conn):
    sql= '''
CREATE TABLE IF NOT EXISTS "Valuation" (
    id BIGINT, 
    code TEXT, 
    pe_ratio FLOAT, 
    turnover_ratio FLOAT, 
    pb_ratio FLOAT, 
    ps_ratio FLOAT, 
    pcf_ratio FLOAT, 
    capitalization FLOAT, 
    market_cap FLOAT, 
    circulating_cap FLOAT, 
    circulating_market_cap FLOAT, 
    day TEXT, 
    pe_ratio_lyr FLOAT
    
    , PRIMARY KEY( id)
    );
    '''
    
    conn.execute( sql) 

def create_balance_table(conn):
    sql = ''' CREATE TABLE IF NOT EXISTS BalanceSheetDay (
       id  BIGINT
       , code   TEXT 
       , day    TEXT
       , pubDate TEXT
       , statDate TEXT
       , "statDate.1" TEXT
       , cash_equivalents  NUMERIC
       , settlement_provi  NUMERIC
       , lend_capital      NUMERIC
       , trading_assets    NUMERIC
       , bill_receivable   NUMERIC
       , account_receivable NUMERIC
       , advance_payment    NUMBER 
       , insurance_receivables     NUMERIC
       , reinsurance_receivables    NUMERIC
       , reinsurance_contract_reserves_receivable    NUMERIC
       , interest_receivable    NUMERIC
       , dividend_receivable     NUMERIC
       , other_receivable        NUMERIC
       , bought_sellback_assets      NUMERIC
       , inventories               NUMERIC
       , non_current_asset_in_one_year     NUMERIC
       , other_current_assets         NUMERIC
       , total_current_assets         NUMERIC
       , loan_and_advance             NUMERIC
       , hold_for_sale_assets         NUMERIC
       , hold_to_maturity_investments        NUMERIC
       , longterm_receivable_account         NUMERIC
       , longterm_equity_invest              NUMERIC
       , investment_property                 NUMERIC
       , fixed_assets                        NUMERIC
       , constru_in_process                  NUMERIC
       , construction_materials              NUMERIC
       , fixed_assets_liquidation            NUMERIC
       , biological_assets                   NUMERIC
       , oil_gas_assets                      NUMERIC
       , intangible_assets                   NUMERIC
       , development_expenditure             NUMERIC
       , good_will                           NUMERIC
       , long_deferred_expense               NUMERIC
       , deferred_tax_assets                 NUMERIC
       , other_non_current_assets            NUMERIC
       , total_non_current_assets            NUMERIC
       , total_assets                        NUMERIC
       , shortterm_loan                      NUMERIC
       , borrowing_from_centralbank          NUMERIC
       , deposit_in_interbank                NUMERIC
       , borrowing_capital                   NUMERIC
       , trading_liability    NUMERIC
       , notes_payable        NUMERIC 
       , accounts_payable     NUMERIC 
       , advance_peceipts     NUMERIC 
       , sold_buyback_secu_proceeds    NUMERIC 
       , commission_payable       NUMERIC 
       , salaries_payable         NUMERIC 
       , taxs_payable             NUMERIC 
       , interest_payable    NUMERIC 
       , dividend_payable    NUMERIC 
       , other_payable       NUMERIC 
       , reinsurance_payables NUMERIC 
       , insurance_contract_reserves  NUMERIC
       , proxy_secu_proceeds    NUMERIC
       , receivings_from_vicariously_sold_securities NUMERIC
       , non_current_liability_in_one_year NUMERIC
       , other_current_liability NUMERIC 
       , total_current_liability NUMERIC 
       , longterm_loan    NUMERIC 
       , bonds_payable    NUMERIC 
       , longterm_account_payable   NUMERIC 
       , specific_account_payable   NUMERIC 
       , estimate_liability         NUMERIC 
       , deferred_tax_liability     NUMERIC 
       , other_non_current_liability NUMERIC 
       , total_non_current_liability NUMERIC
       , total_liability    NUMERIC
       , paidin_capital     NUMERIC
       , capital_reserve_fund NUMERIC 
       , treasury_stock       NUMERIC 
       , specific_reserves    NUMERIC 
       , surplus_reserve_fund NUMERIC 
       , ordinary_risk_reserve_fund  NUMERIC 
       , retained_profit      NUMERIC 
       , foreign_currency_report_conv_diff NUMERIC 
       , equities_parent_company_owners    NUMERIC
       , minority_interests    NUMERIC
       , total_owner_equities  NUMERIC
       , total_sheet_owner_equities NUMERIC

       , PRIMARY KEY( id)
       )
    '''
    conn.execute( sql) 
    

def create_income_table(conn):
    sql= '''
CREATE TABLE  IF NOT EXISTS "Income" (
    id BIGINT, 
    code TEXT, 
    "statDate" TEXT, 
    "pubDate" TEXT, 
    "statDate.1" TEXT, 
    total_operating_revenue FLOAT, 
    operating_revenue FLOAT, 
    interest_income FLOAT, 
    premiums_earned FLOAT, 
    commission_income FLOAT, 
    total_operating_cost FLOAT, 
    operating_cost FLOAT, 
    interest_expense FLOAT, 
    commission_expense FLOAT, 
    refunded_premiums FLOAT, 
    net_pay_insurance_claims FLOAT, 
    withdraw_insurance_contract_reserve FLOAT, 
    policy_dividend_payout FLOAT, 
    reinsurance_cost FLOAT, 
    operating_tax_surcharges FLOAT, 
    sale_expense FLOAT, 
    administration_expense FLOAT, 
    financial_expense FLOAT, 
    asset_impairment_loss FLOAT, 
    fair_value_variable_income FLOAT, 
    investment_income FLOAT, 
    invest_income_associates FLOAT, 
    exchange_income FLOAT, 
    operating_profit FLOAT, 
    non_operating_revenue FLOAT, 
    non_operating_expense FLOAT, 
    disposal_loss_non_current_liability FLOAT, 
    total_profit FLOAT, 
    income_tax_expense FLOAT, 
    net_profit FLOAT, 
    np_parent_company_owners FLOAT, 
    minority_profit FLOAT, 
    basic_eps FLOAT, 
    diluted_eps FLOAT, 
    other_composite_income FLOAT, 
    total_composite_income FLOAT, 
    ci_parent_company_owners FLOAT, 
    ci_minority_owners FLOAT
    
    , PRIMARY KEY( id)
);

    '''
    
    conn.execute( sql) 

def create_daily_line_table(conn):
    sql= '''
CREATE TABLE if not exists "DailyLine" (
    code TEXT, 
    t_day TEXT, 
    open FLOAT, 
    close FLOAT, 
    high FLOAT, 
    low FLOAT, 
    volume FLOAT, 
    money FLOAT,
    high_limit FLOAT,
    low_limit  FLOAT,
    pre_close  FLOAT,
    paused     INT
    , PRIMARY KEY( code, t_day)
);
    '''
    conn.execute( sql) 

#记录某天某股票是否停牌
def create_pause_table(conn):
    sql= '''
CREATE TABLE if not exists "IsPaused" (
    code TEXT, 
    t_day TEXT, 
    is_paused integer
    , PRIMARY KEY( code, t_day)
);
    '''
    conn.execute( sql) 

# 净运营资本，固定资产，带息负债, 少数股东权益
def create_vMagicBalanace(conn):
    sql= '''
create view if not exists vMagicBalance
as 
select code,statDate
    , ifnull(account_receivable,0) + ifnull(other_receivable,0) + ifnull(advance_payment,0) + ifnull(inventories,0) - ifnull(notes_payable,0) - ifnull(accounts_payable,0) - ifnull(advance_peceipts,0) - ifnull(taxs_payable,0) - ifnull(interest_payable,0) -  ifnull(other_payable,0) - ifnull(other_current_liability,0) as net_op_cap
    , fixed_assets
    , ifnull(shortterm_loan,0) + ifnull(non_current_liability_in_one_year,0) + ifnull(longterm_loan,0) + ifnull(bonds_payable,0) + ifnull(longterm_account_payable,0) as nonfree_liability
    , ifnull(minority_interests,0) as minority_interests
from BalanceSheetDay
    '''
    conn.execute( sql) 

#记录成份列表
def create_composition_list(conn):
    sql= '''
CREATE TABLE if not exists "CompositionList" (
    year  Integer,
    month Integer,
    code  TEXT, 
    t_day TEXT, 
    name  TEXT,
    ROC   float,
    EY    float,
    rank_roc integer,
    rank_ey  integer,
    rank_final integer
    , PRIMARY KEY( year, month, code)
);
    '''
    conn.execute( sql) 

# 模拟交易
def create_simu_trade(conn):
    sql= '''
CREATE TABLE if not exists "SimuTrade" (
    year  Integer,
    month Integer,
    code  TEXT, 
    name  TEXT,
    t_day1     TEXT, 
    direction1 integer,
    volumn1    integer,
    price1     float,
    amount1    float,
    fee1       float,
    t_day2     TEXT, 
    direction2 integer,
    volumn2    integer,
    price2     float,
    amount2    float,
    fee2       float,
    profit_r   float
);
    '''
    conn.execute( sql) 

    sql= '''
CREATE unique index if not exists "ind_SimuTrade" on SimuTrade (
    year, 
    month, 
    code
);
    '''
    conn.execute( sql) 




 # 打开DB，并酌情建表，返回 sqlite3.Connection
def get_db_conn():
    conn = sqlite3.connect( data_struct.DB_PATH )
    conn.text_factory = str
 
    create_balance_table( conn )
    create_valuation_table(conn)
    create_income_table(conn)
    create_daily_line_table(conn)
    create_pause_table(conn)
    create_composition_list(conn)

    create_vMagicBalanace(conn)

    create_simu_trade(conn)

    conn.commit()

    return conn 


    
def save_balance_df_to_db(engine, dataframe ):
    #pd.set_option('display.max_columns', 200)

    # a = dataframe.drop( 'statDate.1' ,  axis =1 )  # 这列文档里没有

    #pd.reset_option('display.max_columns')


    a = nr.clean_df_db_dups(dataframe ,'BalanceSheetDay', engine, ['id'] ) 

    a.to_sql( 'BalanceSheetDay', con = engine , index=False, if_exists='append')

def save_valuation_df_to_db(engine, dataframe ): 
    
    if dataframe is None:
        return

    #pd.set_option('display.max_columns', 200)

    #pd.reset_option('display.max_columns')

    a = nr.clean_df_db_dups(dataframe, 'Valuation', engine, ['id'] ) 
    a.to_sql( 'Valuation', con = engine , index=False, if_exists='append')


def save_income_df_to_db(engine, dataframe ):
    #pd.set_option('display.max_columns', 200)

    #pd.reset_option('display.max_columns')

    a = nr.clean_df_db_dups(dataframe, 'Income', engine, ['id'] ) 
    a.to_sql( 'Income', con = engine , index=False, if_exists='append')


def date_only(dt):
    s = str(dt)[:10]
    return s
   

def save_daily_line_to_db(engine, code, dataframe ):
    #pd.set_option('display.max_columns', 200)

    #pd.reset_option('display.max_columns')
    if dataframe is None:
        return

    a= dataframe 
    a.insert(0, 'code', code) # 加一列'code'，都设为code
    a['t_day'] = a.index      # 加一列't_day'，设为该行的index，也就是 datetime64
    a['t_day'] = a['t_day'].apply(date_only)    # 对于所有行的't_day'列，执行一次'date_only'函数

    #print a
    a = nr.clean_df_db_dups(a , 'DailyLine', engine, ['code', 't_day'] ) 
    a.to_sql( 'DailyLine', con = engine , index=False, if_exists='append')


def save_df_to_db_table(engine, dataframe, tablename ):
    dataframe.to_sql( tablename, con = engine , if_exists='append')


def query_paused(engine, code, t_day ):

    yyyymmdd= "%d-%02d-%02d" % (t_day.year , t_day.month, t_day.day)

    conn = engine.connect()

    s = alch_text(
            '''
            select is_paused from IsPaused
            where code = :a and t_day = :b
            '''
            )

    r = conn.execute( s, a = code, b = yyyymmdd).fetchall()
    
    if len(r) ==0 :
        return None

    return r[0][0]

def record_paused(engine, code, t_day, is_paused ):

    yyyymmdd= "%d-%02d-%02d" % (t_day.year , t_day.month, t_day.day)

    conn = engine.connect()

    global s_metadata 
    #print s_metadata.tables

    T_IsPaused = s_metadata.tables['IsPaused']

    ins = T_IsPaused.insert().values(code = code, t_day = yyyymmdd, is_paused = is_paused)

    r = conn.execute( ins )

def db_save_dailyline(engine, code, t_day, open_,close_,high, low,volume,money, high_limit, low_limit, pre_close, paused ):

    conn = engine.connect()

    global s_metadata 
    #print s_metadata.tables

    T_Dailyline = s_metadata.tables['DailyLine']
 
    trans = conn.begin()
    try:
        s = alch_text(
            '''
            delete from DailyLine 
            where t_day  = :t and code = :c 
            '''
            )

        conn.execute( s, t  = t_day, c = code)
 
        ins = T_Dailyline.insert().values(
            code = code
            , t_day = t_day
            , open = open_ 
            , close = close_ 
            , high = high
            , low = low
            , volume = volume
            , money = money
            , high_limit = high_limit
            , low_limit  = low_limit
            , pre_close  = pre_close
            , paused = paused
            )
        r = conn.execute( ins )
     
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

def query_balancesheet(engine, code, statDate ):

    conn = engine.connect()

    s = alch_text(
            '''
            select * from BalanceSheetDay
            where code = :a and statDate = :b
            '''
            )

    r = conn.execute( s, a = code, b = statDate ).fetchall()
    
    #print r

    if len(r) ==0 :
        return None

    return r

def record_balance_df_to_db(engine, df ):
    if df is None:
        return

    df.to_sql( 'BalanceSheetDay', con = engine , index=False, if_exists='append')

def query_income(engine, code, statDate ):

    conn = engine.connect()

    s = alch_text(
            '''
            select * from Income
            where code = :a and statDate = :b
            '''
            )

    r = conn.execute( s, a = code, b = statDate ).fetchall()
    
    #print r

    if len(r) ==0 :
        return None

    return r

def record_income_df_to_db(engine, df ):

    if df is None:
        return

    df.to_sql( 'Income', con = engine , index=False, if_exists='append')

def query_valuation(engine, code, t_day ):

    conn = engine.connect()

    s = alch_text(
            '''
            select * from Valuation
            where code = :a and day = :b
            '''
            )

    r = conn.execute( s, a = code, b = t_day ).fetchall()
    
    #print r

    if len(r) ==0 :
        return None

    return r

def record_valuation_df_to_db(engine, df ):
 
    if df is None:
        return

    df.to_sql( 'Valuation', con = engine , index=False, if_exists='append')


def create_tmp_EBIT( conn,  start_date, end_date ):
    trans = conn.begin()
    try:
        conn.execute("drop table if exists tmpEBIT" )

        sql ='''
create temp table tmpEBIT 
as 
select code, sum( ifnull(net_profit,0) + ifnull(income_tax_expense,0) + ifnull( financial_expense , 0))  as EBIT
from Income
where '%s' <=statDate  and statDate <= '%s'
group by code 
''' % ( start_date, end_date  )
        
        conn.execute(sql)
        trans.commit()
    except:
        trans.rollback()
        raise Exception("清理/创建 tmpEDIT 失败")

def db_fetch_stock_statements(conn,stat_end , t_day, y, m ):
    ymd = '%d-%02d-%02d' % (t_day.year, t_day.month, t_day.day  )
    
    s = '''
select m.*,e.EBIT,v.market_cap
from tmpEBIT e
join vMagicBalance m on ( e.code = m.code and m.statDate= '%s' )
join Valuation v on ( e.code = v.code and v.day = '%s' )
            '''  % ( stat_end , ymd )

    #print s

    r = conn.execute( alch_text(s) ).fetchall()

    #util.bp(r)
    # 代码, 报表期末日,净运营资本，固定资产，有息负债，少数股东权益，EBIT，市值
    # 0     1          2           3         4         5             6     7

    sci_list = []

    for row in r:
        sci = data_struct.StockCandidatorInfo()
        sci.year  = y
        sci.month = m
        sci.stat_end = stat_end 
        sci.code          =  row[0]
        sci.net_op_cap    =  row[2]
        sci.fixed_assets  =  row[3]
        sci.nonfree_liability   = row[4]
        sci.minority_interests  = row[5]
        sci.EBIT       = row[6]
        sci.market_cap = row[7] 

        if sci.net_op_cap is None:
            print "WARN: %s 净运营资本为空，忽略" % sci.code
            continue
    
        if sci.fixed_assets is None:
            print "WARN: %s 固定资产为空，忽略" % sci.code
            continue
    
        # 指标一：ROC = EBIT /（净营运资本 + 固定资产）
        a = sci.net_op_cap + sci.fixed_assets 
        if 0 == a:
            print "WARN!!! %s 的'运营资本+固定资产'为0， 指标1 做 0 处理" % sci.code
            sci.ROC = 0
        else:
            sci.ROC = sci.EBIT / a

        # 指标二：EY = EBIT /（总市值 + 带息负债 + 其他权益工具 + 少数股东权益） 
        
        if sci.market_cap  is None:
            print "WARN: %s 总市值为空，忽略" % sci.code
            continue
    
        if sci.nonfree_liability  is None:
            print "WARN: %s 有息负债为空，忽略" % sci.code
            continue 

        if sci.minority_interests   is None:
            print "WARN: %s 少数股东权益为空，忽略" % sci.code
            continue
    
        b = sci.market_cap * 100000000 + sci.nonfree_liability + sci.minority_interests 
        sci.EY = sci.EBIT / b

        sci_list.append(sci)

    return sci_list

def db_save_composition_entry(conn, y, compo_m, t_day,  entry):
    
    global s_metadata 

    T_CompositionList = s_metadata.tables['CompositionList']

    # entry 来自于  db_fetch_stock_statements

    ins = T_CompositionList.insert().values( \
            year = y
            , month = compo_m
            , code = entry.code
            , t_day = t_day
            , name = entry.name
            , ROC = entry.ROC
            , EY  = entry.EY
            , rank_roc = entry.rank_roc
            , rank_ey  = entry.rank_ey
            , rank_final = entry.rank_final 
            )

    r = conn.execute( ins )


def db_save_composition_list(conn, year, compo_m, t_day,  composition_list):
    trans = conn.begin()
    try:
        s = alch_text(
            '''
            delete from CompositionList
            where year = :y and month = :m 
            '''
            )

        conn.execute( s, y  = year, m = compo_m  )
 
        for entry in composition_list:
            db_save_composition_entry( conn, year, compo_m , t_day, entry)
            
        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

def db_fetch_composition_list(conn, y, m ):
    
    s = '''
select code, name, t_day
from CompositionList 
where year = %d and month = %d
order by code
            '''  % ( y , m )

    r = conn.execute( alch_text(s) ).fetchall()

    # 代码, 名称，交易日
    # 0     1          2

    tr_list = []

    for row in r:
        tr = data_struct.TradeRecord()

        tr.code  = row[0]
        tr.name  = row[1]
        tr.t_day = row[2]
        
        tr_list.append( tr )

    return tr_list

def db_save_simu_trade_entry(conn, y, m, buy_entry,  sell_entry ): 
    global s_metadata 

    T_SimuTrade = s_metadata.tables['SimuTrade']

    # entry 来自于  db_fetch_stock_statements

    pr= (sell_entry.amount - buy_entry.amount) / buy_entry.amount 

    ins = T_SimuTrade.insert().values( \
            year = y
            , month = m
            , code =  buy_entry.code
            , name =  buy_entry.name
            , t_day1 = buy_entry.t_day
            , direction1 = buy_entry.direction 
            , volumn1    = buy_entry.volumn 
            , price1     = buy_entry.price 
            , amount1    = buy_entry.amount
            , fee1       = buy_entry.fee 
            , t_day2     = sell_entry.t_day
            , direction2 = sell_entry.direction 
            , volumn2    = sell_entry.volumn 
            , price2     = sell_entry.price 
            , amount2    = sell_entry.amount
            , fee2       = sell_entry.fee 
            , profit_r   = pr
            )

    r = conn.execute( ins )


    return 

def db_save_simu_trade_list(conn, year, month, buy_list, sell_list ): 
    trans = conn.begin()
    try:
        s = alch_text(
            '''
            delete from SimuTrade
            where year = :y and month = :m 
            '''
            )

        conn.execute( s, y  = year, m = month  )
 
        lb = len(buy_list)
        ls = len(sell_list)

        if lb != ls:
            raise Exception("Error %年%月，买%d条，卖%d条！" \
                    , year, month 
                    , lb, ls
                    )

        for i in range(lb):
            b = buy_list[i]
            s = sell_list[i]

            if b.code != s.code :
                raise Exception ("Error %年%月，第%d行, 买%s，卖%s！" \
                    , year, month 
                    , i
                    , b.code, s.code
                    )

            db_save_simu_trade_entry(conn, year, month, b ,s )
            

        trans.commit()
    except Exception as e:
        trans.rollback()
        raise e

    return


# 返回数组
#     T_day1,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day2,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day3,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     ...
# 其中‘行情’ 是  [收盘价，涨幅， 涨停标志，停牌标志]

# 注：   由于成份会变化，无法‘对数化’

def db_fetch_dailyline(conn, start_day ):
    
    s = '''
select t_day, code, close, high_limit, pre_close, paused
from DailyLine
where t_day >=  '%s' 
order by t_day asc, code asc 
            '''  % ( start_day )

    r = conn.execute( alch_text(s) ).fetchall()

    # 交易日, 代码，收盘， 涨停价， 前日收盘， 停牌标志
    # 0       1     2      3        4          5


    row_num = 0
    his_md = []
    last_t_day = ''

    md_of_1_day = collections.OrderedDict ()
    
    for row in r:

        t_day = row[0]
        code  = row[1]
        close = row[2]

        if t_day != last_t_day:
            # 新的一日行情开始
            is_new_t_day = True
            if len(md_of_1_day) > 0:
                his_md.append( [ last_t_day ,  md_of_1_day ] )
                md_of_1_day = collections.OrderedDict ()

            last_t_day = t_day
        else:
            # 对当前日行情，填入新的代码
            is_new_t_day = False 
 
       
        if (row[3] - close ) < 0.01:
            close_on_ceil = 1 
        else:
            close_on_ceil = 0

        pre_close = row[4]
        if pre_close is not None and pre_close != 0:
            delta_r = (close - pre_close) / pre_close
        else:
            delta_r = 0

        paused = row[5]
            
        
        md_of_1_sec  = [close,delta_r, close_on_ceil, paused]   # 收盘价，涨幅， 涨停，停牌
        md_of_1_day[code] = md_of_1_sec 

    
    his_md.append( [ last_t_day ,  md_of_1_day ] )
 
    return his_md


