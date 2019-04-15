# -*- coding: utf-8 -*-
import sys
import site
import traceback

import  jqdatasdk as jq
import  pandas as pd
import  numpy  as np
from datetime import date,datetime,timedelta

import db_operator
import data_struct 
import jq_acc

def is_paused(engine, code, t_day ):

    q = db_operator.query_paused( engine, code, t_day)

    if q is not None:
        return q == 1

    #print '%s, %s DB里没查到是否停牌' % (code,t_day)

    i = check_if_paused(code, t_day)
    
    db_operator.record_paused(engine , code , t_day, i )
    

    return i != 0



# 获得指定年份沪市所有交易日
def get_trade_days( yyyy):

    start_d  = str(yyyy)+'-01-01'
    end_d    = str(yyyy+1) + '-01-01'

    q= jq.query(
            jq.finance.STK_EXCHANGE_TRADE_INFO
            ).filter(
                    jq.finance.STK_EXCHANGE_TRADE_INFO.exchange_code==322001  #代表沪市
                    , jq.finance.STK_EXCHANGE_TRADE_INFO.date>= start_d 
                    , jq.finance.STK_EXCHANGE_TRADE_INFO.date< end_d
                    ).limit(300)
    df=jq.finance.run_query(q)
    
    alltday = df['date'].tolist()
    #alltday = df['date'].values
    #print alltday
    return alltday


# 获得指定股票指定年度的‘价值投资向’的财务数据集
def get_annual_value_indicator(sec_code , statYYYY):
    q = jq.query(
          jq.valuation.market_cap  #市值
          ,jq.balance.total_assets  #总资产
          ,jq.balance.good_will     #商誉  其实可疑的项目还有很多，比如无形资产，应收帐款，在建工程，库存 ...
          ,jq.balance.total_current_assets #流动资产
          ,jq.balance.total_liability  #总负债
          ,jq.balance.total_current_liability #流动负债
          ,jq.cash_flow.net_operate_cash_flow  # 经营活动产生的现金流量净额(元) 
          ,jq.cash_flow.net_invest_cash_flow  # 投资活动产生的现金流量净额(元)
          ,jq.income.net_profit      #净利润
          ,jq.income.np_parent_company_owners  # 归属于母公司股东的净利润(元)
          ,jq.income.basic_eps  #基本每股收益(元)
          ,jq.indicator.adjusted_profit   # 扣除非经常损益后的净利润(元)
          ,jq.indicator.gross_profit_margin  #销售毛利率(%)
          ).filter(
                  jq.valuation.code == sec_code,
                  jq.balance.code == sec_code,
                  jq.cash_flow.code == sec_code,
                  jq.income.code == sec_code,
                  jq.indicator.code == sec_code,
                  )
    
    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的财务指标没查到 " % (sec_code , statYYYY  )
    return ret


# 获得指定股票指定年度的财务指标数据
def get_annual_indicator(sec_code , statYYYY):
    q = jq.query(
          jq.indicator  
          ).filter(
                  jq.indicator.code == sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的财务指标没查到 " % (sec_code , statYYYY  )
    return ret


# 获得指定股票指定年度的现金流表
def get_annual_cashflow(sec_code , statYYYY):
    q = jq.query(
          jq.cash_flow 
          ).filter(
                  jq.cash_flow.code == sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的现金流表没查到 " % (sec_code , statYYYY  )
    return ret


# 获得指定股票指定年度的负债表
def get_annual_balancesheet(sec_code , statYYYY):
    q = jq.query(
          jq.balance
          ).filter(
                  jq.balance.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的资产表没查到 " % (sec_code , statYYYY  )
    return ret

# 获得指定股票指定年度的利润表
def get_annual_income(sec_code , statYYYY):
    q = jq.query(
          jq.income
          ).filter(
                  jq.income.code== sec_code,
                  )

    ret = jq.get_fundamentals(q, statDate= statYYYY)

    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的利润没查到 " % (sec_code , statYYYY  )
    return ret

valuation_fetched = {}

# 获得指定股票指定日期的市值数据
def get_valuation(sec_code , yyyy_mm_dd):
    k = ( sec_code, yyyy_mm_dd)
    if k in valuation_fetched: 
        print "    skip fetching valuation %s, %s " %  k 
        return None
 
    valuation_fetched[k] = 1 
    #print "    fetch valuation of %s, %s" % k 

    q = jq.query(
          jq.valuation
          ).filter(
                  jq.valuation.code== sec_code,
                  )

    # 传入date时, 查询指定日期date所能看到的最近(对市值表来说, 最近一天, 对其他表来说, 最近一个季度)的数据, 我们会查找上市公司在这个日期之前(包括此日期)发布的数据, 不会有未来函数.
    ret = jq.get_fundamentals(q, date = yyyy_mm_dd)
    if ret is None or len(ret) == 0:
        print "WARN: %s 于 %s 的市值数据没查到 " % (sec_code , yyyy_mm_dd  )
        return None

    return ret


daily_line_fetched = {}

# 获得指定股票指定时间段的日线
def get_daily_line(sec_code , t_start, t_end ):

    k = (sec_code , t_start, t_end )

    if k in daily_line_fetched:
        #print "    skip fetching daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
        return None

    daily_line_fetched[k] = 1 

    #print "    fetch daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
    df = jq.get_price(sec_code
            , start_date= t_start, end_date=t_end
            , frequency='daily'
               #  默认是None(表示[‘open’, ‘close’, ‘high’, ‘low’, ‘volume’, ‘money’]这几个标准字段)
            , fields=['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused', 'factor']
            , skip_paused=False
            , fq='pre'
            )

    return df

# 获得指定股票指定天数的日线
def get_daily_line_n(sec_code , t_start, howmany ):

    dt_delta = timedelta( days =  howmany *2 + 10 )
    dt_end   = t_start + dt_delta

    #print "    fetch daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
    
    try:

        df = jq.get_price(sec_code
            , start_date = t_start, end_date = dt_end
            , frequency='daily'
               #  默认是None(表示[‘open’, ‘close’, ‘high’, ‘low’, ‘volume’, ‘money’]这几个标准字段)
            , fields=['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused', 'factor']
            , skip_paused=True
            , fq='pre'
            )
    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print

        emsg = str(e)
        if  '找不到标的' in emsg:
            return None
        else:
            raise e

    # 只要前 howmany 行
    return df.iloc[ 0: howmany - 1 ]


# 获得指定股票某天为之，最后N交易日的行情 

# 返回这样的数组 [ 
#                    [交易日，收盘价], 
#                    [交易日，收盘价], ... 
#                ]

def get_his_until(sec_code , t_end, howmany ):

    dt_end  = datetime.strptime( t_end , "%Y-%m-%d").date()

    dt_delta = timedelta( days =  howmany *2 +5 )

    dt_start = dt_end - dt_delta



    #print "    fetch daily line of %s, %s ~ %s" % ( sec_code, t_start, t_end  )
    df = jq.get_price(sec_code
            , start_date= dt_start, end_date=t_end
            , frequency='daily'
               #  默认是None(表示[‘open’, ‘close’, ‘high’, ‘low’, ‘volume’, ‘money’]这几个标准字段)
            , fields=['close', 'pre_close']
            , skip_paused=True
            , fq='pre'
            )

    row_count = len(df.index)

    if row_count < howmany:
        print "WARN: %s, ~%s, only got %d dailyline" %(sec_code, t_end, row_count )
        #应该是在停牌过程中被纳入了指数
        return []

    start_loc = row_count - howmany 
    
    mds = []

    tdays = df.index.get_values()

    for loc in range(start_loc, row_count):
        one_md = [  str(tdays[loc])[:10] , df['close'].iloc[loc ], df['pre_close'].iloc[loc ]]
        mds.append(one_md)

    return mds



# 确定某个月的第几个交易日是哪一天
# 返回: 'datetime' or None  
def get_t_day_in_mon( y ,m , tday_no ):
    t_start = "%d-%02d-01" % (y,m)
    
    
    if m == 12:
        y = y +1
        m = 1
    else:
        m = m+1

    t_end = "%d-%02d-01" % (y, m)

    df = jq.get_price(
             '000300.XSHG'
            , start_date= t_start, end_date=t_end
            , frequency='daily'
            , fields=None
            , skip_paused=True
            , fq='pre'
            )

    if len(df.index) < tday_no:
        return None

    #jqdata 返回的行的索引类型是  numpy.datetime64[ns]  (相当于time_t，以ns为单位)
    tt =  df[tday_no -1: tday_no ].index.values[0]

    #print tt
    #print tt.dtype

    ts = (tt - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')
    
    d = datetime.utcfromtimestamp( ts)

    #print d

    return d.date()

# 查询某股票某天是否停牌
def check_if_paused( code, t_day ):
    df = jq.get_price(
             code 
            , start_date= t_day, end_date=t_day
            , frequency='daily'
            , fields=None
            , skip_paused=True
            , fq='pre'
            )
    
    if len(df.index) < 1:
        return 1

    return 0

def get_code_name(code):
    si = jq.get_security_info(code)
    return si.display_name


def fill_stock_name(l):
    for stock in l:
        si = jq.get_security_info(stock.code)
        stock.name = si.display_name

# 如果code不在 md_that_day中，则调用jq api获取md
def smart_get_md_close( t_day, code, md_that_day ):
    if code in md_that_day:
        # ‘行情’ 是  [收盘价，前日收盘，涨幅， 涨停标志，停牌标志]
        return md_that_day[code ][0] 
 
    df = jq.get_price(code
            , start_date= t_day, end_date=t_day
            , frequency='daily'
               #  默认是None(表示[‘open’, ‘close’, ‘high’, ‘low’, ‘volume’, ‘money’]这几个标准字段)
            , fields=['open', 'close', 'high', 'low', 'volume', 'money', 'high_limit', 'low_limit', 'pre_close', 'paused']
            , skip_paused=False
            , fq='pre'
            )
 
    row_count = len(df.index)

    if row_count < 1:
        raise Exception ("无法获得%s于%s的行情" %(code, t_day) )

    p = df['close'].iloc[0]

    #if math.isnan(p):
    #    print df 
    #    raise  Exception ("无法获得%s于%s的收盘是NaN" %(code, t_day) )

    return p


# 获得指定股票的分红送转股记录
def get_distribute_info(sec_code , start_day):

    q = jq.query(
                jq.finance.STK_XR_XD.company_name
                ,jq.finance.STK_XR_XD.code
                ,jq.finance.STK_XR_XD.report_date
                ,jq.finance.STK_XR_XD.bonus_type #年度分红 中期分红 季度分红 特别分红 向公众股东赠送 股改分红
                ,jq.finance.STK_XR_XD.board_plan_pub_date  #董事会预案公告日期 
                ,jq.finance.STK_XR_XD.board_plan_bonusnote  #董事会预案分红说明 每10股送XX转增XX派XX元
                ,jq.finance.STK_XR_XD.shareholders_plan_pub_date  # 股东大会预案公告日期
                ,jq.finance.STK_XR_XD.shareholders_plan_bonusnote  #股东大会预案分红说明
                ,jq.finance.STK_XR_XD.implementation_pub_date  #实施方案公告日期
                ,jq.finance.STK_XR_XD.implementation_bonusnote #实施方案分红说明
                ,jq.finance.STK_XR_XD.dividend_ratio    # 送股比例 每10股送XX股
                ,jq.finance.STK_XR_XD.transfer_ratio    # 转增比例 每10股转增 XX股
                ,jq.finance.STK_XR_XD.bonus_ratio_rmb   # 每10股派 XX。说明：这里的比例为最新的分配比例，预案公布的时候，预案的分配基数在此维护，如果股东大会或实施方案发生变化，再次进行修改，保证此处为最新的分配基数
                ,jq.finance.STK_XR_XD.dividend_number  # 送股数量 单位：万股
                ,jq.finance.STK_XR_XD.transfer_number  # 转增数量 单位：万股
                ,jq.finance.STK_XR_XD.bonus_amount_rmb # 派息金额(人民币)  单位：万元
                ,jq.finance.STK_XR_XD.a_registration_date # A股股权登记日
                ,jq.finance.STK_XR_XD.a_xr_date           # A股除权日
                ,jq.finance.STK_XR_XD.a_bonus_date        # 派息日(A)
                ,jq.finance.STK_XR_XD.dividend_arrival_date  #红股到帐日
                ,jq.finance.STK_XR_XD.a_increment_listing_date  #A股新增股份上市日 
                ,jq.finance.STK_XR_XD.total_capital_before_transfer  #送转前总股本 单位：万股
                ,jq.finance.STK_XR_XD.total_capital_after_transfer   #送转后总股本
                ,jq.finance.STK_XR_XD.float_capital_before_transfer  #送转前流通股本
                ,jq.finance.STK_XR_XD.float_capital_after_transfer   #送转后流通股本
                #,jq.finance.STK_XR_XD.plan_progress_code     # 方案进度编码
                #,jq.finance.STK_XR_XD.plan_progress          # 方案进度说明
            ).filter(
                jq.finance.STK_XR_XD.code == sec_code
                ,jq.finance.STK_XR_XD.report_date >= start_day
                ,jq.finance.STK_XR_XD.plan_progress_code  == '313002'     # 方案进度编码
            ).order_by (
                jq.finance.STK_XR_XD.report_date
            )
    # 返回一个 dataframe， 每一行对应数据表中的一条数据， 列索引是你所查询的字段名称
    df=jq.finance.run_query(q)

    return df

# 获得指定报告年度的分红送转股记录
def get_XrXd_by_year( year ):

    start_d = '%d-01-01' % year
    end_d   = '%d-01-01' % (year +1)


    q = jq.query(
                jq.finance.STK_XR_XD            
            ).filter(
                jq.finance.STK_XR_XD.report_date >= start_d
                ,jq.finance.STK_XR_XD.report_date <  end_d 
                #,jq.finance.STK_XR_XD.plan_progress_code  == '313002'     # 方案进度编码
            ).order_by (
                jq.finance.STK_XR_XD.code
                ,jq.finance.STK_XR_XD.report_date
            )
    # 返回一个 dataframe， 每一行对应数据表中的一条数据， 列索引是你所查询的字段名称
    df=jq.finance.run_query(q)
 
    row_count = len(df.index)
    if row_count < 1:
        print "WARN!!! 未获得%d年度的分红除权除息数据" %  year
        return None

    return df

# 获得指定报告年度的分红送转股记录
# "313001"    "董事会预案"
# "313002"    "实施方案"
# "313003"    "股东大会预案"
# "313004"    "取消分红"
# "313005"    "公司预案"
def get_XrXd_by_year2( year, progress_code  ):

    start_d = '%d-01-01' % year
    end_d   = '%d-01-01' % (year +1)


    q = jq.query(
                jq.finance.STK_XR_XD            
            ).filter(
                jq.finance.STK_XR_XD.report_date >= start_d
                ,jq.finance.STK_XR_XD.report_date <  end_d 
                ,jq.finance.STK_XR_XD.plan_progress_code  == progress_code   # 方案进度编码
                ,jq.finance.STK_XR_XD.board_plan_bonusnote  != "不分配不转增"
            ).order_by (
                jq.finance.STK_XR_XD.code
                ,jq.finance.STK_XR_XD.report_date
            )
    # 返回一个 dataframe， 每一行对应数据表中的一条数据， 列索引是你所查询的字段名称
    df=jq.finance.run_query(q)
 
    row_count = len(df.index)
    if row_count < 1:
        print "WARN!!! 未获得%d年度的分红除权除息数据" %  year
        return None

    return df


# 获得指定报告年度的预告记录
def get_forcast_by_year( year ):

    start_d = '%d-01-01' % year
    end_d   = '%d-01-01' % (year +1)

    q = jq.query(
                jq.finance.STK_FIN_FORCAST  
            ).filter(
                jq.finance.STK_FIN_FORCAST.end_date >= start_d
                ,jq.finance.STK_FIN_FORCAST.end_date <  end_d 
            ).order_by (
                jq.finance.STK_FIN_FORCAST.code
                ,jq.finance.STK_FIN_FORCAST.end_date
                ,jq.finance.STK_FIN_FORCAST.report_type_id
            )
    # 返回一个 dataframe， 每一行对应数据表中的一条数据， 列索引是你所查询的字段名称
    df=jq.finance.run_query(q)
 
    row_count = len(df.index)
    if row_count < 1:
        print "WARN!!! 未获得%d报告年度的预告数据" %  year
        return None

    return df


