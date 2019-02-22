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
import plotter
import make_indices 

VERBOSE = 1
#比较基准
    #BRK_INDEX = '399975.XSHE'    # 不适合，2015开始才有数据
    #BRK_INDEX_NAME = '中证券商'

BRK_INDEX = '000300.XSHG'    # 不适合，2015开始才有数据
BRK_INDEX_NAME = '沪深300'

INITIAL_BALANCE = 10000.0  # 策略期初金额
TRADE_COST      = 0.0003   # 手续费万三 
TRADE_TAX       = 0.0001   # 印花税千1单向


def fetch_brk_candidators(engine,t_day):

    brokers    = jq.get_industry_stocks( 'J67' , date= t_day )

    return brokers 

 
#收集该年券商的市值 数据 (含pb)
def fetch_brk_fundamentals_1_year(engine, the_year):
 
    print "fetching year of %d" % the_year 

    #1. 获取当年度所有交易日
    alltday = data_fetcher.get_trade_days( the_year)

    first_t_day = alltday[ 0]
    last_t_day  = alltday[-1]
    
    #2. 获取候选 -- 券商股
    candidators = fetch_brk_candidators( engine,  last_t_day)
    print candidators 
        
    #3. 获取每个候选每个交易日的市值数据
    print "fetching valuations..." 
    for t_day in alltday: 
        for sec_code in candidators:
            df =  data_fetcher.get_valuation(sec_code , t_day )
            db_operator. save_valuation_df_to_db (engine, df)
    

    #4. 获取每个候选当年的日线
    print "fetching daily lines of candidators ..." 

    for sec_code in candidators:
        print sec_code 
        df =  data_fetcher.get_daily_line(sec_code , first_t_day, last_t_day )
        db_operator. save_daily_line_to_db (engine, sec_code , df)

    #5. 增补比较基准的日线
    print "fetching daily lines of base" 
    base_df = data_fetcher.get_daily_line (
            BRK_INDEX 
            , first_t_day 
            , last_t_day 
            )
    db_operator. save_daily_line_to_db (engine, BRK_INDEX , base_df)

    print "%d year over." % the_year 
   
    return 

# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’ 是  [可买标志，PB偏离度, PB, N日PB平均，N日PB标准差]
# PB偏离度 = ( PB - MA_of_PB ) / STD_of_PB
def make_indices_by_pb_standard_deviation( conn,  his_md , MA_Size1):
    
    make_indices.add_blank_indices( conn,  his_md)
    make_indices.extend_indices_add_buyable( conn,  his_md)
    make_indices.extend_indices_add_pb_std( conn,  his_md, MA_Size1)

    return his_md
    

def fetch_brk_fundamentals_until_now(engine, start_year, end_year ):

    for y in range( start_year, end_year  + 1):
        fetch_brk_fundamentals_1_year( engine, y)

# Input:  2-D array 'md_his'
#         日期   各脚行情  各脚指标
#         ...
#
# Output: 2-D array , 交易数， 交易成本
#         日期  基准收盘价   策略净值 交易次数  换仓详细  
#         ...
#
BRK_BUY_THRESOLD  = -1 # PB偏离度 低于这个值就买
BRK_SELL_THRESOLD = 1  # PB偏离度 高于这个值就卖
def  sim_brk_pb_policy( conn, his_data,  max_hold,  base_code, start_day = "", end_day = ""):    

    if len(his_data) == 0:
        raise Exception("没有行情历史数据。"  );

    result = []
    trans_num = 0 
    trans_cost = 0.0
    
    sec_num = len( his_data[0][1])

    hold_num = 0
    blank_num = 0

    we_hold =  data_struct.make_init_shares(INITIAL_BALANCE, max_hold)  # 我们的持仓

    real_tday_num  = 0

    for i, row in enumerate(his_data):
 
        t_day = row[0]
        if "" != start_day and t_day < start_day:
            # 略过
            continue

        if "" != end_day and t_day >= end_day:
            break
 

        md_that_day      = row[1]   #当日行情    
        indices_that_day = row[2]   #当日指标   

 
        # 要操作了
        if VERBOSE:
            print "============\nT_Day %s,  we hold\n %s" % (row[0], we_hold)
        
        # 这里有一个近似的假设：
        # 我们可以基于昨日的指标，按照昨日的收盘价，进行操作(记作今日操作)，并把操作的损益反映于今日。
        
        #昨日行情
        y_date    =  his_data[i - 1][0]
        y_md      =  his_data[i - 1][1]
        #print 
        #util.bp(y_md)

        #昨日指标  {code1:指标数组1, code2:指标数组2, ... }
        y_indices =  his_data[i - 1][2]

        # ‘指标’ 是  [可买标志，PB偏离度, PB, N日PB平均，N日PB标准差]
        WHICH_INDI = 1 # 我们取指标数组里哪一个指标?

        # 按指标从小到大排序，故‘buy_worst’
        sorted_y_indices = sorted ( y_indices.items(), key=lambda sec:sec[1][WHICH_INDI])
        
        #print 
        #util.bp(sorted_y_indices)


        to_hold = []   # 继续持仓的份额编号
        to_sell = []   # 要卖出的份额编号
        to_buy  = []   # 要买进的代码
 
        # 撸一遍昨日M“强”，看看有哪些要买进
        max_buy = max_hold
        rank = 1
        for code,indi  in sorted_y_indices:
            if max_buy <=0 :
                break
            
            # 比较基准不买
            if code == base_code:
                continue
            
            if  not indi[0] or 0== indi[0] :
            #   可买标志       N日PB平均
                continue
            
            if code not in md_that_day:
                # 当日该code已经不在指数成份里
                continue

            if indi[1] >= BRK_BUY_THRESOLD:
                # 由于是从小到大排序，故以后的entry也不用看了
                break;

            if VERBOSE:
                print "应当买入 %s，排名%d" % (code, rank)
                #print sorted_y_indices 

            to_buy.append(code)
            max_buy = max_buy - 1
            rank = rank + 1


        #print "%s:" % t_day 
        #print we_hold 

        # 撸一遍我们的持仓，看看有哪些要持有，哪些要卖
        for one_hold in we_hold.pos_entries:
            
            if one_hold.is_blank():
                continue

            if one_hold.code not in md_that_day:
                # 当日该code已经不在指数成份里
                to_sell.append( one_hold.seq)
                if VERBOSE:
                    print "%s,卖出 %s，因为不在成份里" % (t_day, one_hold.code)
                continue

            if one_hold.code in to_buy:
                # 太糟了
                to_hold.append( one_hold.seq)
                to_buy.remove( one_hold.code)
                if VERBOSE:
                    print "%s, 继续持有 %s" % (t_day, one_hold.code)
            
            #检查卖出指标
            indi_check_for_sell = y_indices[one_hold.code]
            if indi_check_for_sell[1] > BRK_SELL_THRESOLD: 
                if VERBOSE:
                    print "%s, 理想卖出 %s" % (t_day, one_hold.code)
                to_sell.append( one_hold.seq)

        to_hold_codes = we_hold.get_codes_from_holds(  to_hold)
        to_sell_codes = we_hold.get_codes_from_holds( to_sell)
        
        op_num = 0
        
        hold_num = hold_num + len(to_hold)

        #print util.build_p_hint( t_day,  to_hold_codes ,  to_sell_codes,  to_buy  )

        # 开始调整we_hold  估算当日的净值
        for one_pos in we_hold.pos_entries:
            if one_pos.seq in to_sell:
                # 要卖掉

                trade_price =  smart_get_md_close(y_date,one_pos.code,y_md)  #  FIXME: 如果该code停牌，这里需要寻找到其复牌价
                if math.isnan( trade_price):
                    print "WARN: %s , %s 的昨收盘是NaN，只能以最后的持仓价格卖出" % (t_day, one_pos.code)
                    trade_price = one_pos.now_price 

                op_num = op_num + 1
                if VERBOSE:
                    print "卖出%s, %d股，毛盈亏%f" % (one_pos.code, one_pos.volumn, one_pos.volumn*( one_pos.now_price - one_pos.cost_price ) )
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
        buy_num = min(len(to_buy) , we_hold.get_blank_num()  )
        if  buy_num > 0:
            
            each = we_hold.remaining / buy_num
            
            if math.isnan(each):
                raise Exception( "%s, remaining=%f, black_num = %d"   % (t_day, we_hold.remaining,we_hold.get_blank_num() )   )

            for one_buy in to_buy:
                pos = we_hold.find_first_blank_pos()
                if not  pos:
                    break
 
                trade_price = y_md[one_buy][0]

                trade_volumn =  int( each  / ( trade_price * (1 + TRADE_COST )))
                trade_amount =  trade_volumn * trade_price 
                trade_loss   =  trade_amount *  TRADE_COST 
                
                op_num = op_num + 1
                if VERBOSE:
                    print "买入%s, %d股" % (one_buy, trade_volumn) 
                
                pos.code   = one_buy
                pos.volumn = trade_volumn  
                pos.cost_price = trade_price 
                pos.now_price  = md_that_day[one_buy ][0]
                    
                we_hold.remaining = we_hold.remaining - trade_amount - trade_loss 
                trans_cost  = trans_cost + trade_loss

        #累计空仓日数
        blank_num = blank_num + we_hold.get_blank_num() 

#       日期  基准收盘价   策略净值 交易次数  换仓详细  
  
        #base_price = md_that_day[base_code][0]
        base_md_that_day = db_operator.query_dailyline(conn, t_day, BRK_INDEX)
        base_price = base_md_that_day[0] 

        t_policy = we_hold.get_value()  
        op_num_text = "%d" % op_num
        t_hint = util.build_t_hint(t_day
                , to_sell_codes  
                , to_buy )

        r_that_day= [ t_day, base_price, t_policy ,op_num_text, t_hint ]
        result.append( r_that_day )

        if VERBOSE:
        #print r_that_day
        #print we_hold
            print "\n\n" 
        trans_num = trans_num + op_num 
        
        real_tday_num  = real_tday_num + 1

    print "平均每天持有仓数 %f, 每天空仓数 %f" % (float(hold_num) / len(his_data) , float(blank_num)/len(his_data))

    return (result ,  trans_num , trans_cost )


def bt_brk_pb_policy(engine, start_day, end_day,max_hold, threshold ):
    #从DB抓日线数据
    conn = engine.connect()

    # 获取日线数据
# 返回数组
#     T_day1,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day2,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     T_day3,  {证券1:证券1的行情, 证券2:证券2的行情, ...   }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘，涨幅， 涨停标志，停牌标志, PB, 换手]
    his_md = db_operator. db_fetch_dailyline_w_valuation(conn, threshold )

    # 在日线数据中，扩充加入指标数据
# 返回时，数组md_his_data扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...

    # ‘指标’ 是  [可买标志，N日PB平均，N日PB标准差]
    make_indices_by_pb_standard_deviation( conn,  his_md , threshold)
 
    result, trans_num, trans_cost  = sim_brk_pb_policy( conn,his_md, max_hold , BRK_INDEX, start_day, end_day )    
# Output: 2-D array , 交易数， 交易成本
#         日期  基准收盘价   策略净值 交易次数  换仓详细  
#         ...
#

    #util.bp( result)
    #准备画图

    #chart_head = ['日期', '50指数', 'MA10'  ]
    #chart_data=[]
    #for entry in his_md:
    #    row = [ entry[0], entry[1][FH_BASE_CODE][0] , entry[2][FH_BASE_CODE][2] ]
    #    chart_data.append(row)
    #plotter.simple_generate_line_chart( chart_head, chart_data)

    base_info = data_struct.SecurityInfo()
    base_info.code = BRK_INDEX 
    base_info.name = BRK_INDEX_NAME

    secs = [ base_info ]

    suffix = ".from_%s" % start_day

    plotter.generate_htm_chart_for_faster_horse2( secs, result , suffix)
 
    #show summary
    t_day_num = len(result)
    base_delta   = result[ t_day_num - 1][1] / result[ 0][1]
    policy_delta = result[ t_day_num - 1][2] / result[ 0][2]

    print "券商PB策略(%d日), %s ~ %s, %d个交易日，交易%d笔，交易成本%f，基准表现%f，策略表现%f" % (
             threshold 
            , result[0][0], result[ t_day_num - 1][0], t_day_num
            , trans_num,  trans_cost
            , base_delta, policy_delta 
            )



# 处理 'fetch_brk' 子命令
def handle_fetch_brk( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        start_year = 2003  

        i = len(argv)
        now = datetime.now()
        end_year = now.year 

        if ( 0== i  ):
            start_year = now.year - 1
        else:
            start_year = int(argv[0])
            
            if ( i >= 2 ):
                end_year  = argv[1]

        if start_year < 2003:
            print "开始年份必须不小于2003"
            return 1

        fetch_brk_fundamentals_until_now(engine, start_year, end_year )


        
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

# 处理 'bt_brk' 子命令
def handle_bt_brk( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()
        
        end_day = ''
        max_hold = 2
        threshold = 500

        i = len(argv)
        if ( 0 == i  ):
            start_day = '2017-01-01'  
        else:
            start_day  = argv[0]

            if ( i >= 2 ):
                end_day  = argv[1]

                if (i>=3):
                    threshold  = int(argv[2])
 
        bt_brk_pb_policy(engine, start_day, end_day,max_hold, threshold )


    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0


