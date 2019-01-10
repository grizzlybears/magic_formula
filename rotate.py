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

FH_BASE_CODE  = '000016.XSHG'
FH_BASE_NAME  = '上证50'

POOL_CODES = ['601398.XSHG', '601939.XSHG', '601988.XSHG', '601288.XSHG', '601328.XSHG' ] 
POOL_BASE  = '399951.XSHE'   # 比较基准

#  300银行 vs   601398工行, 601939建行, 601988中行, 601288农行, 601328交行


def fetch_dailyline_in_pool_until_now(engine, pool, start_year):
    
    today  = datetime.now().date()
    t_start  = "%d-01-01" % start_year 

    # 抓行情
    pn = jq.get_price(pool
            , start_date= t_start, end_date= today 
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

    row_num = len(df_open.index)
    print "从%d开始至今，有%d交易日" % ( start_year, row_num)

    for one_compo in pool:

        for i in range(row_num):
            t_day = str(df_open.index[i])[:10]
            
            if math.isnan( df_open[one_compo].iloc[i] ):
                print "略过%s %s" %( t_day, one_compo)
                continue
            #print t_day , one_compo , df_open[one_compo].iloc[i] , df_close[one_compo].iloc[i], df_paused[one_compo].iloc[i]
            print "%s, %s : open=%f close=%f paused=%d" % (t_day
                    , one_compo
                    , df_open[one_compo].iloc[i]
                    , df_close[one_compo].iloc[i] 
                    , df_paused[one_compo].iloc[i]   )

            db_operator.db_save_dailyline(engine
                , one_compo 
                , t_day
                , df_open[one_compo].iloc[i]
                , df_close[one_compo].iloc[i]
                , df_high[one_compo].iloc[i]
                , df_low[one_compo].iloc[i]
                , df_volume[one_compo].iloc[i]
                , df_money[one_compo].iloc[i]
                , df_high_limit[one_compo].iloc[i]
                , df_low_limit[one_compo].iloc[i]
                , df_pre_close[one_compo].iloc[i]
                , df_paused[one_compo].iloc[i]
                )


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
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
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

# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’数组:  [可买标志, 偏离度 =  (收盘 - 均线) / 均线 , MA(收盘, $MA_Size1)  ]  
# ‘指标’ 是  [可买标志，N日累计涨幅]
def  make_indices_by_delta2( conn,  his_md , howlong):
    make_indices.add_blank_indices( conn,  his_md)
    make_indices.extend_indices_add_buyable( conn,  his_md)
    make_indices.extend_indices_add_delta( conn,  his_md, howlong)

 
# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’数组:  [可买标志, 偏离度 =  (收盘 - 均线) / 均线 , MA(收盘, $MA_Size1)  ]      
def make_indices_by_MA_delta( conn,  his_md,MA_Size1 = 5):
    
    make_indices.add_blank_indices( conn,  his_md)
    make_indices.extend_indices_add_buyable( conn,  his_md)
    make_indices.extend_indices_add_ma( conn,  his_md, MA_Size1)

    #在指标数组中的 MA之前，插入一个偏离度
    for md_that_day  in his_md:
        
        mds     =  md_that_day[1]
        indices =  md_that_day[2]

        for code,md_of_the_code in mds.iteritems():

            indi_of_the_code = indices[code]
    
            if indi_of_the_code[0] == 0:
                # 不可买
                deviation = 0
            else:
                MA1 = indi_of_the_code[1]

                if 0 == MA1:
                    deviation = 0
                else:
                    deviation =  (md_of_the_code[0] - MA1) / MA1 
        

            indi_of_the_code.insert( 1, deviation) 

    return his_md
    
# 返回时，数组his_md扩充为
#     T_day1, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day2, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     T_day3, {证券1:证券1的行情, 证券2:证券2的行情, ... }, {证券1:证券1的指标, 证券2:证券2的指标, ... }
#     ...
# 其中‘行情’ 是  [收盘价，前日收盘价, 涨幅， 涨停标志，停牌标志]
# ‘指标’数组:  [可买标志, 偏离度 =  (收盘 - 均线) / 均线 , MA(收盘, $MA_Size1)  ]      
def make_indices_by_MA_delta_old( conn,  his_md,MA_Size1 = 5):
    
    recent_mds = {}  # 代码==> 该代码最后几交易日的‘行情’  ** 跳过停牌日
                   # 其中‘行情’ 是  [ 
                   #                    [交易日，收盘价], 
                   #                    [交易日，收盘价], ... 
                   #                ]
    md_prev_day = None

    for md_that_day  in his_md:
        
        t_day  = md_that_day[0]
        md_set = md_that_day[1]
        
        indices = collections.OrderedDict ()

        for code,md_set in md_that_day[1].iteritems():
        
            if md_prev_day is None or code not in  md_prev_day[1]  or code not in  recent_mds :
                # 第一天              昨日行情里没有本code            ‘最近交易日’记录里没有本code

                # 需要从外部获取本code最后N日记录
                recent_memo = data_fetcher.get_his_until( code, t_day, MA_Size1)
                recent_mds[code] = recent_memo 
            else:
                # 停牌的行情不加入 recent_mds
                if not md_set[4] : 
                    recent_mds[code].append( [t_day, md_set[0]  ]  )

            if len(recent_mds[code]) > MA_Size1:
                del recent_mds[code][0]

            MA1 = util.avg( recent_mds[code])
            # 至此MA有了，准备其他指标
            
            if md_set[3] or md_set[4] :
                # 涨停或者停牌的不能买
                can_buy = 0

                deviation = 0 # 没必要再算偏离
            else:
                can_buy = 1
                if 0 == MA1:
                    deviation = 0
                else:
                    deviation =  (md_set[0] - MA1) / MA1 

            indices[code] = [can_buy, deviation, MA1]
        

        md_that_day.append( indices)
        
        # 准备走向下一天
        md_prev_day = md_that_day

    return his_md

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
    blank_num = 0

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
        #print 
        #util.bp(y_md)

        #昨日指标  {code1:指标数组1, code2:指标数组2, ... }
        y_indices =  his_data[i - 1][2]

# 简单的轮换策略：
#     根据昨日涨幅从从高到底排名。
#     前M名如果>0，则各给1/M的仓位。
        # 指标数组:  [可买标志，三日累计涨幅] 
        WHICH_INDI = 1 # 我们取指标数组里哪一个指标?
        sorted_y_indices = sorted ( y_indices.items(), key=lambda sec:sec[1][WHICH_INDI], reverse=True)
        #print 
        #util.bp(sorted_y_indices)


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
                #print "卖出 %s，因为不在成份里" % one_hold.code
                continue
                

            rank, pos = get_rank(one_hold.code, sorted_y_indices, WHICH_INDI)   #可‘并列’的名次
            
            y_indices_of_we_hold = sorted_y_indices[pos][1]  #该持仓代码的昨日指标

            if rank <= sell_threshold and y_indices_of_we_hold[WHICH_INDI] > 0:
                to_hold.append( one_hold.seq)
                #print "持有 %s，排名%d" % (one_hold.code, rank)
            else:
                to_sell.append( one_hold.seq)
                #print "卖出 %s，排名%d" % (one_hold.code, rank)

        to_hold_codes = we_hold.get_codes_from_holds(  to_hold)

        # 撸一遍昨日M强，看看有哪些要买进
        max_buy = max_hold - len(to_hold)
        rank = 1
        for code,indi  in sorted_y_indices:
            if max_buy <=0 :
                break

            # 比较基准不买
            if code == FH_BASE_CODE:
                break
            
            # 指标为负不买
            if indi[WHICH_INDI] <= 0 :
                break
            
            if code in to_hold_codes  or not indi[0]:
            #                         可买标志  
                continue
            
            if code not in md_that_day:
                # 当日该code已经不在指数成份里
                continue

            #print "买入 %s，排名%d" % (code, rank)
            to_buy.append(code)
            max_buy = max_buy - 1
            rank = rank + 1

        to_sell_codes = we_hold.get_codes_from_holds( to_sell)
        
        op_num = len(to_buy) + len(to_sell)
        blank_howmany = max_hold - len(to_hold) - len(to_buy)
        
        hold_num = hold_num + len(to_hold)

        #print util.build_p_hint( t_day,  to_hold_codes ,  to_sell_codes,  to_buy  )
        assert blank_howmany >= 0 
        blank_num = blank_num + blank_howmany 

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

    print "平均每天持有仓数 %d, 每天空仓数 %d" % (hold_num / len(his_data) , blank_num/len(his_data))

    return (result ,  trans_num , trans_cost )

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


# 买最糟轮换策略：
#     根据指标(目前是N日涨幅)从高到底排名。
#     每20天做如下操作:
#     最指标最差的M名，做为买入对象。
#     不是‘买入对象’的持仓都卖出。
#     然后买入不是持仓的‘买入对象’。
# Input:  2-D array 'md_his'
#         日期   各脚行情  各脚指标
#         ...
#
# Output: 2-D array , 交易数， 交易成本
#         日期  基准收盘价   策略净值 交易次数  换仓详细  
#         ...
#
def sim_rotate_buy_worst( his_data,  max_hold, howlong,  base_code, start_day = "", end_day = ""):    

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
 
        # print "\n============\nT_Day %s,  we hold\n %s" % (row[0], we_hold)

        md_that_day      = row[1]   #当日行情    
        indices_that_day = row[2]   #当日指标   

        
        if 1 != (real_tday_num % howlong):
            # 第一天按照第0天的收盘价和指标模拟操作
            # 每howlong天操作一次

            # 不操作，就更新一下持仓价格
            for one_pos in we_hold.pos_entries:
                if one_pos.is_blank():
                    continue
                p = smart_get_md_close(t_day,one_pos.code, md_that_day)

                if math.isnan(p):
                    print "WARN: %s , %s 的收盘是NaN，不更新持仓价格" % (t_day, one_pos.code)
                else:
                    one_pos.now_price = p
            
            md_of_base = md_that_day[base_code]
            t_policy = we_hold.get_value()  
            r_that_day = [t_day, md_of_base[0],  t_policy,   None      ,None ]
            #                    基准收盘价      策略净值    换仓提示   换仓明细
 
            result.append( r_that_day )

            real_tday_num = real_tday_num + 1
            continue 
 
        # 要操作了

        # 这里有一个近似的假设：
        # 我们可以基于昨日的指标，按照昨日的收盘价，进行操作(记作今日操作)，并把操作的损益反映于今日。
        
        #昨日行情
        y_date    =  his_data[i - 1][0]
        y_md      =  his_data[i - 1][1]
        #print 
        #util.bp(y_md)

        #昨日指标  {code1:指标数组1, code2:指标数组2, ... }
        y_indices =  his_data[i - 1][2]

        # 指标数组:  [可买标志，howlong日累计涨幅] 
        WHICH_INDI = 1 # 我们取指标数组里哪一个指标?

        # 按指标从小到大排序，故‘buy_worst’
        sorted_y_indices = sorted ( y_indices.items(), key=lambda sec:sec[1][WHICH_INDI])
        
        # 尝试buy_best，于是就成了 'buy_best'
        #sorted_y_indices = sorted ( y_indices.items(), key=lambda sec:sec[1][WHICH_INDI], reverse=True)
        
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
            if code == FH_BASE_CODE:
                continue
            
            if  not indi[0]:
            #   可买标志  
                continue
            
            if code not in md_that_day:
                # 当日该code已经不在指数成份里
                continue

            #print "应当买入 %s，排名%d" % (code, rank)
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
                #print "%s,卖出 %s，因为不在成份里" % (t_day, one_hold.code)
                continue

            if one_hold.code in to_buy:
                # 太糟了
                to_hold.append( one_hold.seq)
                to_buy.remove( one_hold.code)
                #print "%s, 继续持有 %s" % (t_day, one_hold.code)
            else:
                to_sell.append( one_hold.seq)

        to_hold_codes = we_hold.get_codes_from_holds(  to_hold)
        to_sell_codes = we_hold.get_codes_from_holds( to_sell)
        
        op_num = len(to_buy) + len(to_sell)
        blank_howmany = max_hold - len(to_hold) - len(to_buy)
        
        hold_num = hold_num + len(to_hold)

        #print util.build_p_hint( t_day,  to_hold_codes ,  to_sell_codes,  to_buy  )
        assert blank_howmany >= 0 
        blank_num = blank_num + blank_howmany 

        # 开始调整we_hold  估算当日的净值
        for one_pos in we_hold.pos_entries:
            if one_pos.seq in to_sell:
                # 要卖掉

                trade_price =  smart_get_md_close(y_date,one_pos.code,y_md)  #  FIXME: 如果该code停牌，这里需要寻找到其复牌价
                if math.isnan( trade_price):
                    print "WARN: %s , %s 的昨收盘是NaN，只能以最后的持仓价格卖出" % (t_day, one_pos.code)
                    trade_price = one_pos.now_price 

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
        if math.isnan(each):
            raise Exception( "%s, remaining=%f, black_num = %d"   % (t_day, we_hold.remaining,we_hold.get_blank_num() )   )

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
        
        real_tday_num  = real_tday_num + 1

    print "平均每天持有仓数 %f, 每天空仓数 %f" % (float(hold_num) / len(his_data) , float(blank_num)/len(his_data))

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

    result, trans_num, trans_cost  = sim_rotate( his_md, 3 , FH_BASE_CODE ,start_day, end_day)    
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


def sh50_buy_worst(engine, start_day, end_day, max_hold, howlong):   # N日涨幅最遭

    #从DB抓日线数据
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


    # ‘指标’ 是  [可买标志，N日累计涨幅]
    make_indices_by_delta2( conn,  his_md , howlong)
 
    result, trans_num, trans_cost  = sim_rotate_buy_worst( his_md, max_hold , howlong, FH_BASE_CODE, start_day, end_day )    
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
    base_info.code = FH_BASE_CODE
    base_info.name = FH_BASE_NAME

    secs = [ base_info ]

    suffix = ".from_%s" % start_day

    plotter.generate_htm_chart_for_faster_horse2( secs, result , suffix)
 
    #show summary
    t_day_num = len(result)
    base_delta   = result[ t_day_num - 1][1] / result[ 0][1]
    policy_delta = result[ t_day_num - 1][2] / result[ 0][2]

    print "%d日最糟, %s ~ %s, %d个交易日，交易%d笔，交易成本%f，基准表现%f，策略表现%f" % (
             howlong
            , result[0][0], result[ t_day_num - 1][0], t_day_num
            , trans_num,  trans_cost
            , base_delta, policy_delta 
            )


    
    
def fh50_above_ma(engine, start_day , end_day , ma_size):
    
    now = datetime.now()

    #从DB抓日线数据

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

    # ‘指标’ 是  [可买标志，均线偏离度， 均线]
    make_indices_by_MA_delta( conn,  his_md , ma_size)
    

    result, trans_num, trans_cost  = sim_rotate( his_md, 45 , FH_BASE_CODE, start_day, end_day )    
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
    base_info.code = FH_BASE_CODE
    base_info.name = FH_BASE_NAME

    secs = [ base_info ]

    suffix = ".from_%s" % start_day

    plotter.generate_htm_chart_for_faster_horse2( secs, result , suffix)
 
    #show summary
    t_day_num = len(result)
    base_delta   = result[ t_day_num - 1][1] / result[ 0][1]
    policy_delta = result[ t_day_num - 1][2] / result[ 0][2]

    print "MA%d上方骑快马, %s ~ %s, %d个交易日，交易%d笔，交易成本%f，基准表现%f，策略表现%f" % (
             ma_size
            , result[0][0], result[ t_day_num - 1][0], t_day_num
            , trans_num,  trans_cost
            , base_delta, policy_delta 
            )


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
        
        

        end_day = ''
        ma_size = 5

        i = len(argv)
        if ( 0 == i  ):
            start_day = '2017-01-01'  
        else:
            start_day  = argv[0]

            if ( i >= 2 ):
                end_day  = argv[1]

                if (i>=3):
                    ma_size = int(argv[2])

    
        #fh50_until_now(engine, start_year)   # N日涨幅最强
        
        fh50_above_ma(engine, start_day, end_day, ma_size)   # N日均线偏离度最强




    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass


    return 0

# 处理 'sh50' 子命令 -- 回测50指数成份骑慢马策略 
def handle_sh50( argv, argv0 ): 
    try:
        # make sure DB exists
        conn = db_operator.get_db_conn()
        conn.close()

        # get db engine
        engine = db_operator.get_db_engine()

        end_day = ''
        max_hold = 5
        ma_size  = 20

        i = len(argv)
        if ( 0 == i  ):
            start_day = '2017-01-01'  
        else:
            start_day  = argv[0]

            if ( i >= 2 ):
                end_day  = argv[1]

                if (i>=3):
                    ma_size = int(argv[2])

        sh50_buy_worst(engine, start_day, end_day,max_hold, ma_size)   # N日涨幅最遭

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0

# 处理 'fetchp' 子命令，下载池中股票的日线
def handle_fetch_in_pool( argv, argv0 ): 
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

        pool = POOL_CODES
        pool.append( POOL_BASE )
        #pool = ['601288.XSHG', '601328.XSHG','399951.XSHE' ]
        
        fetch_dailyline_in_pool_until_now(engine,pool , start_year)

    except  Exception as e:
        (t, v, bt) = sys.exc_info()
        traceback.print_exception(t, v, bt)
        print
        print e
        return 1 
    finally:
        pass

    return 0


