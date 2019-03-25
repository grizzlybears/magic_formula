# -*- coding: utf-8 -*-

import os
import re
import sqlite3

import util

WORKING_DIR = "working_dir"
DB_PATH     = "%s/mf.db" % WORKING_DIR


def is_yyyy_mm_dd (s):

    return  re.match( '^\d{4}-\d{2}-\d{2}$' , s )


# 代码, 报表期末日,净运营资本，固定资产，有息负债，少数股东权益，EBIT，市值
class StockCandidatorInfo:
    #股票代码 
    code       = "" 
    name       = ""

    #成份生成年
    year       = 0

    #成份生成月
    month      = 0

    #报告期末日
    stat_end   = ""

    net_op_cap         = 0.0
    fixed_assets       = 0.0
    nonfree_liability  = 0.0
    minority_interests = 0.0
    EBIT               = 0.0
    market_cap         = 0.0

    ROC = 0.0
    EY  = 0.0

    #基于 ROC 的排名
    rank_roc = 0

    #基于 EY的排名
    rank_ey = 0

    #总体排名
    rank_final= 0

    def __repr__(self):
        if  self.ROC == 0:
            rr = 0
        else: 
            rr = 1 / self.ROC
        s = "%s %s [%s] ROC=%f(%f) EY=%f rank:%d/%d/%d" % (  
                self.code, self.name, self.stat_end
                , self.ROC ,rr
                , self.EY  
                , self.rank_roc , self.rank_ey, self.rank_final 
                )
        return s


class TradeRecord:
    #股票代码 
    code       = "" 
    name       = ""

    #交易日
    t_day   = ""

    #买卖方向, 1:买, -1卖 
    direction = 0

    #成交股数
    volumn = 0

    #单价
    price = 0.0

    #成交金额 
    amount = 0.0

    #交易成本
    fee = 0.0

    def __repr__(self):
        if self.direction == 0:
            direction = '持仓'
        elif self.direction == 1:
            direction = '买'
        elif self.direction == -1:
            direction = '卖'
        else:
            direction = '非法交易方向'


        s = "%s %s %s(%s) %d股，价 = %f，成交金额 = %f" % ( 
                self.t_day  
                , direction 
                , self.code,  self.name 
                , self.volumn 
                , self.price , self.amount 
                )

        return s

class SecurityInfo:
    def __init__(self):
        self.code = ""
        self.name = ""

    def __repr__(self):
        return "%s(%s)" % (self.code, self.name)

    def __str__(self):
        return "%s(%s)" % (self.code, self.name)


# 一个持仓
class PostionEntry:
    #仓位编号
    seq = 0

    #股票代码 
    code       = "" 

    #持有股数
    volumn = 0

    #进价
    cost_price = 0.0

    #现价
    now_price = 0.0
 
    # 是否空仓
    def is_blank(self):
        return self.volumn == 0

    # 计算市值
    def get_value(self):
        return self.volumn * self.now_price  

    def __repr__(self):
        s = "[%d]: %s %d股，成本 %f ，现价 %f，市值 %f" % ( 
                self.seq
                , self.code  , self.volumn 
                , self.cost_price , self.now_price 
                , self.get_value()
                )

        return s

# 整体持仓
class TotalPosition:
    pos_entries = []  #所有持仓
    remaining    = 0.0 #剩余资金

    #总资产
    def get_value(self): 
        
        v = 0.0
        for one_hold in self.pos_entries:
            v = v + one_hold.get_value()

        v = v + self.remaining 

        return v
    
    def __repr__(self): 
       
        s =  ""
       
        for one_hold in self.pos_entries:
            s = s +   str( one_hold) + "\n"

        s = s + "资金:%f\n " % self.remaining 
        s = s + "    ==== 总资产 %f ====" % self.get_value()
        
        return s

    def get_blank_num(self):
        n = 0
        for one_hold in self.pos_entries:
            if one_hold.is_blank():
                n = n +1

        return n

    def get_codes_from_holds( self, seqs):
        r = []
        for one in seqs:
            r.append( self.pos_entries[one].code )

        return r

    def find_first_blank_pos( self):
        for one_hold in self.pos_entries:
            if one_hold.is_blank():
                return one_hold
        return None

    #统计持仓中，每个代码有几个单位 (用于网格化持仓)
    def who_howmuch(self):
        sta = {}
        for one_hold in self.pos_entries:
            if one_hold.is_blank():
                continue

            if one_hold.code in sta:
                num = sta[one_hold.code] + 1
            else:
                num = 1

            sta[one_hold.code] = num 

        return sta

    #指定的代码持有几仓？
    def get_pos_of_code(self, code):
        sta = self.who_howmuch()
        if code in sta:
            return sta[code]

        return 0

    #返回进价最低的'code'仓位编号，找不到则返回负数。
    def find_lowest_pos( self, code):
        no = -1
        cur_price = 9999999

        for one_hold in self.pos_entries:
            if one_hold.is_blank() or one_hold.code != code:
                continue
            
            if one_hold.cost_price < cur_price:
                cur_price = one_hold.cost_price
                no = one_hold.seq 
        
        return no
                
    #返回进价最高的'code'仓位编号，找不到则返回负数。
    def find_highest_pos( self, code):
        no = -1
        cur_price = -9999

        for one_hold in self.pos_entries:
            if one_hold.is_blank() or one_hold.code != code:
                continue
            
            if one_hold.cost_price > cur_price:
                cur_price = one_hold.cost_price
                no = one_hold.seq 
        
        return no
 

# 把初始资金'init_amount'，分成'share_num'个份额
def make_init_shares( init_amount, share_num):
    each = init_amount / share_num

    total_pos = TotalPosition()
    total_pos.remaining = init_amount 

    for i in range(share_num):
        one_hold =  PostionEntry() 
        one_hold.seq = i

        total_pos.pos_entries.append( one_hold)

    return total_pos 


class XrXdInfo:
    #代码
    code = ''

    #报告期
    report_date = ''

    #董事会公告日
    board_plan_pub_date = ''
    #董事会公告方案
    board_plan_bonusnote = ''

    #股东大会公告日
    shareholders_plan_pub_date =''
    #股东大会方案 
    shareholders_plan_bonusnote = ''

    #实施公告日
    implementation_pub_date = ''
    #实施方案
    implementation_bonusnote = ''

    #A股登记日
    a_registration_date = ''
    #每10股送几股
    dividend_ratio = 0
    #每10股转几股   
    transfer_ratio = 0 
    #每10股派多少  
    bonus_ratio_rmb = 0.0

    #分配基盘(万股)
    distributed_share_base_implement = 0.0
    #送股数(万股) 
    dividend_number = 0.0
    #转股数  (万股) 
    transfer_number = 0.0
    #分红额(万元)  
    bonus_amount_rmb = 0.0

    # 股息率
    distr_r = 0.0

    # 登记日总市值(亿元)  
    market_cap = 0.0

    # 董事会公告后行情
    md_of_board = None

    #股东大会公告后行情
    md_of_shareholders = None

    #实施公告后行情
    md_of_implementation = None 

    #A股登记日行情
    md_of_registration  = None

    # 代码，报告期，董事会公告日，董事会方案，董事会公告日行情，
    #               股东大会公告日，股东大会方案，股东大会公告日行情，
    #               实施公告日，实施公告方案，实施公告日行情
    #               A股登记日，登记日行情
    #分配基盘(万股)，送股数(万股)，转股数(万股)，分红金额(万股)，登记日股息率，登记日总市值
    #                
    def to_csv_str(self):
        s = ''
        s = s + "%s,%s" % (self.code, self.report_date)

    #   董事会公告日，董事会方案，董事会公告日行情(带5日，10日，20日)，
        s = s + ",%s,%s" % (self.board_plan_pub_date, self.board_plan_bonusnote ) 
        if self.md_of_board is not None:
            s = s + self.md_of_board.to_csv_str_headcomma()
        else:
            s = s + XrXdCheckInfo.blank_csv_headcomma()
 
    #               股东大会公告日，股东大会方案，股东大会公告日行情，
        s = s + ",%s,%s" % (self.shareholders_plan_pub_date, self.shareholders_plan_bonusnote) 
        if  self.md_of_shareholders is not None:
            s = s +  self.md_of_shareholders.to_csv_str_headcomma_md1()
        else:
            s = s + XrXdCheckInfo.blank_csv_headcomma()

    #               实施公告日，实施公告方案，实施公告日行情
        s = s + ",%s,%s" % (self.implementation_pub_date, self.implementation_bonusnote) 
        if  self.md_of_implementation is not None:
            s = s +  self.md_of_implementation.to_csv_str_headcomma_md1()
        else:
            s = s + XrXdCheckInfo.blank_csv_headcomma()

    #               A股登记日，登记日行情(带5日，10日，20日)
        s = s + ",%s" % (self.a_registration_date) 
        if  self.md_of_registration is not None:
            s = s +  self.md_of_registration.to_csv_str_headcomma()
        else:
            s = s + XrXdCheckInfo.blank_csv_headcomma()

        #分配基盘(万股)，送股数(万股)，转股数(万股)，分红金额(万股)，登记日股息率，登记日总市值

        s = s + ",%s,%s,%s,%s,%s,%s" % (
                util.nullable_float( self.distributed_share_base_implement)
                ,util.nullable_float( self.dividend_number )
                ,util.nullable_float( self.transfer_number )
                ,util.nullable_float( self.bonus_amount_rmb )
                ,util.nullable_float( self.distr_r )
                ,util.nullable_float( self.market_cap) 
                )

        return s

class XrXdCheckInfo: 
    # 调研日，一般是 董事会公告日/股东大会公告日/实施公告日/登记日 之一
    check_date = ''

    check_reason = ''

    #每10股送几股
    dividend_ratio = 0
    #每10股转几股   
    transfer_ratio = 0 
    #每10股派多少  
    bonus_ratio_rmb = 0.0

    #第一个交易日
    t_day = ''

    #第一个交易日的行情
    p_open  = 0.0
    p_close = 0.0
    p_pre_close = 0.0
    p_pre_close_nonrestore = 0.0

    #同日比较标准的行情
    b_open = 0.0
    b_close = 0.0
    b_pre_close = 0.0

    #更多行情比较
    #[
    #    [第几天,t_day,标的收盘,基准收盘 ]
    #    [第几天,t_day,标的收盘,基准收盘 ]
    #    ...
    #]
    more_md = []
    
    def __init__(self):
        self.check_date = ''

        self.check_reason = ''
        self.t_day  = ''
        self.more_md = []

    @staticmethod
    def blank_csv_headcomma():
        return ',,,,,,,,,,'

    # , 送股率，转股率，分红率，第一交易日，开盘，收盘，昨收，不复权昨收, 基准开盘，基准收盘，基准昨收
    # , 第五交易日，收盘，基准收盘
    # , 第十交易日，收盘，基准收盘
    # , 第二十交易日，收盘，基准收盘
    def to_csv_str_headcomma(self):
    
        s =self.to_csv_str_headcomma_md1()
       
        if len(self.more_md) >=1:
            #有第五日
            s = s + ',%s,%f,%f' % (self.more_md[0][1],self.more_md[0][2], self.more_md[0][3])
        else:
            s = s + ',,,'

        if len(self.more_md) >=2:
            #有第10日
            s = s + ',%s,%f,%f' % (self.more_md[1][1],self.more_md[1][2], self.more_md[1][3])
        else:
            s = s + ',,,'

        if len(self.more_md) >=3:
            #有第20日
            s = s + ',%s,%f,%f' % (self.more_md[2][1],self.more_md[2][2], self.more_md[2][3])
        else:
            s = s + ',,,'


        return s
    
    # , 送股率，转股率，分红率，第一交易日，开盘，收盘，昨收，不复权昨收, 基准开盘，基准收盘，基准昨收
    def to_csv_str_headcomma_md1(self):
        s =''
        if self.dividend_ratio:
            s = s + ',%f' % self.dividend_ratio 
        else:
            s = s + ','

        if self.transfer_ratio:
            s = s + ',%f' % self.transfer_ratio 
        else:
            s = s + ','

        if self.bonus_ratio_rmb:
            s = s + ',%f' % self.bonus_ratio_rmb
        else:
            s = s + ','

        if self.t_day == '':
            s = s + ',,,,,,,,'
        else:
            s = s + ',%s' % self.t_day 
            s = s + ',%f,%f,%f,%f' % (self.p_open, self.p_close, self.p_pre_close, self.p_pre_close_nonrestore)
            s = s + ',%f,%f,%f' % (self.b_open, self.b_close, self.b_pre_close)
     
        return s

class ForcastInfo:
    #代码
    code = ''

    #报告期
    end_date = ''

    #预告期类型编码:  304001 一季度预告,  304002  中报预告, 304003  三季度预告,  304004  四季度预告
    report_type_id = 0
    
    #预告期类型
    report_type  = ''

    # 公布日期
    pub_date = ''

    #预告类型编码: 305001   业绩大幅上升, 305002  业绩预增, 305003  业绩预盈, 305004  预计扭亏
    #              305005  业绩持平,  305006  无大幅变动
    #              305007  业绩预亏,  305008  业绩大幅下降, 305009  大幅减亏, 305010  业绩预降, 305011  预计减亏
    #              305012  不确定,  305013  取消预测
    type_id = 0

    #预告类型
    forcast_type  = ''    # DB字段是 'type'
    
    #预告净利润（下限）
    profit_min = 0.0 
    
    #预告净利润（上限）
    profit_max = 0.0
    
    #去年同期净利润
    profit_last = 0.0

    #预告净利润变动幅度(下限) 单位：%
    profit_ratio_min = 0.0

    #预告净利润变动幅度(上限) 单位：%
    profit_ratio_max = 0.0
 
    #预告内容
    content    = ''

    # (文档里未说明，实际数据中为空) 
    #board_plan_pub_date  = ''
    
    # (文档里未说明，实际数据中为空)
    #board_plan_bonusnote = ''
    
    # 公告后行情
    #[
    #    [第几天,t_day,标的开盘，标的收盘,标的昨收，基准开盘，基准收盘，基准昨收 ]
    #    [第几天,t_day,标的开盘，标的收盘,标的昨收，基准开盘，基准收盘，基准昨收 ]
    #    ...
    #]
    md_from_pub = []

    def __init__(self):
        self.md_from_pub = []


   
