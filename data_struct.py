# -*- coding: utf-8 -*-

import os
import re
import sqlite3

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


# 骑快马策略把总资产池分成M‘份’
# 每份包括一个持仓以及残余的资金
class ShareOfRotation:
    #份额编号
    seq = 0

    #股票代码 
    code       = "" 

    #持有股数
    volumn = 0

    #进价
    cost_price = 0.0

    #现价
    now_price = 0.0

    #残余资金
    remaining = 0.0

    # 是否空仓
    def is_blank(self):
        return self.volumn == 0

    # 计算总价值
    def get_value(self):
        return self.volumn * self.now_price + self.remaining 

    def __repr__(self):
        s = "[%d]: %s %d股，成本 %f ，现价 %f，可用资金 %f，总价值 %f" % ( 
                self.seq
                , self.code  , self.volumn 
                , self.cost_price , self.now_price 
                , self.remaining 
                , self.get_value()
                )

        return s

# 把初始资金'init_amount'，分成'share_num'个份额
def make_init_shares( init_amount, share_num):
    each = init_amount / share_num

    we_hold = []

    for i in range(share_num):
        one_hold = ShareOfRotation()
        one_hold.seq = i
        one_hold.remaining = each

        we_hold.append( one_hold)

    return we_hold

def get_total_hold_value( we_hold):
    v = 0.0
    for one_hold in we_hold:
        v = v + one_hold.get_value()

    return v

def get_codes_from_holds( we_hold, seqs):
    r = []
    for one in seqs:
        r.append( we_hold[one].code )

    return r

def find_first_blank_pos( we_hold):
    for one_hold in we_hold:
        if one_hold.is_blank():
            return one_hold

    return None

