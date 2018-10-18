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

