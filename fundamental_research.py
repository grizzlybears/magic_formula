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

#需要从05年开始，每年的净利润，净资产，市值，自由现金流，毛利率
#经营活动现金流量净额 - 投资活动现金流量净额 = 自由现金流
#能把大行业也标出来么？ 比如，制造，金融，服务，采选什么的
#如果能列出每年四月三十号的复权价格就更好了
#输出是一个大表格，每个证券一行，然后一年N列， 一行包括n年


