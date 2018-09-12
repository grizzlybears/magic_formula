## -*- coding: utf-8 -*-
from datetime import date
from datetime import datetime

import sqlite3
import data_struct 
import subprocess

import data_struct 

 # 打开DB，并酌情建表，返回 sqlite3.Connection
def get_db_conn():
    conn = sqlite3.connect( data_struct.DB_PATH)
    conn.text_factory = str
 
    sql = ''' CREATE TABLE IF NOT EXISTS SecurityInfo (
       code   TEXT 
       , name TEXT
       , dir  TEXT
       , PRIMARY KEY( code)
       )
    '''
    conn.execute( sql) 

    sql = ''' CREATE TABLE IF NOT EXISTS MdHis (
       code   TEXT 
       , t_day      TEXT
       , t_week_day TEXT
       , open  NUMERIC
       , close NUMERIC
       , high  NUMERIC
       , low   NUMERIC
       , delta_r NUMERIC
       , volume  NUMERIC
       , amount  NUMERIC
       , delta_alpha NUMBER 
       , turnover_r NUMERIC
       , PRIMARY KEY( code,t_day)
       )
    '''
    conn.execute( sql) 
    

    conn.commit()

    return conn 


def save_sec_info_to_db( dbcur, info): 
    dbcur.execute( '''insert or replace into  SecurityInfo(
                code, name , dir 
                )
            values (?, ?, ?
                 )'''
                , ( info.code , info.name  , info.dirpath 
                  )
                )


def save_sec_info_to_db_if_not_exists( dbcur, info): 
    dbcur.execute( '''
                insert into SecurityInfo (code,name,dir)
                select ? , ? ,?
                where 
                   not exists (select 1 from SecurityInfo  where  code = ? )
                 '''
                , ( info.code , info.name  , info.dirpath , info.code       )
                )



