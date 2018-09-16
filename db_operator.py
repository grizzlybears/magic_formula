## -*- coding: utf-8 -*-
from datetime import date
from datetime import datetime

import sqlite3
import data_struct 
import subprocess

import data_struct 

import  pandas as pd
from   sqlalchemy import create_engine

# 打开DB，返回 sqlalchemy 的 db engine 
def get_db_engine():
    engine = create_engine( "sqlite:///%s" % data_struct.DB_PATH, echo=True)
    return engine 


 # 打开DB，并酌情建表，返回 sqlite3.Connection
def get_db_conn():
    conn = sqlite3.connect( data_struct.DB_PATH )
    conn.text_factory = str
 
    sql = ''' CREATE TABLE IF NOT EXISTS BalanceSheetDay (
       id  INTERGER
       , code   TEXT 
       , day    TEXT
       , pubDate TEXT
       , statDate TEXT
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

    
def save_balance_df_to_db(engine, dataframe ):
    #pd.set_option('display.max_columns', 200)

    a = dataframe.drop( 'statDate.1' ,  axis =1 )  # 这列表里没有

    #pd.reset_option('display.max_columns')


    a.to_sql( 'BalanceSheetDay', con = engine , index=False, if_exists='append')


def save_df_to_db_table(engine, dataframe, tablename ):
    dataframe.to_sql( tablename, con = engine , if_exists='append')




