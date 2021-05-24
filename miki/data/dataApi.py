import pandas as pd 
import numpy as np
import pickle
from miki.data import dataGlovar
from jqdatasdk import *


class DataApi(object):
	# 从Api获取数据
	def __init__(self):
		pass

	def get_all_securities(self):
		# 获取股票信息
		df = get_all_securities()
		df.to_pickle(dataGlovar.DataPath+'/stock_info.pkl')
		return df.index.values		

	def get_all_trade_days(self):
		# 获取所有交易日
		all_trade_days = get_all_trade_days()
		with open(dataGlovar.DataPath+'/all_trade_days.pkl', 'wb') as f:
			pickle.dump(all_trade_days, f)
		return all_trade_days

	def get_valuation_data(self, name, date):
		# 获取指标数据
		if name=='valuation':
			df = get_fundamentals(query(valuation), date=str(pd.to_datetime(date).date()))
		elif name=='indicator':
			df = get_fundamentals(query(indicator), date=str(pd.to_datetime(date).date()))
		return df

	def get_finance_data(self, types, start, end):
		# 获取报表数据
		start,end = pd.to_datetime(start).date(),pd.to_datetime(end).date()
		if types == 'forcast': # 业绩预告
			table = finance.STK_FIN_FORCAST
		elif types == 'capital_change': # 股本变动
			table = finance.STK_CAPITAL_CHANGE		
		elif types == 'income': # 利润表
			table = finance.STK_INCOME_STATEMENT
		elif types == 'income_parent': # 母公司利润表
			table = finance.STK_INCOME_STATEMENT_PARENT
		elif types == 'cashflow': # 现金流量表
			table = finance.STK_CASHFLOW_STATEMENT
		elif types == 'cashflow_parent': # 母公司现金流量表
			table = finance.STK_CASHFLOW_STATEMENT_PARENT
		elif types == 'balance': # 资产负债表
			table = finance.STK_BALANCE_SHEET
		elif types == 'balance_parent': # 母公司资产负债表
			table = finance.STK_BALANCE_SHEET_PARENT
		elif types == 'income_finance': # 金融类利润表
			table = finance.FINANCE_INCOME_STATEMENT
		elif types == 'income_finance_parent': # 金融类母公司利润表
			table = finance.FINANCE_INCOME_STATEMENT_PARENT
		elif types == 'cashflow_finance': # 金融类现金流量表
			table = finance.FINANCE_CASHFLOW_STATEMENT
		elif types == 'cashflow_finance_parent': # 金融类母公司现金流量表
			table = finance.FINANCE_CASHFLOW_STATEMENT_PARENT
		elif types == 'balance_finance': # 金融类资产负债表
			table = finance.FINANCE_BALANCE_SHEET
		elif types == 'balance_finance_parent': # 金融类母公司资产负债表
			table = finance.FINANCE_BALANCE_SHEET_PARENT
		elif types == 'shareholder_top10': # 前十大股东
			table = finance.STK_SHAREHOLDER_TOP10
		elif types == 'shareholder_floating_top10': # 前十大流通股股东
			table = finance.STK_SHAREHOLDER_FLOATING_TOP10
		df = finance.run_query(query(table).filter(table.pub_date>=start, table.pub_date<=end))
		return df

	def get_security_data(self, security, start, end):
		# 获取单只股票数据
		df = get_price(security, start_date=start, end_date=end, frequency='1m', 
					   fields=['factor','open','high','low','close','volume','high_limit','low_limit','paused'], fq='post')
		if len(df)>0:
			df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(3)
			df['date'] = [i.timestamp() for i in df.index]
			df = df.loc[:,['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]
			df = df.dropna()
			df = df[df.paused==0]	
		return df.values

	def get_data(self, end_date):
		# 获取行情数据
		stock_list = get_all_securities(types=dataGlovar.types).index.values.tolist()
		df = get_price(stock_list,
					   end_date=end_date,
					   frequency='1m',
					   fields=['factor','open','close','high','low','high_limit','low_limit','volume','paused'],
					   count=1, 
					   skip_paused=False,
					   fq='post',
					   panel=False)
		now_time = df['time'].iloc[-1]
		df['date'] = int(now_time.timestamp())
		stock_list = df['code'].values.tolist()
		df = df.loc[:,['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]	
		df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(2)
		return df.values, stock_list, now_time



