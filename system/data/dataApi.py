import pandas as pd 
import numpy as np 
import os, sys
from datetime import datetime, timedelta 
import time
import redis
import pickle
from dataGlovar import DataPath
from jqdatasdk import *


class DataApi(object):
	'''
		1.目前采用jqdata的api接收数据，也可以采用rqdata等其它数据接口
		2.jqdata网址：https://www.joinquant.com/help/api/help?name=JQData
		3.rqdata网址：https://www.ricequant.com/welcome/rqdata
		4.欢迎大家贡献其它数据来源代码

	'''
	def __init__(self):
		self.login()
		self.stock_info = get_all_securities(types=['stock','index'])

	def logout(self):
		logout()
		print('logout api')

	def login(self):
		for _ in range(10):
			try:
				auth('你的账户', '你的密码')
				print('login api')
				break
			except Exception as e:
				continue

	def get_all_securities(self, types=None):
		# api获取股票信息
		df = get_all_securities(types=['stock','index'])
		df.to_pickle(DataPath+'/pickle/stock_info.pkl')
		if types is not None:
			df = df[df.type.isin(types)]
		return list(df.index.values)		

	def get_all_trade_days(self):
		# api获取所有交易日
		all_trade_days = get_all_trade_days()
		with open(DataPath+'/pickle/all_trade_days.pkl', 'wb') as f:
			pickle.dump(all_trade_days, f)
		return all_trade_days
		
	def get_finance_data(self, search_column=None, code=None, end=None, types=None, limit=None, get_recent_date=False):
		# api获取报表数据
		if types == 'forcast':
			table = finance.STK_FIN_FORCAST
		elif types == 'capital_change':
			table = finance.STK_CAPITAL_CHANGE
		elif types == 'indicator':
			q = query(indicator).filter(indicator.code==code)
			df = get_fundamentals_continuously(q, count=limit, panel=False)
			del df['id'], df['code.1'], df['day.1']
			df = df.rename(columns={'day':'date','pubDate':'pub_date','statDate':'start_date'})
			df['date'] = df['date'].apply(lambda x:pd.to_datetime(x).date())
			return df
		elif types == 'valuation':
			q = query(valuation).filter(valuation.code==code)
			df = get_fundamentals_continuously(q, count=limit, panel=False)
			del df['id'], df['code.1'], df['day.1']
			df = df.rename(columns={'day':'date'})
			df['date'] = df['date'].apply(lambda x:pd.to_datetime(x).date())
			return df			
		elif types == 'income':
			table = finance.STK_INCOME_STATEMENT
		elif types == 'income_parent':
			table = finance.STK_INCOME_STATEMENT_PARENT
		elif types == 'cashflow':
			table = finance.STK_CASHFLOW_STATEMENT
		elif types == 'cashflow_parent':
			table = finance.STK_CASHFLOW_STATEMENT_PARENT
		elif types == 'balance':
			table = finance.STK_BALANCE_SHEET
		elif types == 'balance_parent':
			table = finance.STK_BALANCE_SHEET_PARENT
		elif types == 'income_finance':
			table = finance.FINANCE_INCOME_STATEMENT
		elif types == 'income_finance_parent':
			table = finance.FINANCE_INCOME_STATEMENT_PARENT
		elif types == 'cashflow_finance':
			table = finance.FINANCE_CASHFLOW_STATEMENT
		elif types == 'cashflow_finance_parent':
			table = finance.FINANCE_CASHFLOW_STATEMENT_PARENT
		elif types == 'balance_finance':
			table = finance.FINANCE_BALANCE_SHEET
		elif types == 'balance_finance_parent':
			table = finance.FINANCE_BALANCE_SHEET_PARENT
		elif types == 'stock_info':
			df = finance.run_query(query(finance.STK_LIST))
			return df
		elif types == 'company_info':
			df = finance.run_query(query(finance.STK_COMPANY_INFO))
			return df			

		if get_recent_date:
			df = finance.run_query(query(table).filter(table.pub_date>end))
			return df
			
		if type(search_column) == int:
			df = finance.run_query(query(table).filter(table.company_id==search_column,table.pub_date<=end).limit(limit))
		else:
			df = finance.run_query(query(table).filter(table.code==search_column,table.pub_date<=end).limit(limit))
		return df

	def get_security_data(self, security, start, end, return_df=False):
		# 获取单只股票数据
		df = get_price(security, start_date=start, end_date=end, frequency='1m', 
					   fields=['factor','open','high','low','close','volume','high_limit','low_limit','paused'], fq='post')
		if len(df)>0:
			df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(2)
			df['date'] = [i.timestamp() for i in df.index]
			df = df.loc[:,['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]
			df = df.dropna()
		if return_df:
			return df		
		return df.values

	def get_data(self, end_date):
		# 实时获取行情数据
		stock_list = list(get_all_securities(types=['stock','index'], date=datetime.now()).index)
		panel = get_price(stock_list,
						  end_date=end_date,
						  frequency='1m',
						  fields=['factor','open','close','high','low','high_limit','low_limit','volume','paused'],
						  count=1, 
						  skip_paused=False,
						  fq='post')
		df = panel.iloc[:,-1,:]
		now_time = panel.major_axis[-1]
		# api接口特性，后除权
		df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(2)
		df['date'] = int(now_time.timestamp())
		df = df[['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]
		df = df.dropna(axis=0)
		stock_list = df.index.values
		return df.values, stock_list, now_time

if __name__ == '__main__':
	d = DataApi()





