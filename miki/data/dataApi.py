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
		# api获取股票信息
		df = get_all_securities(types=['stock','index'])
		df.to_pickle(dataGlovar.DataPath+'/stock_info.pkl')
		return df.index.values		

	def get_all_trade_days(self):
		# api获取所有交易日
		all_trade_days = get_all_trade_days()
		with open(dataGlovar.DataPath+'/all_trade_days.pkl', 'wb') as f:
			pickle.dump(all_trade_days, f)
		return all_trade_days
		
	def get_security_data(self, security, start, end):
		# 获取单只股票数据
		df = get_price(security, start_date=start, end_date=end, frequency='1m', 
					   fields=['factor','open','high','low','close','volume','high_limit','low_limit','paused'], fq='post')
		if len(df)>0:
			df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(2)
			df['date'] = [i.timestamp() for i in df.index]
			df = df.loc[:,['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]
			df = df.dropna()
			df = df[df.paused==0]	
		return df.values

	def get_data(self, end_date):
		# 获取行情数据
		stock_list = get_all_securities(types=['stock','index']).index.values.tolist()
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
		stock_list = df['code'].values
		df = df.loc[:,['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']]	
		df.loc[:,['open','high','low','close','high_limit','low_limit']] = (df.loc[:,['open','high','low','close','high_limit','low_limit']]/df.loc[:,'factor'].values[:,np.newaxis]).round(2)
		return df.values, stock_list, now_time





