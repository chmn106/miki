import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import os, sys, pickle, bcolz
from miki.data import dataGlovar
from miki.data.dataFunction import DataFunction


class Query(object):
	def __init__(self):
		pass
	def get_security_info(self, types=None):
		# 获取所有股票信息
		df = pd.read_pickle(dataGlovar.DataPath+'/stock_info.pkl')
		return df
	def get_all_trade_days(self):
		# 获取所有交易日
		with open(dataGlovar.DataPath+'/all_trade_days.pkl', 'rb') as f:
			all_trade_days = pickle.load(f)
		return all_trade_days		
	def get_time_list(self, start, end, unit):
		# 获取交易时间列表
		def func(x):
			x = (x.hour-9)*60+x.minute-30 if x.hour<=11 else (x.hour-13+2)*60+x.minute
			return x
		start,end = pd.to_datetime(start),pd.to_datetime(end)
		date_list = bcolz.open(DataFunction.get_path('000001.XSHG', unit='1m')+'/date', mode='r')
		date_list = [datetime.utcfromtimestamp(i) for i in date_list]
		series = pd.Series(date_list, index=date_list)
		series = series[(series.index>=start)&(series.index<=end)]
		if len(series) == 0: return []
		series = series.apply(func)
		return list(series[series.values%unit==0].index)
	def get_dataframe(self, year, field, unit, update):
		# 获取某年的数据，用于回测推送
		start = pd.to_datetime('{}-01-01'.format(year))
		end = pd.to_datetime('{}-01-01'.format(year+1))
		cache_path = dataGlovar.DataPath+'/{}-{}-{}.pkl'.format(field, unit, year)
		if not os.path.exists(cache_path) or update:
			print('not exist path {}, generate data !'.format(cache_path))
			# 获取股票代码
			security_list = []
			for param in ['stock1m']:
				for i in os.listdir(dataGlovar.DataPath+'/'+param):
					for j in os.listdir(os.path.join(dataGlovar.DataPath+'/'+param, i)):
						security_list.append(j)
			# 沪深300、中证500基准
			security_list += ['399300.XSHE','000905.XSHG']
			df_list = []
			for security in security_list:
				path = DataFunction.get_path(security, unit=unit)			
				if not os.path.exists(path):
					return None
				table = bcolz.open(path, mode='r')
				outcols = ['date',field]
				table = table.fetchwhere('(date<={})&(date>={})'.format(end.timestamp(), start.timestamp()), outcols=outcols)
				if len(table)==0:
					continue
				df = table.todataframe()
				if unit=='1m':
					df['date'] = df['date'].apply(lambda x:datetime.utcfromtimestamp(x))
				else:	
					df['date'] = df['date'].apply(lambda x:datetime.utcfromtimestamp(x).date())
				df = df.set_index('date')
				df.columns = [security]
				df_list.append(df)
			df = pd.concat(df_list, axis=1, sort=True)
			df.to_pickle(cache_path)
		else:
			df = pd.read_pickle(cache_path)			
		return df
	def get_stock(self, security, start, end, field_list, unit, fq=None):
		# 获取单只股票数据
		start,end = pd.to_datetime(pd.to_datetime(start).date()),pd.to_datetime(end)
		map_dict = {'5m':5,'15m':15,'30m':30,'60m':60,'120m':120}
		unit = map_dict.get(unit, unit)
		path1d = DataFunction.get_path(security, unit='1d')
		path1m = DataFunction.get_path(security, unit='1m')
		if not os.path.exists(path1d):
			print('not exists {} data'.format(security))
			return pd.DataFrame(columns=field_list)
		ctable1 = bcolz.open(path1m, mode='r')
		outcols = ['date','factor'] + [i for i in field_list if i in ['open','high','low','close','volume']]
		ctable1 = ctable1.fetchwhere('(date<={})&(date>={})'.format((end+timedelta(days=1)).timestamp(), start.timestamp()), outcols=outcols)
		ctable2 = bcolz.open(path1d, mode='r')
		outcols = ['date'] + [i for i in field_list if i in ['high_limit','low_limit']]
		ctable2 = ctable2.fetchwhere('(date<={})&(date>={})'.format((end+timedelta(days=1)).timestamp(), start.timestamp()), outcols=outcols)
		df1 = ctable1.todataframe()
		df2 = ctable2.todataframe()
		df1['date'] = df1['date'].apply(lambda x: datetime.utcfromtimestamp(x))
		df2['date'] = df2['date'].apply(lambda x: datetime.utcfromtimestamp(x))
		df1 = df1.set_index('date')
		df2 = df2.set_index('date')
		df = pd.concat([df1, df2], axis=1)
		df = df.fillna(method='bfill')
		df = df[(df.index>=start)&(df.index<=end)]
		if len(df)==0:
			return pd.DataFrame(columns=field_list)
		if 'volume' in field_list:
			# 成交量需要除权
			df['volume'] = df['volume'] * df['factor']
		if fq:
			cols = [i for i in field_list if i in ['open','high','low','close']]
			if fq=='pre':
				df['factor'] = df['factor']/df['factor'].iloc[-1]
			else:
				df['factor'] = df['factor']/df['factor'].iloc[0]
			df[cols] = (df[cols] * df['factor'].values[:,np.newaxis]).round(2)
		if unit != '1m':
			agg = {'date':'last','open':'first','high':'max','low':'min','close':'last','volume':'sum',
				   'factor':'last','high_limit':'max','low_limit':'min'}
			how = {'date':'last'}
			for i in field_list:
				how.update({i:agg[i]})
			df['date'] = df.index.values
			if unit in ['5m','15m','30m','60m','120m']:				
				df = df.groupby(np.array(range(len(df)))//int(unit[:-1]))
			elif unit=='1d':
				df = df.groupby(lambda x:x.year+x.month*0.01+x.day*0.0001)
			elif unit=='1W':
				df = df.groupby(lambda x:x.year+x.week*0.01)
			elif unit=='1M':
				df = df.groupby(lambda x:x.year+x.month*0.01)
			else:
				raise Exception('only 5m,15m,30m,60m,120m,1d,1W,1M unit is available')
			df = df.agg(how).set_index('date')
		else:
			df = df[field_list]
		return df
		

