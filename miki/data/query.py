import pandas as pd 
import numpy as np
from datetime import datetime, timedelta
import os, sys, pickle, bcolz
from miki.data import dataGlovar
from miki.data.dataFunction import DataFunction
from miki.data.dataBcolz import DataBcolz


class Query(object):
	def __init__(self):
		self.__time1 = pd.to_datetime('09:30:00').time()
		self.__time2 = pd.to_datetime('15:00:00').time()	
		self.base_path = dataGlovar.DataPath+'/pickle'
		os.makedirs(self.base_path, exist_ok=True)
		self.last_end = None
		self.cache_data = {}
		self.cache_time = None
	def get_security_info(self):
		# 获取所有股票信息
		df = pd.read_pickle(dataGlovar.DataPath+'/stock_info.pkl')
		return df
	def get_security_list(self, types=['stock']):
		df = df = pd.read_pickle(dataGlovar.DataPath+'/stock_info.pkl')
		df = df[df.type.isin(types)]
		return df.index.values.tolist()	
	def get_all_trade_days(self):
		# 获取所有交易日
		with open(dataGlovar.DataPath+'/all_trade_days.pkl', 'rb') as f:
			all_trade_days = pickle.load(f)
		return all_trade_days		
	def get_time_list(self, start, end, unit):
		# 获取交易时间列表
		def func(x):
			return (x.hour-9)*60+x.minute-30 if x.hour<=11 else (x.hour-13+2)*60+x.minute
		start,end = pd.to_datetime(start),pd.to_datetime(end)
		date_list = bcolz.open(DataFunction.get_path('000001.XSHG', unit='1m')+'/date', mode='r')
		date_list = [datetime.utcfromtimestamp(i) for i in date_list]
		series = pd.Series(date_list, index=date_list)
		series = series[(series.index>=start)&(series.index<=end)]
		if len(series) == 0: return []
		series = series.apply(func)
		return list(series[series.values%unit==0].index)
	def get_valuation_x(self, name, date):
		# 获取某天的指标数据
		assert name in ['valuation','indicator'], 'name should be valuation or indicator'
		df = pd.read_pickle(dataGlovar.DataPath+'/finance/{}/{}.pkl'.format(name, str(pd.to_datetime(date).date())))
		return df
	def get_valuation_y(self, name):
		# 获取某个指标的数据
		for types in ['valuation','indicator']:
			path = dataGlovar.DataPath+'/finance/{}/{}.pkl'.format(types, name)
			if os.path.exists(path):
				df = pd.read_pickle(path)
				return df
		return None
	def get_dataframe(self, year, field, unit, update):
		# 获取某年的数据，用于回测推送
		start = pd.to_datetime('{}-01-01'.format(year))
		end = pd.to_datetime('{}-01-01'.format(year+1))
		cache_path = self.base_path+'/{}-{}-{}.pkl'.format(field, unit, year)
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
	def get_today_data(self, fq='post'):
		# 获取当天数据
		today_data, security_list, field_list = DataBcolz.get_today_data()
		if fq=='post':
			factor_list = []
			for security in security_list:
				path = DataFunction.get_path(security, '1d')+'/factor'
				if os.path.exists(path):
					init_factor = bcolz.open(path, mode='r')[0]
				else:
					init_factor = 1
				factor_list.append(init_factor)
			ix = [field_list.index(i) for i in ['open','high','low','close','high_limit','low_limit']]
			today_data[:,:,ix] = today_data[:,:,ix] * np.array(factor_list)[np.newaxis,:,np.newaxis]
		return today_data, security_list, field_list		
	def get_stock(self, security, end, field_list, unit, start=None, limit=None, fq=None, use_cache=False):
		# 获取单只股票数据 use_cache会在内存缓存数据 
		end = pd.to_datetime(pd.to_datetime(end).strftime('%Y-%m-%d %H:%M:00'))
		if use_cache and security in self.cache_data and end.date()==self.cache_time:
			df = self.cache_data[security].copy()
		else:
			if unit in ['1m','5m','15m','30m','60m','120m']:
				path = DataFunction.get_path(security, '1m')
				if not os.path.exists(path): return pd.DataFrame(columns=field_list)			
				outcols = ['date','factor'] + [i for i in field_list if i in ['open','high','low','close','volume']]
			elif unit in ['1d','1W','1M']:
				path = DataFunction.get_path(security, '1d')
				if not os.path.exists(path): return pd.DataFrame(columns=field_list)
				outcols = ['date','factor'] + [i for i in field_list if i in ['open','high','low','close','volume','high_limit','low_limit']]
			else:
				raise Exception('only 1m,5m,15m,30m,60m,120m,1d,1W,1M unit is available, but got {}'.format(unit))
			ctable = bcolz.open(path, mode='r')
			if limit:
				array = ctable.fetchwhere('date<={}'.format(end.timestamp()), outcols=outcols)[-limit:]
				df = pd.DataFrame(array, columns=outcols)
			else:	
				start = pd.to_datetime(pd.to_datetime(start).date())
				ctable = ctable.fetchwhere('(date<={})&(date>={})'.format(end.timestamp(), start.timestamp()), outcols=outcols)
				df = ctable.todataframe()
			if use_cache:
				self.cache_time = end.date()
				self.cache_data[security] = df.copy()
		end_timestamp = df['date'].iloc[-1] if len(df)>0 else 0
		if end.date()>=datetime.now().date() and self.__time1<datetime.now().time()<self.__time2:
			if self.last_end is None or self.last_end!=end: # 同一个end不重复提取数据
				DataBcolz.get_today_data()
				self.last_end = end
			if unit in ['1d','1W','1M']:
				today_data, securities, fields = dataGlovar.today_data_1d
			else:
				today_data, securities, fields = dataGlovar.today_data_1m
			if len(today_data)>0 and security in securities:	
				if today_data[0,0,0]>end_timestamp:	# 检查数据是否已经写入本地数据库		
					id1,id2 = securities.index(security), [fields.index(i) for i in df.columns]
					if not today_data[-1,id1,-1]: # 不停牌
						today_df = pd.DataFrame(today_data[:,id1,id2], columns=df.columns)		
						df = pd.concat([df, today_df], axis=0)
		if len(df)==0:
			return pd.DataFrame(columns=field_list)
		df['date'] = df['date'].apply(lambda x: datetime.utcfromtimestamp(x))
		df = df.set_index('date')
		if 'volume' in field_list:
			df['volume'] = df['volume'] * df['factor'] # 成交量需要除权
		if fq:
			cols = [i for i in field_list if i in ['open','high','low','close']]
			if fq=='pre':
				df['factor'] = df['factor']/df['factor'].iloc[-1]
			else:
				path = DataFunction.get_path(security, '1d')+'/factor'
				if os.path.exists(path):
					init_factor = bcolz.open(path, mode='r')[0]
				else:
					init_factor = 1
				df['factor'] = df['factor']/init_factor
			df[cols] = (df[cols] * df['factor'].values[:,np.newaxis]).round(2)
		if unit in ['5m','15m','30m','60m','120m','1W','1M']:
			agg = {'date':'last','open':'first','high':'max','low':'min','close':'last','volume':'sum',
				   'factor':'last','high_limit':'max','low_limit':'min'}
			how = {'date':'last'}
			for i in field_list:
				how.update({i:agg[i]})
			df['date'] = df.index.values
			if unit in ['5m','15m','30m','60m','120m']:				
				df = df.groupby(np.array(range(len(df)))//int(unit[:-1]))
			elif unit=='1W':
				df = df.groupby(lambda x:x.year+x.week*0.01)
			elif unit=='1M':
				df = df.groupby(lambda x:x.year+x.month*0.01)
			df = df.agg(how).set_index('date')
		else:
			df = df[field_list]
		return df
		

