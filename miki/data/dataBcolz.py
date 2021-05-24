import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta
import time, bcolz, pickle, os
from miki.data.dataApi import DataApi
from miki.data.dataFunction import DataFunction
from miki.data import dataGlovar
		

class DataBcolz(object):
	# 行情数据模块：bcolz存储冷数据，redis缓存热数据
	def __init__(self):
		self.dataApi = DataApi()
		self.time_list = []
		self.check_today_data()

	@staticmethod
	def get_today_data():
		# 获取当天数据
		today_data,security_list,field_list = [],[],[]
		today_date_list = DataFunction.get_today_date_list()
		for now_time in today_date_list:
			key = now_time.strftime('%Y-%m-%d %H:%M:%S')
			if key in dataGlovar.redisCon:
				now_data, security_list, field_list = pickle.loads(dataGlovar.redisCon.get(key))
				today_data.append(now_data)
		today_data,security_list,field_list = np.array(today_data), list(security_list), list(field_list)
		today_data_1d = []
		if len(today_data)>0:
			d = today_data[-1,:,0]
			f = today_data[-1,:,1]
			o = today_data[0,:,2]
			h = today_data[:,:,3].max(axis=0)
			l = today_data[:,:,4].min(axis=0)
			c = today_data[-1,:,5]
			v = today_data[:,:,6].sum(axis=0)
			hl = today_data[0,:,7]
			ll = today_data[0,:,8]
			p = today_data[0,:,9]
			today_data_1d = np.stack([d,f,o,h,l,c,v,hl,ll,p], axis=-1)[np.newaxis,:,:]
		dataGlovar.today_data_1m = [today_data, security_list, field_list]	
		dataGlovar.today_data_1d = [today_data_1d, security_list, field_list]	
		return today_data, security_list, field_list

	def check_old_data(self, security_list):
		# 历史数据补全	
		for security in security_list:
			path = DataFunction.get_path(security, unit='1m')+'/date'
			if os.path.exists(path):
				start = datetime.utcfromtimestamp(bcolz.open(path, mode='r')[-1]+3600)
			else:
				start = pd.to_datetime('2005-01-01')
			end = pd.to_datetime(datetime.now().strftime('%Y-%m-%d 15:00:00'))
			array = self.dataApi.get_security_data(security, start, end)
			if len(array)>0:
				if len(array)%240!=0:
					raise Exception('{} length {}%240!=0'.format(security, len(array)))
				end_time = datetime.utcfromtimestamp(array[-1,0]).strftime('%H:%M:%S')
				if end_time!='15:00:00':
					raise Exception('{} end time {}'.format(security, end_time))
				array = array[:,np.newaxis,:]
				array_minute, array_day, security_list = self.transform_data(array, [security])
				if array_minute is not None:
					DataFunction.to_bcolz(security_list, array_minute, unit='1m')
					DataFunction.to_bcolz(security_list, array_day, unit='1d')
					print('{} {} {} {}'.format(security, start, end, array_minute.shape))
		print('check old data done !')

	def check_today_data(self):
		# 当天数据补全，用于盘中中断
		if datetime.now().date() in self.dataApi.get_all_trade_days():
			today_date_list = DataFunction.get_today_date_list()
			self.time_list = []
			for now_time in today_date_list:
				key = now_time.strftime('%Y-%m-%d %H:%M:%S')
				if key in dataGlovar.redisCon:
					self.time_list.append(now_time)
					now_data, security_list, field_list = pickle.loads(dataGlovar.redisCon.get(key))
				elif now_time not in self.time_list:
					now_data, security_list, now_time = self.dataApi.get_data(end_date=now_time)
					if now_data is not None:
						DataFunction.to_redis(dataGlovar.redisCon, now_data, security_list, now_time)
						self.time_list.append(now_time)
						print('{}'.format(now_time.strftime('%Y-%m-%d %H:%M:%S')))
		else:
			print('only check today data in trade_days and before 15:30')

	@staticmethod
	def transform_data(array1m, security_list):
		# 1m: 'date','factor','open','high','low','close','volume'
		# 1d: 'date','factor','open','high','low','close','volume','high_limit','low_limit','paused'				
		assert len(array1m.shape)==3, 'shape should be 3 dims, but got {}'.format(array1m.shape)
		assert (array1m.shape[0]%240==0 and array1m.shape[1]==1) or array1m.shape[0]==240,  'shape should like (240*N,1,dims) or (240,None,dims), but got {}'.format(array1m.shape)
		# 去除停牌数据
		if array1m.shape[1]==1:
			index = (array1m[:,:,-1]==0).any(axis=1)
			array1m = array1m[index,:,:]
		else:
			index = (array1m[:,:,-1]==0).transpose((1,0)).all(axis=1)
			array1m = array1m[:,index,:]	
			security_list = np.array(security_list)[index]
		if len(array1m)==0:
			return None, None, None
		list_of_array = np.split(array1m, len(array1m)//240, axis=0)
		def func(array):
			a1 = array[-1,:,0]
			a2 = array[-1,:,1]
			a3 = array[0,:,2]
			a4 = array[:,:,3].max(axis=0)
			a5 = array[:,:,4].min(axis=0)
			a6 = array[-1,:,5]
			a7 = array[:,:,6].sum(axis=0)
			a8 = array[-1,:,7]
			a9 = array[-1,:,8]
			a0 = array[-1,:,9]
			day_array = np.stack([a1,a2,a3,a4,a5,a6,a7,a8,a9,a0], axis=1)[np.newaxis,:,:]				
			return day_array
		array_day = [func(i) for i in list_of_array]
		if len(array_day)>1:
			array_day = np.concatenate(array_day, axis=0).transpose((1,2,0))
		else:
			array_day = np.array(array_day[0]).transpose((1,2,0))
		array_minute = array1m[:,:,:7].transpose((1,2,0))
		return array_minute, array_day, security_list

	def before_trading_start(self):
		self.dataApi.get_all_trade_days()
		self.dataApi.get_all_securities()
		dataGlovar.today_cache = {}
		dataGlovar.today_time_list = {}

	def run_every_minute(self):
		data, security_list, now_time = self.dataApi.get_data(end_date=datetime.now())
		if now_time.date() == datetime.now().date():
			if now_time not in self.time_list:
				DataFunction.to_redis(dataGlovar.redisCon, data, security_list, now_time)
				self.time_list.append(now_time)
				print(now_time.strftime('%Y-%m-%d %H:%M:%S'))
				if len(self.time_list) != len(DataFunction.get_today_date_list()):
					self.check_today_data()			

	def after_trading_end(self):
		today_data,security_list,field_list = DataBcolz.get_today_data()
		if len(today_data) != 240:
			raise Exception('wrong data length, shape {}'.format(today_data.shape))
		array_minute, array_day, security_list = self.transform_data(today_data, security_list)
		DataFunction.to_bcolz(security_list, array_minute, unit='1m')
		DataFunction.to_bcolz(security_list, array_day, unit='1d')
		self.time_list = []
		print('writing today data success')






