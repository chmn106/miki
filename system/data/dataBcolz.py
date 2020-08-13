import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta
import time
import bcolz
import redis
import pickle
import os, sys
from dataApi import DataApi
from dataGlovar import DataPath

		
class DataBcolz(object):
	# 行情数据模块：bcolz存储冷数据，redis缓存热数据
	def __init__(self):
		self.redis_conn = redis.StrictRedis(host='127.0.0.1')
		self.dataApi = DataApi()
		self.init_params()
		self.check_today_data()

	def init_params(self):
		self.time_list = []

	@staticmethod
	def to_redis(redis_conn, now_data, security_list, now_time, prefix=''):
		# 数据字段：'date','factor','open','high','low','close','volume','high_limit','low_limit','paused'		
		now_time = prefix+str(now_time)
		redis_conn.set(now_time, pickle.dumps([now_data, security_list]))
		redis_conn.expire(now_time, 60*60*24)

	@staticmethod
	def get_today_date_list():
		# 获取当天交易时间列表
		t1 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')+' 09:31:00')
		t2 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')+' 11:30:00')
		t3 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')+' 13:01:00')
		t4 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d')+' 15:00:00')
		t5 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M')+':00')
		date_list = []
		c = t1
		for i in range(480):
			date_list.append(c)
			c += timedelta(seconds=60)
		date_list = [i for i in date_list if ((t1<=i<=t2 or t3<=i<=t4) and i<=t5)]
		return date_list

	@staticmethod
	def get_today_data(redis_conn, prefix=''):
		# 获取当天数据
		today_data = [] 
		today_date_list = DataBcolz.get_today_date_list()
		for now_time in today_date_list:
			now_time = prefix+now_time.strftime('%Y-%m-%d %H:%M:%S')
			if now_time.encode('utf-8') in redis_conn.keys():
				now_data, security_list = pickle.loads(redis_conn.get(now_time))
				today_data.append(now_data)
		return np.array(today_data), list(security_list)

	@staticmethod
	def get_path(security, unit='1m'):
		# 存储路径设置	
		if [security[:3], security[-1]] in [['300','E']] or [security[0], security[-1]] in [['6','G'],['0','E']]:
			param1 = 'stock'
			param2 = security[4:6]		
		elif [security[:3], security[-1]] in [['399','E']] or [security[0], security[-1]] in [['0','G']]:
			param1 = 'index'
			param2 = security[4:6]
		else:
			raise Exception('{} unknown security name type'.format(security))
		param1 += unit
		path = DataPath+'/{}/{}/{}'.format(param1,param2,security)				
		return path

	def check_old_data(self):
		# 历史数据补全，盘后进行
		t1 = datetime.strptime('09:00:00', '%H:%M:%S').time()
		t2 = datetime.strptime('15:10:00', '%H:%M:%S').time()
		all_trade_days = self.dataApi.get_all_trade_days()
		all_trade_days = [i for i in all_trade_days if i<=datetime.now().date()]
		if datetime.now().time()<t1 or datetime.now().time()>t2 or datetime.now().date() not in all_trade_days:
			all_securities = self.dataApi.get_all_securities(types=['stock','index'])	
			if datetime.now().time() < datetime.strptime('15:00:00', '%H:%M:%S').time() and datetime.now().date() in all_trade_days:
				end = pd.to_datetime(str(all_trade_days[-1])+' 00:00:00')
			else:
				end = pd.to_datetime(str(all_trade_days[-1])+' 15:00:00')
			for security in all_securities:
				# to_bcolz模块会检查数据日期进行插入
				start = pd.to_datetime(all_trade_days[-2])
				array = self.dataApi.get_security_data(security, start, end)
				if len(array)>0:
					array = array[:,np.newaxis,:]
					array_minute, array_day, security_list = self.transform_data(array, [security])
					if array_minute is not None:
						self.to_bcolz(security_list, array_minute, is_day=False)
						self.to_bcolz(security_list, array_day, is_day=True)
						print('{} {} {} {}'.format(security, start, end, array_minute.shape))
			print('check old data done !')
		else:
			print('only check old data after 15:10 or not in trade_days')

	def check_today_data(self):
		# 当天数据补全，用于盘中中断
		if datetime.now().date() in self.dataApi.get_all_trade_days():
			today_date_list = self.get_today_date_list()
			self.time_list = []
			for now_time in today_date_list:
				if now_time.strftime('%Y-%m-%d %H:%M:%S').encode('utf-8') in self.redis_conn.keys():
					self.time_list.append(now_time)
					now_data, security_list = pickle.loads(self.redis_conn.get(now_time.strftime('%Y-%m-%d %H:%M:%S')))
				elif now_time not in self.time_list:
					now_data, security_list, now_time = self.dataApi.get_data(end_date=now_time)
					if now_data is not None:
						DataBcolz.to_redis(self.redis_conn, now_data, security_list, now_time)
						self.time_list.append(now_time)
						print('{}'.format(now_time.strftime('%Y-%m-%d %H:%M:%S')))
		else:
			print('only check today data in trade_days and before 15:30')

	@staticmethod
	def transform_data(data, security_list):
		# 1分钟数据合成日级数据，转为bcolz的存储格式
		# data: 'date','factor','open','high','low','close','volume','high_limit','low_limit','paused'
		# 1m: 'date','factor','open','high','low','close','volume'
		# 1d: 'date','factor','open','high','low','close','volume','high_limit','low_limit','paused'				
		data = np.array(data)
		assert len(data.shape)==3, 'shape should be 3 dims, but got {}'.format(data.shape)
		assert (data.shape[0]%240==0 and data.shape[1]==1) or data.shape[0]==240, \
		'shape should like (240*N,1,dims) or (240,None,dims), but got {}'.format(data.shape)
		# 去除停牌数据
		if data.shape[1]==1:
			index = (data[:,:,-1]==0).any(axis=1)
			data = data[index,:,:]
		else:
			index = (data[:,:,-1]==0).transpose((1,0)).all(axis=1)
			data = data[:,index,:]	
			security_list = np.array(security_list)[index]
		if len(data)==0:
			return None, None, None

		list_of_array = np.split(data, len(data)//240, axis=0)
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
		array_minute = data[:,:,:7].transpose((1,2,0))
		return array_minute, array_day, security_list

	@staticmethod
	def to_bcolz(security_list, data, is_day, target_path=None, names=None):
		# data shape (security_list, dims, length)
		for i,security in enumerate(security_list):
			if is_day:
				unit = '1d'
				if names is None:
					names = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']
			else:
				unit = '1m'
				if names is None:
					names = ['date','factor','open','high','low','close','volume']
			path = DataBcolz.get_path(security, unit) if target_path is None else target_path

			array = data[i].astype('float')
			if not os.path.exists(path):
				os.makedirs(path, exist_ok=True)
				table = bcolz.ctable(rootdir=path,
									 columns=list(array),
									 names=names,
									 mode='w')
				table.flush()
			else:
				# 进行数据检查
				table = bcolz.open(path, mode='a')					
				date_index = table.names.index('date')
				array = array[:,array[0,:]>table[-1][date_index]]
				array = list(map(lambda x:tuple(x), array))
				table.append(array)
				table.flush()

	def run_before_trading_start(self):
		self.dataApi.login()		
		self.dataApi.get_all_trade_days()
		self.dataApi.get_all_securities(types=['stock','index'])

	def run_after_trading_end(self):
		today_data,security_list = DataBcolz.get_today_data(self.redis_conn)
		if len(today_data) != 240:
			raise Exception('wrong data length, shape {}'.format(today_data.shape))
		end_date = datetime.utcfromtimestamp(today_data[0,0,0]).date()
		if end_date != datetime.now().date():
			raise Exception('wrong time {}'.format(end_date))			
		array_minute, array_day, security_list = self.transform_data(today_data, security_list)
		if array_minute is not None:									
			self.to_bcolz(security_list, array_minute, is_day=False)
			self.to_bcolz(security_list, array_day, is_day=True)
		self.check_data()
		print('writing today data success，please check today data !!!')
		self.init_params()
		self.dataApi.logout()

	def check_data(self):
		# 检查是否写入成功
		path = DataBcolz.get_path('000001.XSHG', '1d')+'/date'
		date = datetime.utcfromtimestamp(bcolz.open(path, mode='r')[-1]).date()
		if datetime.now().date() != date:
			raise Exception('writing today data wrong, pre date is {}'.format(date))

	def run_every_minute(self):
		data, security_list, now_time = self.dataApi.get_data(end_date=datetime.now())
		if now_time.date() == datetime.now().date():
			if now_time not in self.time_list:
				DataBcolz.to_redis(self.redis_conn, data, security_list, now_time)
				self.time_list.append(now_time)
				print(now_time.strftime('%Y-%m-%d %H:%M:%S'))
				if len(self.time_list) != len(self.get_today_date_list()):
					self.check_today_data()			

if __name__ == '__main__':
	d = DataBcolz()




