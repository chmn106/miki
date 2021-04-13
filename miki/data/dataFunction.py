import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta
import bcolz, pickle, os
from miki.data import dataGlovar


class DataFunction(object):
	@staticmethod
	def get_path(security, unit='1m'):
		# 存储路径设置
		if [security[:3], security[-1]] in [['300','E']] or [security[0], security[-1]] in [['6','G'],['0','E']]:
			param1 = 'stock'
			param2 = security[4:6]		
		elif [security[:3], security[-1]] in [['399','E']] or [security[0], security[-1]] in [['0','G']]:
			param1 = 'index'
			param2 = security[4:6]
		elif security[0] in ['1','5']:
			param1 = 'fund'
			param2 = security[4:6]			
		else:
			raise Exception('{} unknown name type'.format(security))
		param1 += unit
		path = '{}/{}/{}/{}'.format(dataGlovar.DataPath, param1, param2, security)
		return path
	@staticmethod
	def get_today_date_list():
		# 获取当天交易时间列表
		prefix = datetime.now().strftime('%Y-%m-%d')
		t1 = pd.to_datetime(prefix+' 09:31:00')
		t2 = pd.to_datetime(prefix+' 11:30:00')
		t3 = pd.to_datetime(prefix+' 13:01:00')
		t4 = pd.to_datetime(prefix+' 15:00:00')
		t5 = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:00'))
		date_list = []
		c = t1
		for i in range(480):
			date_list.append(c)
			c += timedelta(seconds=60)
		date_list = [i for i in date_list if ((t1<=i<=t2 or t3<=i<=t4) and i<=t5)]
		return date_list
	@staticmethod
	def to_bcolz(security_list, data, unit):		
		# shape: (num, dims, length)
		for i,security in enumerate(security_list):
			if unit=='1d':
				names = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']
			else:
				names = ['date','factor','open','high','low','close','volume']
			path = DataFunction.get_path(security, unit)
			array = data[i].astype('float')
			if not os.path.exists(path):
				os.makedirs(path, exist_ok=True)
				table = bcolz.ctable(rootdir=path, columns=list(array), names=names, mode='w')
				table.flush()
			else:
				# 进行数据检查
				table = bcolz.open(path, mode='a')					
				date_index = table.names.index('date')
				array = array[:,array[0,:]>table[-1][date_index]]
				array = list(map(lambda x:tuple(x), array))
				table.append(array)
				table.flush()
	@staticmethod
	def to_redis(redisCon, now_data, security_list, now_time):
		# 数据字段：'date','factor','open','high','low','close','volume','high_limit','low_limit','paused'		
		now_time = str(now_time)
		field_list = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']
		redisCon.set(now_time, pickle.dumps([now_data, security_list, field_list]))
		redisCon.expire(now_time, 60*60*24)
