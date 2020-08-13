import pandas as pd 
import numpy as np
import time
from datetime import datetime, timedelta
import os, sys
import redis
import pickle
from system.data.query import Query
from system.data.dataGlovar import DataPath


class DataGenerator(object):
	'''
		1.数据的推送格式为均为dataframe格式，column是股票列表，index为数据类型列表

	'''
	def __init__(self, target_stocks):
		self.redis_conn = redis.StrictRedis(host='127.0.0.1')
		self.query = Query()
		self.push_list = []
		self.todayData_field = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']

	def get_data_sim(self, now_time):
		now_data = self.redis_conn.get(str(now_time))
		if now_data is not None:
			data, security_list = pickle.loads(now_data)
			df = pd.DataFrame(data, index=security_list, columns=self.todayData_field)
			now_time = datetime.utcfromtimestamp(data[0,0])
			if now_time not in self.push_list:
				self.push_list.append(now_time)
				return [df.T, now_time]
			else:
				time.sleep(1)
		else:
			time.sleep(1)

	def get_data_back(self, run_params):
		day_list = self.query.get_time_list(run_params['start_date'], run_params['end_date'], unit=240)
		last_month = None
		for now_day in day_list:
			now_day = pd.to_datetime(now_day.date())
			if last_month is None or now_day.strftime('%Y-%m') != last_month:
				with open(DataPath+'/cache/query/{}.pkl'.format(now_day.strftime('%Y-%m')), 'rb') as f:
					data_dict = pickle.load(f)
				last_month = now_day.strftime('%Y-%m')
			panel = data_dict[now_day]
			time_list = panel.axes[1]
			for i in range(len(time_list)):
				now_time = time_list[i]
				dataUnit = panel.iloc[:,i,:]
				yield [dataUnit, now_time]
				
if __name__ == '__main__':
	d = DataGenerator()
	

