from datetime import datetime
import time
import pandas as pd 
from miki.data.dataApi import DataApi
from miki.data.dataBcolz import DataBcolz
from miki.data.dataFinance import DataFinance


class BasicSchedule(object):
	def __init__(self):
		self.dataApi = DataApi()
		self.__run_before = False
		self.__run_after = False
	def before_trading_start(self):
		# 盘前运行
		pass
	def run_every_minute(self):
		# 盘中
		pass
	def after_trading_end(self):
		# 盘后运行
		pass
	def run(self):			
		all_trade_days = self.dataApi.get_all_trade_days()	
		time1 = pd.to_datetime('08:00:00').time()
		time2 = pd.to_datetime('09:00:00').time()
		time3 = pd.to_datetime('09:31:00').time()
		time4 = pd.to_datetime('11:30:00').time()
		time5 = pd.to_datetime('13:00:00').time()
		time6 = pd.to_datetime('15:01:00').time()
		time7 = pd.to_datetime('15:05:00').time()
		time8 = pd.to_datetime('15:10:00').time()
		time_list = []
		while True:
			time.sleep(1)
			now_time = datetime.now()
			if now_time.date() in all_trade_days:
				if not self.__run_before and time1<=now_time.time()<=time2:
					t1 = time.time()
					all_trade_days = self.dataApi.get_all_trade_days()
					self.before_trading_start()
					self.__run_before = True
					t2 = time.time()
					print('run_before_trading_start use {} s'.format(int(t2-t1)))
				if time3<=now_time.time()<=time4 or time5<=now_time.time()<=time6:
					self.__run_before = False
					self.__run_after = False
					key = now_time.strftime('%Y-%m-%d %H:%M:00')
					if key not in time_list:
						self.run_every_minute()
						time_list.append(key)
				if not self.__run_after and time7<=now_time.time()<=time8:
					t1 = time.time()
					self.after_trading_end()
					self.__run_after = True
					t2 = time.time()
					print('run_after_trading_end use {} s'.format(int(t2-t1)))					
			else:
				time.sleep(3600)
	
class MikiData(BasicSchedule):
	def __init__(self, **kwarg):
		super(MikiData, self).__init__()
		self.dataBcolz = DataBcolz()
		self.dataFinance = DataFinance()
		self.dataOthers = kwarg

	def before_trading_start(self):
		self.dataBcolz.before_trading_start()
		print('run dataBcolz before_trading_start')		
		self.dataFinance.before_trading_start()
		print('run dataFinance before_trading_start')		
		for key in self.dataOthers:
			self.dataOthers[key].before_trading_start()
			print('run {} before_trading_start'.format(key))

	def run_every_minute(self):
		self.dataBcolz.run_every_minute()
		for key in self.dataOthers:
			self.dataOthers[key].run_every_minute()
		
	def after_trading_end(self):
		self.dataBcolz.after_trading_end()
		print('run dataBcolz after_trading_end')
		self.dataFinance.after_trading_end()
		print('run dataFinance after_trading_end')		
		for key in self.dataOthers:
			self.dataOthers[key].after_trading_end()
			print('run {} after_trading_end'.format(key))







