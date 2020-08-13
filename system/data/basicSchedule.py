from datetime import datetime 
import time
from query import Query


class BasicSchedule(object):
	# 时间调度模块
	def __init__(self):
		self.query = Query()
		self.all_trade_days = None
		self.system_run_before_trading_start = False
		self.system_run_after_trading_end = False

	def run_before_trading_start(self):
		# 盘前运行
		pass

	def run_every_day(self):
		# 每天05:00运行
		pass

	def run_every_minute(self):
		# 盘中
		pass

	def run_after_trading_end(self):
		# 盘后运行
		pass
		
	def run(self):				
		time1 = datetime.strptime('07:00:00', '%H:%M:%S').time()
		time2 = datetime.strptime('09:00:00', '%H:%M:%S').time()

		time3 = datetime.strptime('09:31:00', '%H:%M:%S').time()
		time4 = datetime.strptime('11:30:00', '%H:%M:%S').time()
		time5 = datetime.strptime('13:00:00', '%H:%M:%S').time()
		time6 = datetime.strptime('15:01:00', '%H:%M:%S').time()

		time7 = datetime.strptime('15:05:00', '%H:%M:%S').time()
		time8 = datetime.strptime('15:10:00', '%H:%M:%S').time()

		time9 = datetime.strptime('05:00:00', '%H:%M:%S').time()
		now_time = datetime.strptime('00:00:00', '%H:%M:%S').time()

		while True:
			# 每一秒钟运行一次，避免占用CPU计算
			time.sleep(1)

			if self.all_trade_days is None or datetime.now().date()!=self.time_count.date():
				self.all_trade_days = self.query.get_all_trade_days()
				self.time_count = datetime.now()

			if datetime.now().date() in self.all_trade_days:
				if datetime.now().time() == time9:
					t1 = time.time()
					self.run_every_day()
					t2 = time.time()
					print('run_every_day use {}s'.format(t2-t1))

				if not self.system_run_before_trading_start and time1<=datetime.now().time()<=time2:
					t1 = time.time()
					self.run_before_trading_start()
					self.system_run_before_trading_start = True
					t2 = time.time()
					print('run_before_trading_start use {}s'.format(t2-t1))

				while time3<=datetime.now().time()<=time4 or time5<=datetime.now().time()<=time6:
					self.system_run_before_trading_start = False
					self.system_run_after_trading_end = False
					if [datetime.now().hour, datetime.now().minute] != [now_time.hour, now_time.minute]:
						now_time = datetime.now()
						self.run_every_minute()				
					else:
						time.sleep(1)

				if not self.system_run_after_trading_end and time7<=datetime.now().time()<=time8:
					t1 = time.time()
					self.run_after_trading_end()
					self.system_run_after_trading_end = True
					t2 = time.time()
					print('run_after_trading_end use {}s'.format(t2-t1))					
			else:
				time.sleep(60)





	