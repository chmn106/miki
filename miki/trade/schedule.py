from miki.trade import glovar as g
from miki.trade.context import Context
from miki.trade.logger import Logger
from miki.trade.function import MikiFunction
from datetime import datetime, timedelta
import pandas as pd
import pickle, os, time, redis


class Schedule(object):
	# 系统调度
	def __init__(self, run_params):
		self.init_glovar()
		self.save_path = run_params['pre_path']+'/file/{}.pkl'.format(run_params['name'])
		self.img_path = run_params['pre_path']+'/img/{}/{}'.format(run_params['mode'], run_params['name'])
		self.log_path = run_params['pre_path']+'/log/{}/{}'.format(run_params['mode'], run_params['name'])
		if run_params['mode']=='sim_trade' and os.path.exists(self.save_path):
			with open(self.save_path, 'rb') as f:
				g.context = pickle.load(f)
			g.log = Logger(path=self.log_path, clear_log=False)
		else:
			g.context = Context(run_params=run_params)
			g.log = Logger(path=self.log_path, clear_log=True)
			self.init_context()
			g.log.info('init context')
		g.run_params = run_params
		g.redisCon = redis.StrictRedis(host='127.0.0.1')

	def init_glovar(self):
		g.run_params = None # 运行参数
		g.redisCon = None # redis
		g.all_trade_days = None # 所有交易日
		g.display_name = None # 股票中文名，字典类型
		g.close_df = None
		g.factor_df = None
		g.highLimit_df = None
		g.lowLimit_df = None
		g.time_list = None # 回测时间列表
		g.context = None # 上下文会话context
		g.log = None # 日志
		g.tradeable = None # 用于判断bar数据结束是否可交易，bool类型或字典bool
		g.before_time = '09:00:00' # 盘前函数运行时间
		g.after_time = '15:05:00' # 盘后函数运行时间

	def init_context(self):
		# 初始化运行一次
		pass

	def before_trading_start(self):
		# 盘前运行
		pass

	def after_trading_end(self):
		# 盘后运行
		pass

	def after_backtest_end(self):
		# 回测结束运行
		pass

	def onMinute(self):
		pass

	def update_data(self, now_time):
		# 挂载数据的更新，需要自定义实现
		pass

	def is_tradeable(self, now_time):
		# 判断当前是否可交易
		if g.run_params['types']=='stocks':
			if now_time.strftime('%H:%M:%S') in ['11:30:00','15:00:00']:
				g.tradeable = False
			else:
				g.tradeable = True
		else:
			raise Exception('define tradeable time!')

	def backtest(self):
		start = pd.to_datetime(self.run_params['start'])
		end = pd.to_datetime(self.run_params['end'])
		# 获取交易时间序列
		self.update_data(start)
		# 运行盘前函数
		now_date = g.time_list[0].date()
		g.context.current_dt = g.time_list[0]
		g.context.before_trading_start()
		self.before_trading_start()
		for now_time in g.time_list:
			self.update_data(now_time)
			# 隔天先运行盘后函数，再运行盘前函数
			if now_time.date()!=now_date:
				g.context.after_trading_end()
				self.after_trading_end()				
				g.context.current_dt = now_time
				g.context.before_trading_start()
				self.before_trading_start()
				now_date = now_time.date()
			self.is_tradeable(now_time)
			g.context.onMinute(now_time)
			self.onMinute()
		self.after_backtest_end()
		MikiFunction.plot_value(value_history=g.context.valueHistory,
								order_history=g.context.orderHistory,
								begin_cash=g.context.starting_cash,
								end_cash=g.context.total_value,
								save_path=self.img_path,
								print_result=True,
								open_file=True)

	def simtrade(self):
		# 盘前、盘后运行时间
		before_time1, before_time2 = pd.to_datetime(g.before_time).time(), (pd.to_datetime(g.before_time)+timedelta(seconds=60*60*2)).time()
		after_time1, after_time2 = pd.to_datetime(g.after_time).time(), (pd.to_datetime(g.after_time)+timedelta(seconds=60*60*2)).time()
		while True:
			time.sleep(1)
			now_time = pd.to_datetime(datetime.now().strftime('%Y-%m-%d %H:%M:00'))
			if now_time.date() in g.all_trade_days:
				if before_time1<=now_time.time()<=before_time2 and not g.context.run_before_trading_start:
					g.context.current_dt = now_time
					g.context.before_trading_start()
					self.before_trading_start()
					with open(self.save_path, 'wb') as f:
						pickle.dump(g.context, f)
					print('{} run before_trading_start'.format(now_time))
				elif after_time1<=now_time.time()<=after_time2 and not g.context.run_after_trading_end:
					g.context.current_dt = now_time
					g.context.after_trading_end()
					self.after_trading_end()
					with open(self.save_path, 'wb') as f:
						pickle.dump(g.context, f)
					print('{} run after_trading_end'.format(now_time))
				else:
					fetch_data = self.update_data(now_time)
					if fetch_data:
						self.is_tradeable(now_time)
						g.context.onMinute(now_time)
						self.onMinute()
						with open(self.save_path, 'wb') as f:
							pickle.dump(g.context, f)
						MikiFunction.plot_portfolio(context=g.context,
													now_time=now_time,
													save_path=self.img_path,
													redisCon=g.redisCon,
													display_name=g.display_name)
						MikiFunction.plot_value(value_history=g.context.valueHistory,
												order_history=g.context.orderHistory,
												begin_cash=g.context.starting_cash,
												end_cash=g.context.total_value,
												save_path=self.img_path,
												print_result=False)

	def run(self):
		if self.run_params['mode']=='sim_trade':
			self.simtrade()
		else:
			self.backtest()





















