from datetime import datetime 
import numpy as np 
import pandas as pd
import sys,os
import pickle
import time
import importlib
import pyecharts.options as opts
from pyecharts.charts import Line
from pyecharts.components import Table
import redis

from system.trade.context import Context
from system.data.query import Query
from system.trade import glovar
from system.trade.logger import Logger
from system.trade.dataGenerator import DataGenerator


class System(object):
	'''
	   1.系统主引擎，调用各个部件
	   2.股票、期货、期权分为独立部分，0股票1期货2期权
	   3.目前暂未实现期货功能

	'''
	def __init__(self, run_params, load_context, target_stocks=None):
		super(System, self).__init__()	
		self.run_params = run_params
		self.do_load_context = load_context	
		self.strategy = None
		self.dataGenerator = DataGenerator(target_stocks)
		self.init_system()

	def init_system(self, run_params=None):
		if run_params is not None:
			self.run_params = run_params

		glovar.context = Context(run_params=self.run_params)
		glovar.query = Query(mode=self.run_params['mode'], context=glovar.context)
		glovar.log = Logger(log_path='/{}/{}'.format(self.run_params['mode'], self.run_params['strategy']))
		glovar.redis_con = redis.StrictRedis(host='127.0.0.1')
		
		if self.strategy is None:
			self.strategy = importlib.import_module('system.strategy.'+self.run_params['strategy'])
		self.strategy.initialize(context=glovar.context)

		self.order_history = []
		self.betaStock = glovar.context.betaStock
		self.all_trade_days = glovar.query.get_all_trade_days()
		self.stock_info = glovar.query.get_security_info()
		if self.do_load_context and self.run_params['mode']=='sim_trade':
			self.load_context()			
		os.makedirs(sys.path[0]+'/img/{}/{}'.format(self.run_params['mode'], self.run_params['strategy']), exist_ok=True)		
		os.makedirs(sys.path[0]+'/log/{}/{}'.format(self.run_params['mode'], self.run_params['strategy']), exist_ok=True)

	def save_context(self):
		with open(sys.path[0]+'/system/trade/cache/{}-context.pkl'.format(self.run_params['strategy']), 'wb') as f:
			pickle.dump(glovar.context, f)

	def load_context(self):
		path = sys.path[0]+'/system/trade/cache/{}-context.pkl'.format(self.run_params['strategy'])
		if os.path.exists(path):
			with open(path, 'rb') as f:
				glovar.context = pickle.load(f)
			glovar.log.info('load context success')

	def run_backtest(self, do_plot=True):
		generator = self.dataGenerator.get_data_back(self.run_params)		
		day_start,day_end = '09:31','15:00'
		for [data_df, now_time] in generator:
			glovar.log.update(now_time)
			glovar.context.run_every_minute(data_df=data_df, now_time=now_time, stock_info=self.stock_info)
			if now_time.strftime('%H:%M') == day_start:
				glovar.context.run_before_trading_start()
				if hasattr(self.strategy, 'before_trading_start'):
					self.strategy.before_trading_start(context=glovar.context)
			self.strategy.handle_data(context=glovar.context, data=data_df)
			if now_time.strftime('%H:%M') == day_end:
				self.order_history.extend(glovar.context.filled_orders)
				glovar.context.run_after_trading_end()
				if hasattr(self.strategy, 'after_trading_end'):
					self.strategy.after_trading_end(context=glovar.context)					
		if hasattr(self.strategy, 'after_backtest_end'):
			self.strategy.after_backtest_end(context=glovar.context)
		if do_plot:
			self.plot()

	def run_simtrade(self):
		time1 = datetime.strptime('09:00:00', '%H:%M:%S').time()
		time2 = datetime.strptime('09:30:00', '%H:%M:%S').time()

		time3 = datetime.strptime('09:31:00', '%H:%M:%S').time()
		time4 = datetime.strptime('11:30:00', '%H:%M:%S').time()
		time5 = datetime.strptime('13:00:00', '%H:%M:%S').time()
		time6 = datetime.strptime('15:00:00', '%H:%M:%S').time()

		time7 = datetime.strptime('15:00:00', '%H:%M:%S').time()
		time8 = datetime.strptime('16:00:00', '%H:%M:%S').time()
		time9 = datetime.strptime('01:00:00', '%H:%M:%S').time()
		run_before_trading_start = False
		run_after_trading_end = False
		now_time = datetime.strptime('00:00:00', '%H:%M:%S').time()
		
		while True:
			time.sleep(1)
			if datetime.now().strftime('%H:%M:%S') == '08:00:00':
				self.all_trade_days = glovar.query.get_all_trade_days()
				self.stock_info = glovar.query.get_security_info()

			if datetime.now().time()<=time9:
				run_before_trading_start = False
				run_after_trading_end = False						

			if datetime.now().date() in self.all_trade_days:
				if not run_before_trading_start and time1<=datetime.now().time()<=time2:
					glovar.context.current_dt = datetime.now()
					if hasattr(self.strategy, 'before_trading_start'):
						self.strategy.before_trading_start(context=glovar.context)
						glovar.log.debug('run before_trading_start success')						
					glovar.context.run_before_trading_start()
					glovar.query.run_before_trading_start()
					self.save_context()
					run_before_trading_start = True				

				while time3<=datetime.now().time()<=time4 or time5<=datetime.now().time()<=time6:
					run_before_trading_start = False
					run_after_trading_end = False						
					if datetime.now().strftime('%H:%M') != now_time.strftime('%H:%M'):
						data = self.dataGenerator.get_data_sim(now_time=datetime.now().strftime('%Y-%m-%d %H:%M')+':00')
						if data is not None:
							[data_df, now_time] = data
							glovar.log.update(now_time)
							glovar.context.run_every_minute(data_df=data_df, now_time=now_time, stock_info=self.stock_info)
							self.strategy.handle_data(context=glovar.context, data=data_df)
							self.save_context()
							self.plot_portfolio()

				if not run_after_trading_end and time7<=datetime.now().time()<=time8:
					now_time = datetime.now()
					glovar.context.current_dt = now_time						
					if hasattr(self.strategy, 'after_trading_end'):
						self.strategy.after_trading_end(context=glovar.context)
						glovar.log.debug('run after_trading_end success')
					glovar.context.run_after_trading_end()
					glovar.query.run_after_trading_end()
					self.save_context()
					self.plot()
					run_after_trading_end = True		

	def calculate(self):
		def func(x):
		    rate = 1-x[-1]/x.max()
		    return rate

		total_commission,total_slipcost,time_list,profitRate_array,profit_array = 0,0,[],[],[]
		for order in self.order_history:
			total_commission += order.commission
			total_slipcost += order.slipcost
			time_list.append(order.add_time)
			if not order.is_buy:
				rate = order.profit/(order.filled*order.price-order.profit)
				profit_array.append(order.profit)
				profitRate_array.append(rate)
		total_counts = len(self.order_history)
		profit_array = np.array(profit_array)
		profitRate_array = np.array(profitRate_array)
		value_history = np.array(glovar.context.value_history)

		begin_cash = glovar.context.portfolio.starting_cash
		end_cash = glovar.context.portfolio.total_value
		if len(profit_array)>0:
			avg_profitRate = profitRate_array.mean()
			max_profitRate = profitRate_array.max()
			yl_array = profit_array[profit_array>0]
			ks_array = profit_array[profit_array<=0]
			win_rate = len(yl_array) / max(len(profit_array),1)
			yl_times,yl_avg,ks_times,ks_avg,yk_ratio,max_yl_rate,max_ks_rate = 0,0,0,0,0,0,0
			if len(yl_array) > 0:
				max_yl_rate = yl_array.max()/yl_array.sum()
				yl_times = len(yl_array)
				yl_avg = yl_array.mean()
			if len(ks_array) > 0:
				max_ks_rate = ks_array.min()/ks_array.sum()
				ks_times = len(ks_array)
				ks_avg = ks_array.mean()
				yk_ratio = abs(yl_avg/ks_avg)

		date_array = value_history[:,0]
		value_array = value_history[:,1].astype('float')
		retrace_array = np.array(list(map(lambda x: func(value_array[:x]), range(1,len(value_array)+1))))
		max_retrace = retrace_array.max()
		max_retrace_date = date_array[retrace_array.argmax()].date()
		max_return = value_array.max()/value_array[0]-1
		min_return = value_array.min()/value_array[0]-1
		value_array = value_array/value_array[0]-1

		if len(profit_array)>0:
			message = '''
						 首个交易日：{} 
						 最后交易日：{} 
						 起始资金：{}
						 结束资金：{}
						 总收益率：{:.2%}         			      			
						 总交易次数：{}
						 总佣金：{:.2f}
						 平均每笔佣金：{:.2f}
						 总滑点：{:.2f}
						 平均每笔滑点：{:.2f}
						 胜率：{:.2%}
						 最高收益率：{:.2%}
						 最低收益率：{:.2%}
						 最大回撤率：{:.2%}
						 最大回撤发生时间：{}  
						 盈利次数：{}
						 亏损次数：{}
						 平均每笔盈利：{:.2f}
						 平均每笔亏损：{:.2f}
						 盈亏比：{:.2f}
						 平均每笔收益率：{:.2%}
						 单笔最大收益率：{:.2%}
						 最大收益占比：{:.2%}
						 最大亏损占比：{:.2%}
					  '''.format(time_list[0], \
					  			 time_list[-1], \
					  			 begin_cash, \
					  			 end_cash, \
					  			 end_cash/begin_cash-1, \
					  			 total_counts, \
					  			 total_commission, \
					  			 total_commission/total_counts, \
					  			 total_slipcost, \
					  			 total_slipcost/total_counts, \
					  			 win_rate, \
					  			 max_return, \
					  			 min_return, \
					  			 max_retrace, \
					  			 max_retrace_date, \
					  			 yl_times, \
					  			 ks_times, \
					  			 yl_avg, \
					  			 -ks_avg, \
					  			 yk_ratio, \
					  			 avg_profitRate, \
					  			 max_profitRate, \
					  			 max_yl_rate, \
					  			 max_ks_rate)
			if self.run_params['mode'] == 'backtest':
				glovar.log.info(message)
		return value_array,date_array,max_retrace,max_retrace_date,max_return,min_return

	def plot(self):
		# 资金曲线
		value_array,date_array,max_retrace,max_retrace_date,max_return,min_return = self.calculate()
		date_list = [i.strftime('%Y-%m-%d') for i in date_array]
		subtitle = "最大回撤 {}  {:.2%},  最高收益率 {:.2%} 最低收益率 {:.2%},  目前收益率 {:.2%}".format(max_retrace_date,max_retrace,max_return,min_return,value_array[-1])
		
		data_dict = glovar.query.history(security_list=self.betaStock, field_list=['close'], limit=len(date_list), unit='1D')
		beta = data_dict[self.betaStock][:,0]
		beta = beta/beta[0]-1
		alpha = value_array - beta
		security_name = self.betaStock

		save_name = glovar.context.run_params['start_date'][:10]+'---'+glovar.context.run_params['end_date'][:10]
		(
		    Line()
		    .add_xaxis(date_list)
		    .add_yaxis("策略", value_array.round(3).tolist())
		    .add_yaxis(security_name, beta.round(3).tolist())
		    .add_yaxis("Alpha", alpha.round(3).tolist(), is_symbol_show=False)	
		    .set_global_opts(title_opts=opts.TitleOpts(title='资金曲线',subtitle=subtitle),
		                     xaxis_opts=opts.AxisOpts(is_scale=True),
		                     yaxis_opts=opts.AxisOpts(is_scale=True,splitarea_opts=opts.SplitAreaOpts(is_show=True, areastyle_opts=opts.AreaStyleOpts(opacity=1))),
		                     datazoom_opts=[opts.DataZoomOpts()])    
		    .render(sys.path[0]+'/img/{}/{}/{}.html'.format(self.run_params['mode'], self.run_params['strategy'], save_name))
		)

	def plot_portfolio(self):
		# 持仓
		total_value = glovar.context.portfolio.total_value	
		if len(glovar.context.value_history)==0:			
			day_return = total_value/glovar.context.portfolio.starting_cash-1
		else:
			day_return = total_value/glovar.context.value_history[-1][-1]-1

		headers = ['股票代码','股票名称','持仓数量','开仓时间','收益率','资金占比']
		rows = []
		value_count = 0
		for security in glovar.context.portfolio.positions.index:
			position = glovar.context.portfolio.positions.loc[security,:]
			if security in self.stock_info.index.values:
				name = self.stock_info.loc[security, 'display_name']
			else:
				name = security[:-5]
			position_amount = position.amount
			position_value = position.value
			profit = (position.price-position.avg_cost)*position.amount*position.multiplier*position.side
			init_time = position.init_time.strftime('%Y-%m-%d %H:%M:%S')
			returns_rate = '{:.2%}'.format(profit/position_value)
			position_rate = '{:.2%}'.format(position_value/total_value)
			value_count += position_value			
			rows.append([security, name, position_amount, init_time, returns_rate, position_rate])
		total_position_rate = value_count/total_value
		title = '{}  当天收益率：{:.2%} 总资产：{:.0f} 持仓比率：{:.2%} 持股{}只'.format(glovar.context.current_dt.strftime('%Y-%m-%d %H:%M:%S'), 
			day_return, total_value, total_position_rate, len(glovar.context.portfolio.positions))
		(
		    Table()
		    .add(headers, rows)
		    .set_global_opts(title_opts=opts.ComponentTitleOpts(title=title))
		    .render(sys.path[0]+'/img/{}/{}/持仓.html'.format(self.run_params['mode'], self.run_params['strategy']))
		)



