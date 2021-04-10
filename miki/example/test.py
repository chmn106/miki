import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta
import time, pickle, os, sys, redis, random
from miki.trade.technical import Technical
from miki.trade.function import MikiFunction
from miki.trade import glovar as g
from miki.trade.schedule import Schedule
from miki.trade.types import OrderCost, FixedSlippage, PriceRelatedSlippage, SubPortfolio
from miki.trade.order import order_amount, order_value, order_target_amount, order_target_rate, order_target_value
from jqdatasdk import *


class Strategy(Schedule):
	def __init__(self, run_params):
		super(Strategy, self).__init__(run_params)
		g.display_name = None
		g.all_trade_days = None

	def init_context(self):
		g.context.set_order_cost(OrderCost(open_tax=0,
										   close_tax=0.001,
										   open_commission=0.0003,
										   close_commission=0.0003,
										   close_today_commission=0,
										   min_commission=5), 'stocks')
		g.context.set_slippage(PriceRelatedSlippage(0.002), 'stocks')
		
	def update_data(self, now_time):
		# 挂载回测数据
		pass

	def before_trading_start(self):
		print(g.context.current_dt, 'run before_trading_start')

	def onMinute(self):
		print(g.context.current_dt)

if __name__ == '__main__':
	run_params = {'name':'testStrategy',
				  'mode':'backtest',
				  'start':'2018-01-01',
				  'end':'2019-01-01',
				  'unit':5,
				  'starting_cash':1000000, 
				  'types':'stocks',
				  'beta':'beta',				  
				  'pre_path':'',
				  'data_path':''}
	s = Strategy(run_params)
	s.run()






