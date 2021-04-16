# miki  ![](https://img.shields.io/badge/license-MIT-blue) ![](https://img.shields.io/badge/miki-v2.0.1-blue)

miki是一个开源量化框架，它具有以下特点
+ 简单性： 代码采用python
+ 优雅性： 框架简单，具体API阅读源码
+ 拓展性： 支持股票、期货等

## 安装
```
pip install miki
```
## 数据存储
```js
from miki.data.dataSchedule import MikiData
from miki.data import dataGlovar
from jqdatasdk import *
import os, redis

if __name__ == '__main__':
	auth('账户','密码') # 需要在JoinQuant官网注册
	dataGlovar.DataPath = '数据存储地址'
	dataGlovar.redisCon = redis.StrictRedis(host='127.0.0.1')
	os.makedirs(dataGlovar.DataPath, exist_ok=True)
	m = MikiData()
	m.run()

```

## 策略运行
```js
import pandas as pd 
import numpy as np 
import pickle, redis
from miki.trade.technical import Technical
from miki.trade.function import TradeFunction
from miki.trade import glovar as g
from miki.trade.schedule import Schedule
from miki.trade.types import OrderCost, FixedSlippage, PriceRelatedSlippage, SubPortfolio
from miki.trade.order import order_amount, order_value, order_target_amount, order_target_rate, order_target_value
from miki.data.query import Query
from miki.data import dataGlovar
class Strategy(Schedule):
	def __init__(self, run_params):
		super(Strategy, self).__init__(run_params)
		dataGlovar.DataPath = '数据存储地址'
		dataGlovar.redisCon = redis.StrictRedis(host='127.0.0.1')
		self.run_params = run_params
		self.now_year = None
		self.query = Query()
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
		if self.run_params['mode']=='backtest' and not self.initOnce:
			def get_data(year, update):
				beta = self.query.get_stock('399300.XSHE', start='{}-01-01'.format(year), end='{}-01-01'.format(year+1), field_list=['close'], unit='1m')['close']
				g.close_df = self.query.get_dataframe(year, field='close', unit='1m', update=update)
				g.close_df['beta'] = beta
				g.factor_df = self.query.get_dataframe(year, field='factor', unit='1d', update=update)
				g.highLimit_df = self.query.get_dataframe(year, field='high_limit', unit='1d', update=update)
				g.lowLimit_df = self.query.get_dataframe(year, field='low_limit', unit='1d', update=update)
			if self.now_year is None or now_time.year!=self.now_year:
				g.all_trade_days = self.query.get_all_trade_days()
				g.display_name = self.query.get_security_info()['display_name']
				g.time_list = self.query.get_time_list(self.run_params['start'], self.run_params['end'], self.run_params['unit'])
				get_data(now_time.year, update=False)
				self.now_year = now_time.year
			if now_time>g.close_df.index[-1]:
				get_data(now_time.year, update=True)			
		elif self.run_params['mode']=='sim_trade':			
			key = now_time.strftime('%Y-%m-%d %H:%M:%S')
			if key in g.redisCon and key not in self.time_list:								
				self.time_list.append(key)	
				array, security_list, field_list = pickle.loads(g.redisCon.get(str(now_time)))
				df = pd.DataFrame(array, index=security_list, columns=field_list)
				g.close_df = df[['close']].T
				g.close_df.index = [now_time]
				g.close_df['beta'] = g.close_df['399632.XSHE']
				g.highLimit_df = df[['high_limit']].T
				g.highLimit_df.index = [now_time.date()]
				g.lowLimit_df = df[['low_limit']].T
				g.lowLimit_df.index = [now_time.date()]
				g.factor_df = df[['factor']].T
				g.factor_df.index = [now_time.date()]
				return True
	def before_trading_start(self):
		print(g.context.current_dt, 'run before_trading_start')
	def after_trading_end(self):
		print(g.context.current_dt, 'run after_trading_end')
	def after_backtest_end(self):
		print(g.context.current_dt, 'run after_backtest_end')
	def onMinute(self):
		print(g.context.current_dt)
if __name__ == '__main__':
	run_params = {'name':'testStrategy',
				  'mode':'backtest',
				  'start':'2020-01-01',
				  'end':'2021-01-01',
				  'unit':5,
				  'starting_cash':1000000, 
				  'types':'stocks',
				  'beta':'399300.XSHE',				  
				  'file_path':'.'}
	s = Strategy(run_params)
	s.run()

```
## 交流
QQ群：1042883511
## License
MIT License
