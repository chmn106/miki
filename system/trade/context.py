import pandas as pd
import numpy as np
import time
from datetime import datetime 
import yaml
import sys
from system.trade.types import *
from system.trade import glovar


class Context(object):
	'''
		1.不支持限价单交易
		2.股票、期货、期权分为独立部分

	'''
	__slots__ = ('run_params','current_dt','data_df','_portfolios','filled_orders', \
				 'value_history','run_count','multiplier','change_dict')
	def __init__(self, run_params):
		self.run_params = run_params
		self._portfolios = {'stocks':PortfolioStocks(init_cash=self.run_params['starting_cash'])}
		self.current_dt = datetime.now() if self.run_params['mode']=='sim_trade' else datetime.strptime(self.run_params['start_date'], '%Y-%m-%d %H:%M:%S')
		self.data_df = None
		self.filled_orders = []
		self.value_history = []
		self.run_count = False

		with open(sys.path[0]+'/system/trade/others.yaml', 'r') as f:
			data = yaml.load(f.read())
		self.multiplier = data['Multiplier']
		self.change_dict = data['ChangeDict']

	@property
	def portfolio(self):
		if 'future' in self._portfolios and 'stocks' in self._portfolios:
			return self._portfolios
		elif 'future' in self._portfolios:
			return self._portfolios['future']
		else:
			return self._portfolios['stocks']

	def set_portfolios(self, cashDict):
		self._portfolios = {}	
		if 'stocks' in cashDict:
			self._portfolios['stocks'] = PortfolioStocks(init_cash=cashDict['stocks'])
		if 'future' in cashDict:
			self._portfolios['future'] = PortfolioFuture(init_cash=cashDict['future'], multiplier=self.multiplier)		
		
	def set_order_cost(self, cost, types):
		self._portfolios[types].ordercost = cost

	def set_lever(self, rate, types):
		self._portfolios[types].lever = rate

	def set_slippage(self, slip, types):
		self._portfolios[types].slip = slip

	def match_order(self, order, types):
		data = self.data_df[order.security]
		if order.is_buy:
			if data.close == data.high_limit:
				return order
			amount, profit, commission, slipcost = self._portfolios[types].open(code=order.security, amount=order.amount, side=order.side)
		else:
			if data.close == data.low_limit:
				return order
			amount, profit, commission, slipcost = self._portfolios[types].close(code=order.security, amount=order.amount, side=order.side)
		order.order_status = OrderStatus.held				
		order.filled = amount
		order.price = data.close
		order.commission = commission
		order.slipcost = slipcost
		order.profit = profit
		if amount > 0:
			self.filled_orders.append(order)
		return order

	def run_before_trading_start(self):
		pass

	def run_every_minute(self, data_df, now_time, stock_info):
		# 受网络通信、数据API等限制，股票、期货、期权分别进行推送
		self.current_dt = now_time
		self.data_df = data_df
		for types in self._portfolios:
			if not self.run_count:
				self._portfolios[types].run_at_trading_start(data_df, now_time, stock_info, self.change_dict)
				self.run_count = True
			self._portfolios[types].run_every_minute(data_df, now_time)

	def run_after_trading_end(self):
		self.filled_orders = []
		self.run_count = False
		total_value = 0
		for types in self._portfolios:
			total_value += self._portfolios[types].total_value
		self.value_history.append([self.current_dt, total_value])

class BasicPortfolio(object):
	'''
	   1.采用dataframe作为持仓列表
	   2.'factor','avg_cost','amount','price','value','side','closeable_amount','multiplier','lever','init_time'
	   3.side 1多 -1空 action 0买入 1卖出 multiplier 合约乘数 lever 杠杆倍率
	   4.支持股票、期货等交易 types: 0股票 1期货 2期权

	'''
	__slots__ = ('_positions', '_starting_cash', '_available_cash', 'ordercost', 'slip', 'lever','multiplier')
	def __init__(self, init_cash, multiplier=None):
		self._starting_cash = init_cash
		self._available_cash = init_cash
		self._positions = pd.DataFrame(columns=['factor','avg_cost','amount','price','value','side','closeable_amount','multiplier','lever','init_time'])
		self.data_df = None
		self.now_time = None
		self.types = None
		self.lever = None
		self.multiplier = multiplier

		self.ordercost = OrderCost(open_tax=0, 
								   close_tax=0.001, 
								   open_commission=0.0003, 
								   close_commission=0.0003, 
								   close_today_commission=0, 
								   min_commission=5)
		self.slip = 0.0025

	def run_at_trading_start(self, data_df, now_time, stock_info, change_dict):
		if self.types == 0:
			# 代码变更 优先
			change_dict = change_dict.get(now_time.strftime('%Y-%m-%d'), {})
			if len(change_dict)>0:
				self._positions = self._positions.rename(index=change_dict)
				glovar.log.warn('change dict {}'.format(change_dict))

			# 退市
			delisted_stocks = set(stock_info[stock_info.end_date==str(now_time.date())].index)&set(self._positions.index)
			for stock in delisted_stocks:
				increase_cash = self._positions.loc[stock,'value']
				self._available_cash += increase_cash
				self._positions = self._positions.drop(labels=stock)
				glovar.log.warn('security {} is delisted, value turn to cash {:.2f}'.format(stock, increase_cash))

			# 分红、送股
			loc_index = set(data_df.columns)&set(self._positions.index)
			new_factor = data_df.loc['factor', loc_index]
			old_factor = self._positions.loc[loc_index, 'factor']
			amount = (new_factor/old_factor - 1) * self._positions.loc[loc_index,'amount']
			increase_amount = amount // 100 * 100
			increase_cash = (amount - increase_amount) * data_df.loc['close', loc_index]
			self._positions.loc[loc_index, 'amount'] += increase_amount
			self._positions['factor'].update(new_factor)
			self._available_cash += increase_cash.sum()
			increase_amount = increase_amount[increase_amount>0]
			increase_cash = increase_cash[increase_cash>0].round(2)
			if len(increase_amount)>0 or len(increase_cash)>0:
				glovar.log.warn('increase_cash {} increase_amount {}'.format(increase_cash.to_dict(), increase_amount.to_dict()))
				
			# T+1
			self._positions['closeable_amount'] = self._positions['amount']

	def run_every_minute(self, data_df, now_time):
		self.data_df = data_df
		self.now_time = now_time
		self._positions['price'].update(data_df.loc['close',:])
		self._positions['value'] = self._positions['price']*self._positions['amount']*self._positions['multiplier']*self._positions['lever']

	def open(self, code, amount, side):
		price,factor = self.data_df.loc[['close','factor'],code]
		if self.types == 0:
			multiplier = 1
			cost = price * multiplier * self.lever
			amount = (min(self._available_cash*0.99, amount*cost) / cost + 0.001) // 100 * 100
			closeable_amount = 0
			side = 1
		else:
			multiplier = self.multiplier[code[:-9]]
			cost = price * multiplier * self.lever
			amount = int(min(self._available_cash*0.99, amount*cost) / cost + 0.001)
			closeable_amount = amount
			side = 1 if side=='long' else -1
		if amount == 0:
			glovar.log.info('security {} can open amount is 0 !'.format(code))			
			return 0, 0, 0, 0
		filled_money = amount * price * multiplier
		commission = filled_money * (self.ordercost.open_commission + self.ordercost.open_tax)
		commission = max(commission, self.ordercost.min_commission)
		slipcost = filled_money * self.slip

		loc_index = (self._positions.index==code)&(self._positions.side==side)
		status = loc_index.sum()
		if status == 1:
			old_amount,old_value = self._positions.loc[loc_index, ['amount','value']].iloc[-1]
			new_amount,new_value = old_amount+amount, old_value+filled_money*self.lever
			avg_cost = new_value/new_amount
			self._positions.loc[loc_index, ['amount','value','avg_cost']] = new_amount,new_value,avg_cost
		elif status == 0:
			to_open = {'factor':factor,
					   'avg_cost':price,
					   'amount':amount, 
					   'price':price,
					   'value':filled_money * self.lever,
					   'side':side, 
					   'closeable_amount':closeable_amount,
					   'multiplier':multiplier,
					   'lever':self.lever,
					   'init_time':self.now_time}
			to_open = pd.Series(to_open, name=code)
			self._positions = self._positions.append(to_open)
		else:
			raise Exception('{} duplicate index in positions'.format(code))
		self._available_cash -= (filled_money * self.lever + slipcost + commission)
		glovar.log.debug('security {} open amount {} price {} commission {:.2f} slipcost {:.2f}'.format(code, amount, price, commission, slipcost))
		return amount, 0, commission, slipcost

	def close(self, code, amount, side):
		if self.types == 0:
			side = 1
		else:
			side = 1 if side=='long' else -1
		loc_index = (self._positions.index==code)&(self._positions.side==side)
		status = loc_index.sum()
		if status == 0:
			raise Exception('{} not in position'.format(code))
		old_amount,closeable_amount,price,multiplier,lever,avg_cost,side = self._positions.loc[loc_index,['amount','closeable_amount','price','multiplier','lever','avg_cost','side']].iloc[-1]
		close_amount = min(amount, closeable_amount)
		if close_amount == 0:
			return 0, 0, 0, 0		
		if old_amount == close_amount:
			self._positions = self._positions.loc[~loc_index]
		else:
			self._positions.loc[loc_index,['amount','closeable_amount']] = old_amount-close_amount, closeable_amount-close_amount
		profit = (price - avg_cost) * close_amount * multiplier * side
		filled_money = amount * price * multiplier
		commission = filled_money * (self.ordercost.close_commission + self.ordercost.close_tax)
		commission = max(commission, self.ordercost.min_commission)
		slipcost = filled_money * self.slip		
		self._available_cash += (filled_money * lever - slipcost - commission)
		glovar.log.debug('security {} close amount {} price {} commission {:.2f} slipcost {:.2f} profit {:.2f}'.format(code, amount, price, commission, slipcost, profit))		
		return amount, profit, commission, slipcost
		
	@property
	def positions(self):
		return self._positions
	
	@property
	def long_positions(self):
		return self._positions[self._positions.side==1]

	@property
	def short_positions(self):
		return self._positions[self._positions.side==-1]

	@property
	def starting_cash(self):
		return self._starting_cash

	@property
	def available_cash(self):
		return self._available_cash

	@property
	def closeable_value(self):
		value = self._positions['price']*self._positions['closeable_amount']*self._positions['multiplier']*self._positions['lever']
		return value
		
	@property
	def positions_value(self):
		value = self._positions['value'].sum()
		return value

	@property
	def total_value(self):
		if self.types == 1:
			total_value = self.positions_value + self._available_cash + self.profit
		else:
			total_value = self.positions_value + self._available_cash
		return total_value

	@property
	def profit(self):
		profit = (self._positions['price']-self._positions['avg_cost'])*self._positions['multiplier']*self._positions['side']
		return profit.sum()

class PortfolioStocks(BasicPortfolio):
	def __init__(self, init_cash):
		super(PortfolioStocks, self).__init__(init_cash)
		self.types = 0
		self.lever = 1.0

class PortfolioFuture(BasicPortfolio):
	def __init__(self, init_cash):
		super(PortfolioFuture, self).__init__(init_cash)
		self.types = 1
		self.lever = 0.15

	def is_dangerous(self, rate):
		# 保证金占用比率预警
		return self.total_value<=0 or self.positions_value/self.total_value>rate

	@property
	def margin_rate(self):
		# 保证金占用比率
		return round(self.positions_value / self.total_value, 2)
















