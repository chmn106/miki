from miki.trade.types import OrderStatus, OrderCost, G, FixedSlippage, PriceRelatedSlippage
from miki.trade import glovar as g
import pandas as pd


class Context(object):
	def __init__(self, run_params):
		self.run_params = run_params
		self.current_dt = None
		self.close_series = None
		self.run_before_trading_start = False # context内部记录是否运行了盘前和盘后函数
		self.run_after_trading_end = False
		self.orderHistory = [] # 记录成交
		self.valueHistory = [] # 记录资金
		self._cashForPortfolio = 0 # 账户外可用资金
		self._portfolios = {run_params['types']: Portfolio(init_cash=run_params['starting_cash'], types=run_params['types'])}
		self._preDayValue = run_params['starting_cash']
		self.g = G()

	def set_portfolio(self, subportfolio):
		# 设置账户，调整账户之间的资金
		name = subportfolio.name
		if name in self._portfolios:
			change_cash = subportfolio.cash - self._portfolios[name].total_value
			if change_cash>0:
				change_cash = min(change_cash, self._cashForPortfolio)
			elif change_cash<0:
				change_cash = -min(-change_cash, self._portfolios[name].available_cash)
			if change_cash==0:
				return 0
			self._portfolios[name]._available_cash += change_cash
			self._cashForPortfolio -= change_cash
			g.log.info('portfolio {} change {} cash {:.2f} cashForPortfolio {:.2f}'.format(name, 'in' if change_cash>0 else 'out', abs(change_cash), self._cashForPortfolio))
		else:
			change_cash = min(self._cashForPortfolio, subportfolio.cash)
			self._portfolios[name] = Portfolio(init_cash=change_cash, types=subportfolio.types)
			self._portfolios[name].order_cost = subportfolio.order_cost
			self._portfolios[name].slippage = subportfolio.slippage
			self._cashForPortfolio -= change_cash
			g.log.info('add portfolio {} types {} init_cash {:.2f}'.format(name, subportfolio.types, change_cash))
		return change_cash

	def set_order_cost(self, cost, name):
		self._portfolios[name].order_cost = cost

	def set_lever(self, rate, name):
		self._portfolios[name].lever = rate

	def set_slippage(self, slip, name):
		self._portfolios[name].slippage = slip

	def match_order(self, order, name):
		# 订单撮合成交
		if order.is_buy:
			amount, profit = self._portfolios[name].open(order)
		else:
			amount, profit = self._portfolios[name].close(order)
		order.order_status = OrderStatus.held				
		order.filled = amount
		order.profit = profit
		self.orderHistory.append(order)
		return order

	@property
	def cashForPortfolio(self):
		# 账户外可用资金，用于在不同账户之间分配资金
		return self._cashForPortfolio
	
	@property
	def portfolioDict(self):
		return self._portfolios
	
	@property
	def portfolio(self):
		return list(self._portfolios.values())[0]

	@property
	def preDayValue(self):
		return self._preDayValue

	@property
	def total_value(self):
		value = 0
		for key in self._portfolios:
			value += self._portfolios[key].total_value
		return round(value, 2)

	@property
	def starting_cash(self):
		value = 0
		for key in self._portfolios:
			value += self._portfolios[key].starting_cash
		return round(value, 2)

	def before_trading_start(self):
		if not self.run_before_trading_start:
			self.run_before_trading_start = True
			self.run_after_trading_end = False
			for key in self._portfolios:
				self._portfolios[key].before_trading_start()
			g.log.debug('{} run context before_trading_start !'.format(self.current_dt))

	def onData(self, now_time):
		self.current_dt = now_time
		self.close_series = g.close_df.loc[now_time,:].dropna()
		factor_series = g.factor_df.loc[now_time.date(),:].dropna()
		for key in self._portfolios:
			self._portfolios[key].onData(self.close_series, factor_series, now_time)
		self.valueHistory.append([now_time, self.total_value, self.close_series[self.run_params['beta']]])

	def after_trading_end(self):
		if not self.run_after_trading_end:
			self.run_after_trading_end = True
			self.run_before_trading_start = False
			self._preDayValue = self.total_value
			g.log.debug('{} run context after_trading_end !'.format(self.current_dt))

class Portfolio(object):
	'''
	   1.采用dataframe作为持仓列表:avg_cost,amount,price,value,side,closeable_amount,today_amount,multiplier,
	   							  lever,init_time,recent_time,winPointRate
	   2.side 1多 -1空 multiplier 合约乘数 lever 杠杆倍率
	'''
	def __init__(self, init_cash, types):
		self._starting_cash = init_cash
		self._available_cash = init_cash
		self._preDayValue = init_cash
		self.positions = pd.DataFrame(columns=['avg_cost','amount','price','value','side','closeable_amount','multiplier','lever','init_time','winPointRate'])
		self.close_series = None
		self.factor_series = None
		self.now_time = None
		self.types = types
		self.run_count = False # 一天运行一次
		self.order_cost = OrderCost(open_tax=0, 
									close_tax=0.001, 
									open_commission=0.0003, 
								 	close_commission=0.0003, 
									close_today_commission=0, 
									min_commission=5)
		self.slippage = PriceRelatedSlippage(0.0025)

	def before_trading_start(self):
		self.positions['closeable_amount'] = self.positions['amount']
		self.positions['today_amount'] = 0
		self._preDayValue = self.total_value
		self.run_count = False

	def onData(self, close_series, factor_series, now_time):
		if self.types=='stocks' and self.factor_series is not None and not self.run_count:
			self.run_count = True
			# 分红、送股、股票更名
			increase_series = factor_series[self.positions.index]/self.factor_series[self.positions.index]-1
			if len(increase_series[increase_series>0]>0):
				amount = increase_series * self.positions['amount']
				increase_amount = amount // 100 * 100
				increase_cash = (amount - increase_amount) * self.positions['price']
				self.positions.loc[increase_amount.index, 'amount'] += increase_amount # dataframe相加需要index相同
				self._available_cash += increase_cash.sum()
				increase_amount = increase_amount[increase_amount!=0]
				increase_cash = increase_cash[increase_cash!=0]
				if len(increase_amount)>0:
					g.log.debug('{} increase_amount \n{}'.format(now_time, increase_amount))				
				if len(increase_cash)>0:
					g.log.debug('{} increase_cash \n{}'.format(now_time, increase_cash))
		self.factor_series = factor_series
		self.close_series = close_series
		self.now_time = now_time
		self.positions['price'].update(close_series)
		self.positions['value'] = self.positions['price']*self.positions['amount']*self.positions['multiplier']*self.positions['lever']
		self.positions['winPointRate'] = (self.positions['price']/self.positions['avg_cost']-1)*self.positions['side']
		# 期货保证金占用预警
		if self.types=='future' and self.margin_rate>0.95:
			raise Exception('your portfolio blow up !')

	def open(self, order):
		code,price,amount,side,multiplier,lever,commission,slipcost = order.security, order.price, order.amount, order.side, order.multiplier, order.lever, order.commission, order.slipcost
		order_money = amount*price*multiplier

		location = (self.positions.index==code)&(self.positions.side==side)
		status = location.sum()
		if status == 1:
			hold_amount,hold_value = self.positions.loc[location, ['amount','value']].iloc[-1]
			hold_amount,hold_value = hold_amount+amount, hold_value+order_money*lever
			avg_cost = hold_value / (hold_amount*multiplier*lever)
			self.positions.loc[location, ['amount','value','avg_cost','recent_time']] = hold_amount,hold_value,avg_cost,self.now_time
		elif status == 0:
			to_open = {'avg_cost':price,
					   'amount':amount, 
					   'price':price,
					   'value':order_money * lever,
					   'side':side, 
					   'closeable_amount':0 if self.types=='stocks' else amount, # A股T+1
					   'today_amount':amount,
					   'multiplier':multiplier,
					   'lever':lever,
					   'init_time':self.now_time,
					   'recent_time':self.now_time,
					   'winPointRate':0}
			to_open = pd.Series(to_open, name=code)
			self.positions = self.positions.append(to_open)
		else:
			raise Exception('{} duplicate index in positions'.format(code))
		self._available_cash -= (order_money * lever + slipcost + commission)
		side = 'long' if side==1 else 'short'
		g.log.debug('{} security {} open {} amount {} price {} commission {:.2f} slipcost {:.2f}'.format(self.now_time, code, side, amount, price, commission, slipcost))
		return amount, 0

	def close(self, order):
		code,price,amount,td_amount,side,multiplier,lever,commission,slipcost = order.security, order.price, order.amount, order.today_amount, order.side, order.multiplier, order.lever, order.commission, order.slipcost		
		order_money = amount * price * multiplier
		
		location = (self.positions.index==code)&(self.positions.side==side)	
		hold_amount,closeable_amount,today_amount,avg_cost = self.positions.loc[location,['amount','closeable_amount','today_amount','avg_cost']].iloc[-1]
		value = (hold_amount - amount) * price * multiplier * lever
		self.positions.loc[location,['amount','value','closeable_amount','today_amount']] = hold_amount-amount, value, closeable_amount-amount, today_amount-td_amount
		self.positions = self.positions[self.positions.amount>0]
		profit = (price - avg_cost) * amount * multiplier * side
		# 期货平仓计算盈亏
		if self.types=='future': self._available_cash += profit
		# 股票计算成交金额，期货计算保证金	
		self._available_cash += (order_money * lever - slipcost - commission)
		side = 'long' if side==1 else 'short'
		g.log.debug('{} security {} close {} amount {} price {} commission {:.2f} slipcost {:.2f} profit {:.2f}'.format(self.now_time, code, side, amount, price, commission, slipcost, profit))				
		return amount, profit
			
	@property
	def long_positions(self):
		return self.positions[self.positions.side==1]

	@property
	def short_positions(self):
		return self.positions[self.positions.side==-1]

	@property
	def starting_cash(self):
		return self._starting_cash

	@property
	def preDayValue(self):
		return self._preDayValue
		
	@property
	def available_cash(self):
		assert self._available_cash>=0, 'available_cash {}<0'.format(self._available_cash)
		if self.types=='future':
			# 期货浮动盈亏占用可用资金
			return round(self._available_cash+self.profit, 2)
		return round(self._available_cash, 2)

	@property
	def positions_value(self):
		# 持仓市值，期货为保证金
		return round(self.positions['value'].sum(), 2)

	@property
	def total_value(self):
		# 账户总资产
		total_value = self.positions_value + self.available_cash
		return round(total_value, 2)

	@property
	def profit(self):
		# 持仓盈利
		profit = (self.positions['price']-self.positions['avg_cost'])*self.positions['amount']*self.positions['multiplier']*self.positions['side']
		return round(profit.sum(), 2)

	@property
	def margin_rate(self):
		# 保证金占用比率
		return round(self.positions_value/self.total_value, 2)




















