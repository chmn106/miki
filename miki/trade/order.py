from miki.trade.types import Order, OrderStatus
from miki.trade import glovar as g


def __order(security, side, portfolio_name, amount=None, value=None):
	# 检查下单
	now_time = g.context.current_dt
	portfolio = g.context.portfolioDict[portfolio_name]
	# 检查是否可成交
	tradeable = g.tradeable[security] if type(g.tradeable)==dict else g.tradeable
	if not tradeable:
		return
	# 检查涨跌停、停牌
	if security not in g.context.close_series:
		g.log.info('{} {} paused, unable to trade'.format(now_time, security))
		return
	else:
		close = g.context.close_series[security]
		high_limit = g.highLimit_df.loc[g.context.current_dt.date(), security]
		low_limit = g.lowLimit_df.loc[g.context.current_dt.date(), security]
	if side==1:
		if ((amount and amount>0) or (value and value>0)) and close>=high_limit:
			g.log.info('{} {} close {} >= high_limit {}, unable to open long'.format(now_time, security, close, high_limit))
			return
		elif ((amount and amount<0) or (value and value<0)) and close<=low_limit:
			g.log.info('{} {} close {} <= low_limit {}, unable to close long'.format(now_time, security, close, low_limit))
			return
	else:
		if ((amount and amount>0) or (value and value>0)) and close<=low_limit:
			g.log.info('{} {} close {} <= low_limit {}, unable to open short'.format(now_time, security, close, low_limit))
			return
		elif ((amount and amount<0) or (value and value<0)) and close>=high_limit:
			g.log.info('{} {} close {} >= high_limit {}, unable to close short'.format(now_time, security, close, high_limit))
			return
	location = (portfolio.positions.index==security)&(portfolio.positions.side==side)
	# 保证金和杠杆率
	if portfolio.types=='stocks':
		multiplier,lever = 1, 1
	else:
		multiplier,lever = g.multiplier[security], g.lever
	# 下单金额调整
	if value is not None:
		if value>0:
			value = min(portfolio.available_cash*0.99, value) # 需要计算手续费、滑点，扣除1%
		if portfolio.types=='stocks':
			amount = value / close // 100 * 100
		else:
			amount = int(value / (close * multiplier * lever))
		if amount<0:
			if location.sum() == 0:
				g.log.error('{} not in position'.format(security))
				return
			closeable_amount = portfolio.positions.loc[location, 'closeable_amount'].iloc[-1]
			amount = -min(-amount, closeable_amount)	
	# 下单数量调整
	elif amount is not None:
		if amount>0:
			# 买入
			if portfolio.types=='stocks':
				amount = (min(portfolio.available_cash*0.99, amount*close) / close) // 100 * 100
			else:
				cost = close * multiplier * lever # 一手的保证金
				amount = int(min(portfolio.available_cash*0.99, amount*cost) / cost)
		else:
			# 卖出
			if location.sum() == 0:
				g.log.error('{} not in position'.format(security))
				return
			closeable_amount = portfolio.positions.loc[location, 'closeable_amount'].iloc[-1]
			amount = -min(-amount, closeable_amount)
	if amount > 0:
		is_buy = True
		action = 'open'
	elif amount < 0:
		is_buy = False
		action = 'close'
	else:
		return		
	# 计算手续费、滑点
	order_money = amount * close * multiplier
	if action=='open':
		commission = order_money * (portfolio.order_cost.open_commission + portfolio.order_cost.open_tax)
		td_amount = amount
	else:
		# 优先平昨仓，再平今仓
		hold_amount,today_amount = portfolio.positions.loc[location, ['amount','today_amount']].iloc[-1]
		yd_amount = min(hold_amount-today_amount, -amount)	
		td_amount = -amount - yd_amount
		yd_commission = yd_amount*close*multiplier*(portfolio.order_cost.close_commission+portfolio.order_cost.close_tax)
		td_commission = td_amount*close*multiplier*(portfolio.order_cost.close_today_commission+portfolio.order_cost.close_tax)
		commission = yd_commission + td_commission
	commission = max(commission, portfolio.order_cost.min_commission)
	if portfolio.slippage.name=='PriceRelatedSlippage':
		slipcost = abs(order_money) * portfolio.slippage.slip
	else:
		slipcost = abs(amount) * portfolio.slippage.slip * multiplier
	# 开仓时可用资金不足扣除下单费用和手续费、滑点等
	if action=='open' and slipcost + commission + order_money * lever > portfolio.available_cash:
		return
	order = Order()
	order.security = security
	order.amount = abs(amount)
	order.price = close
	order.add_time = g.context.current_dt
	order.is_buy = is_buy
	order.order_status = OrderStatus.open
	order.filled = 0
	order.commission = commission
	order.slipcost = slipcost
	order.side = side
	order.action = action
	order.today_amount = td_amount
	order.multiplier = multiplier
	order.lever = lever
	order.portfolio_name = portfolio_name
	order = g.context.match_order(order, portfolio_name)
	return order

def order_amount(security, amount, side='long', portfolio_name='stocks'):
	side = 1 if side=='long' else -1
	if amount==0:
		return
	order = __order(security, side, portfolio_name, amount=amount)
	return order

def order_value(security, value, side='long', portfolio_name='stocks'):
	side = 1 if side=='long' else -1
	if value<0:
		portfolio = g.context.portfolioDict[portfolio_name]	
		location = (portfolio.positions.index==security)&(portfolio.positions.side==side)
		if location.sum() == 0:
			g.log.error('{} not in position'.format(security))
			return
		value = -min(-value, portfolio.positions.loc[location, 'value'].iloc[-1])
	if value==0:
		return
	order = __order(security, side, portfolio_name, value=value)
	return order

def order_target_rate(security, rate, side='long', portfolio_name='stocks'):
	# 调整到目标持仓比例
	assert rate>=0, 'rate should >= 0'
	side = 1 if side=='long' else -1
	portfolio = g.context.portfolioDict[portfolio_name]	
	location = (portfolio.positions.index==security)&(portfolio.positions.side==side)
	if location.sum()>0:
		hold_value = portfolio.positions.loc[location, 'value'].iloc[-1]
	else:
		hold_value = 0
	adjust_value = portfolio.total_value * rate - hold_value
	if adjust_value==0:
		return
	order = __order(security, side, portfolio_name, value=adjust_value)
	return order

def order_target_value(security, value, side='long', portfolio_name='stocks'):
	# 调整到目标持仓金额
	assert value>=0, 'value should >= 0'
	side = 1 if side=='long' else -1
	portfolio = g.context.portfolioDict[portfolio_name]
	location = (portfolio.positions.index==security)&(portfolio.positions.side==side)
	if location.sum()>0:
		hold_value = portfolio.positions.loc[location, 'value'].iloc[-1]
	else:
		hold_value = 0
	adjust_value = value - hold_value
	if adjust_value==0:
		return
	order = __order(security, side, portfolio_name, value=adjust_value)
	return order

def order_target_amount(security, amount, side='long', portfolio_name='stocks'):
	# 调整到目标持仓数量
	assert amount>=0, 'amount should >= 0'
	side = 1 if side=='long' else -1
	portfolio = g.context.portfolioDict[portfolio_name]	
	location = (portfolio.positions.index==security)&(portfolio.positions.side==side)
	if location.sum()>0:
		hold_amount = portfolio.positions.loc[location, 'amount'].iloc[-1]
	else:
		hold_amount = 0
	adjust_amount = amount - hold_amount
	if adjust_amount==0:
		return
	order = __order(security, side, portfolio_name, amount=adjust_amount)
	return order

