from system.trade.strategyVar import *
import time

'''
	1.策略测试模块
	2.策略实现均在strategy目录下，主要有
	  initialize
	  before_trading_start 
	  handle_data
	  after_trading_end
	  after_backtest_end这些函数

'''

def after_trading_end(context):
	t2 = time.time()
	print(context.current_dt, t2-g.t1)
	g.t1 = t2

def before_trading_start(context):
	g.stock_list = query.get_all_securities(date=context.current_dt)

def initialize(context):
	context.set_order_cost(OrderCost(open_tax=0, 
									 close_tax=0.001, 
									 open_commission=0.0003, 
									 close_commission=0.0003, 
									 close_today_commission=0,
									 min_commission=5),
						   'stocks')
	context.set_slippage(0.002, 'stocks')
	g.t1 = time.time()
	g.last_day = None

def handle_data(context, data):
	now_time = context.current_dt
	today_minutes = (now_time.hour-9)*60+now_time.minute-30 if now_time.hour<=11 else (now_time.hour-13+2)*60+now_time.minute
	if today_minutes == 10:
		wait_to_buy = 50 - len(context.portfolio.positions)
		if wait_to_buy > 0:
			value = context.portfolio.available_cash / wait_to_buy
			stock_list = set(g.stock_list)&set(data.columns)
			for stock in stock_list:
				if stock not in context.portfolio.positions.index \
				and not data[stock].paused and data[stock].close<data[stock].high_limit:
					order_amount(stock, amount=100)
					wait_to_buy -= 1
				if wait_to_buy == 0:
					break
			g.last_day = context.current_dt
		log.info('{} positions {} hold_value is {:.2f} available_cash is {:.2f} total_value is {:.2f}'.format(context.current_dt, 
				len(context.portfolio.positions), context.portfolio.positions_value, context.portfolio.available_cash, context.portfolio.total_value))

	if today_minutes == 210:
		if (context.current_dt-g.last_day).days>=2:
			for stock in context.portfolio.positions.index:
				if not data[stock].paused and data[stock].close>data[stock].low_limit:
					amount = context.portfolio.positions.loc[stock,'amount']
					order_amount(stock, amount=-amount)
			log.info('{} positions {} hold_value is {:.2f} available_cash is {:.2f} total_value is {:.2f}'.format(context.current_dt, 
					len(context.portfolio.positions), context.portfolio.positions_value, context.portfolio.available_cash, context.portfolio.total_value))


