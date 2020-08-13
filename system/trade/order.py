from system.trade import glovar
from system.trade.types import Order, OrderStatus
import pickle

# 发布订单到redis
def publish_order(order):
	if glovar.context.run_params['mode'] == 'sim_trade' and order.filled>0:
		_order = {'id':order.order_id, 
				  'security':order.security, 
				  'is_buy':order.is_buy,
				  'volume':order.filled,
				  'price':order.price,
				  'time':glovar.context.current_dt}		 
		glovar.redis_con.publish(glovar.context.run_params['strategy'], pickle.dumps(_order))

def order_amount(security, amount, side='long', stock_type='stocks'):
	if glovar.context.current_dt.strftime('%H:%M:%S') in ['11:30:00','15:00:00']:
		return 
		
	data = glovar.context.data_df[security]
	if data.paused:
		glovar.log.info('security {} paused, unable to trade'.format(security))
		return
	
	if amount > 0:
		is_buy = True
		action = 'open'
	elif amount < 0:
		is_buy = False
		action = 'close'
	else:
		return None
		
	order = Order()
	order.security = security
	order.amount = abs(amount)
	order.add_time = glovar.context.current_dt
	order.is_buy = is_buy
	order.order_status = OrderStatus.open
	order.filled = 0
	order.side = side
	order.action = action
	order.stock_type = stock_type

	order = glovar.context.match_order(order, stock_type)
	publish_order(order)
	return order





