from enum import Enum
import uuid

'''
	1.数据类模块

'''
class G(object):
	pass
	
class Order(object):
	__slots__ = ('security','amount','is_buy','add_time','filled',
				 'order_id','order_status','price','action','side','commission','slipcost','profit','stock_type')
	def __init__(self):
		self.security = None
		self.amount = None
		self.is_buy = None
		self.add_time = None
		self.filled = None
		self.order_id = uuid.uuid4().hex
		self.order_status = None
		self.price = None,
		self.action = None,
		self.side = None,
		self.commission = None,
		self.slipcost = None,
		self.profit = None
		self.stock_type = None

class Trade(object):
	__slots__ = ('time','amount','price','trade_id','order_id','action')
	def __init__(self):
		self.time = None
		self.amount = None
		self.price = None
		self.action = None
		self.order_id = None
		self.trade_id = uuid.uuid4().hex

class OrderStatus(Enum):
	# 订单未完成, 无任何成交
	open = 0
	# 订单未完成, 部分成交
	filled = 1
	# 订单完成, 交易所已拒绝, 可能有成交, 需要看 Order.filled 字段
	rejected = 2   
	# 订单完成, 已撤销，可能有成交，需要看 Order.filled 字段
	canceled = 3
	# 订单完成, 全部成交
	held = 4
	# 订单取消中，只有实盘会出现，回测/模拟不会出现这个状态
	pending_cancel = 5 

class OrderCost(object):
	__slots__ = ('open_tax','close_tax','open_commission','close_commission','close_today_commission','min_commission')
	def __init__(self, open_tax, close_tax, open_commission, close_commission, close_today_commission, min_commission):
		self.open_tax = open_tax
		self.close_tax = close_tax
		self.open_commission = open_commission
		self.close_commission = close_commission
		self.close_today_commission = close_today_commission
		self.min_commission = min_commission



























