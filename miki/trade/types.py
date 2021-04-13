from enum import Enum
import uuid


class G: pass
		
class Order(object):
	__slots__ = ('security','amount','is_buy','add_time','filled',
				 'order_id','order_status','price','action','side','multiplier','today_amount',
				 'lever','commission','slipcost','profit','portfolio_name')
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
		self.multiplier = None
		self.today_amount = None # 今仓数量
		self.lever = None
		self.commission = None,
		self.slipcost = None,
		self.profit = None
		self.portfolio_name = None

class TradeObject(object):
	__slots__ = ('time','amount','price','trade_id','order_id','action')
	def __init__(self):
		self.time = None
		self.amount = None
		self.price = None
		self.action = None
		self.order_id = None
		self.trade_id = uuid.uuid4().hex

class OrderStatus(Enum):
	open = 0 # 订单未完成，无任何成交
	filled = 1 # 订单未完成，部分成交
	canceled = 2 # 订单完成，已撤销，可能有成交
	held = 3 # 订单完成，全部成交

class OrderCost(object):
	# 手续费、佣金等
	__slots__ = ('open_tax','close_tax','open_commission','close_commission','close_today_commission','min_commission')
	def __init__(self, open_tax, close_tax, open_commission, close_commission, close_today_commission, min_commission):
		self.open_tax = open_tax
		self.close_tax = close_tax
		self.open_commission = open_commission
		self.close_commission = close_commission
		self.close_today_commission = close_today_commission
		self.min_commission = min_commission

class FixedSlippage(object):
	# 固定滑点
	__slots__ = ('slip','name')
	def __init__(self, slip):
		self.slip = slip
		self.name = 'FixedSlippage'

class PriceRelatedSlippage(object):
	# 百分比滑点
	__slots__ = ('slip','name')
	def __init__(self, slip):
		self.slip = slip
		self.name = 'PriceRelatedSlippage'
		
class SubPortfolio(object):
	__slots__ = ('name','cash','types','order_cost','slippage')
	def __init__(self, name, cash, types, order_cost, slippage):
		self.name = name
		self.cash = cash
		self.types = types
		self.order_cost = order_cost
		self.slippage = slippage
		

























