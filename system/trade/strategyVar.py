from system.trade.types import G, OrderCost
from system.trade.order import order_amount
from system.trade import glovar

query = glovar.query
g = G()
log = glovar.log


# 预加载变量
__all__ = ['OrderCost','order_amount','log','g','query']







