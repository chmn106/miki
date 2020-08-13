import time
from datetime import datetime
import redis
import pickle


'''
	1.api等模块，欢迎大家贡献期货等其它api模块
	
'''
class OrderPool(object):
	# 下单模块
	def __init__(self):
		redis_con = redis.StrictRedis(host='127.0.0.1')
		self.redisListener = redis_con.pubsub()
		self.redisListener.subscribe(['strategy'])

	def do_order(self, security, volume, is_buy):
		print('{} {} is_buy:{}'.format(security, volume, is_buy))

	def run(self):
		while True:
			for item in self.redisListener.listen():
				if item['type'] == 'message':
					strategyID = item['channel']
					order = pickle.loads(item['data'])
					self.do_order(security=order['security'], volume=order['volume'], is_buy=order['is_buy'])
	
if __name__ == '__main__':
	orderPool = OrderPool()
	orderPool.run()




