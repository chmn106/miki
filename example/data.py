from miki.data.dataSchedule import MikiData
from miki.data.dataBcolz import DataBcolz
from miki.data.dataApi import DataApi
from miki.data import dataGlovar
from jqdatasdk import *
import os, redis


if __name__ == '__main__':
	auth('账户','密码')
	dataGlovar.DataPath = '数据存储地址'
	dataGlovar.redisCon = redis.StrictRedis(host='127.0.0.1')
	os.makedirs(dataGlovar.DataPath, exist_ok=True)
	m = MikiData()
	m.run()
	# # 下载数据
	# start,end = '2021-01-01','2021-04-01'
	# api = DataApi()
	# security_list = api.get_all_securities() # 股票、指数数据
	# d = DataBcolz()
	# d.check_old_data(start, end, security_list)
