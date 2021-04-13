from miki.data.dataSchedule import MikiData
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

	