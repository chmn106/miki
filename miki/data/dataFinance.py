from datetime import datetime, timedelta
import pandas as pd
import os
from miki.data.dataApi import DataApi
from miki.data import dataGlovar
from jqdatasdk import *


class DataFinance(object):
	def __init__(self):
		self.dataApi = DataApi()

	def get_finance_data(self):
		# 更新业绩预告、利润表、现金流量表、资产负债表、十大流通股东等数据
		name_list = [
		'forcast','capital_change',
		'shareholder_top10','shareholder_floating_top10',
		'income','cashflow','balance',
		'income_parent','cashflow_parent','balance_parent',
		'income_finance','cashflow_finance','balance_finance',
		'income_finance_parent','cashflow_finance_parent','balance_finance_parent']
		base_path = dataGlovar.DataPath+'/finance'
		os.makedirs(base_path, exist_ok=True)
		for name in name_list:
			path = base_path+'/{}.pkl'.format(name)
			if os.path.exists(path):
				start = pd.read_pickle(path)['pub_date'].sort_values(ascending=True).iloc[-1]
				start = pd.to_datetime(start)
			else:
				start = pd.to_datetime('2005-01-01')
			time_list = []
			while start<datetime.now():
				time_list.append([start, start+timedelta(days=10)])
				start += timedelta(days=10)
			for start, end in time_list:
				df = self.dataApi.get_finance_data(name, start=start, end=end)
				if os.path.exists(path):
					old_df = pd.read_pickle(path)
					df = pd.concat([old_df, df], axis=0).drop_duplicates()
				df.to_pickle(path)

	def before_trading_start(self):
		self.get_finance_data()

	def after_trading_end(self):
		pass











