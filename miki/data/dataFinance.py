from datetime import datetime, timedelta
import pandas as pd
import os
from miki.data.dataApi import DataApi
from miki.data import dataGlovar
from miki.data.query import Query


class DataFinance(object):
	def __init__(self):
		self.dataApi = DataApi()
		self.query = Query()

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
		for name in ['valuation','indicator']:			
			base_path = dataGlovar.DataPath+'/finance/{}'.format(name)
			os.makedirs(base_path, exist_ok=True)
			all_trade_days = self.query.get_all_trade_days()
			for date in all_trade_days:
				path = base_path+'/{}.pkl'.format(date)
				if os.path.exists(path) or date>=datetime.now().date():
					continue
				df = self.dataApi.get_valuation_data(name, date)
				df.to_pickle(path)
			df = pd.read_pickle(base_path+'/2021-01-04.pkl')
			for column in df.columns.tolist():
				if column in ['id','code','day','pubDate','statDate']:
					continue
				path = base_path+'/{}.pkl'.format(column)
				if os.path.exists(path):
					old_df = pd.read_pickle(path)
					date_list = all_trade_days[all_trade_days>old_df.columns[-1]]
				else:
					old_df = pd.DataFrame()
					date_list = all_trade_days
				for date in date_list:
					path = base_path+'/{}.pkl'.format(date)
					if os.path.exists(path):
						df = pd.read_pickle(base_path+'/{}.pkl'.format(date))
						series = df[column]
						series.index = df['code'].values
						series.name = date
						old_df = pd.concat([old_df, series], axis=1)
				old_df.to_pickle(base_path+'/{}.pkl'.format(column))

	def before_trading_start(self):
		self.get_finance_data()

	def after_trading_end(self):
		pass










