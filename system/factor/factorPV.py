from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pickle
import os, sys
sys.path.append(os.getcwd()[:2]+'/Miki/system/data')
from dataTables import VALUATION
from query import Query
from dataGlovar import DataPath
from baseFactor import BaseFactor


class FactorPV(BaseFactor):
	# 量价因子
	def __init__(self, mode='sim_trade'):
		super(FactorPV, self).__init__()
		self.mode = mode
		self.query = Query(mode=mode)
		self.init()

	def init(self, update=False):
		security_list = self.query.get_all_securities()
		field_list = ['open','high','low','close','volume','money']
		self.pv_data = self.query.get_price(security_list, field_list=field_list, start='2007-01-01', \
											 end=datetime.now(), unit='1D', fq='post', return_df=True)
		self.pv_data = pd.Panel(self.pv_data)
		df = self.query.get_fundamentals(self.query.query(VALUATION))
		group_df = df.groupby(by='code')
		data_df = {}
		for code,df in group_df:
			df = df.drop_duplicates().set_index('date')
			data_df[code] = df
		self.valuation_df = pd.Panel(data_df)

	def cal_factor(self):		
		# MC 总市值
		df = self.valuation_df.loc[:,:,'market_cap']
		self.to_pickle(df, 'MC')		
		# CMC 流通市值
		df = self.valuation_df.loc[:,:,'circulating_market_cap']
		self.to_pickle(df, 'CMC')
		# CAP 总股本
		df = self.valuation_df.loc[:,:,'capitalization']
		self.to_pickle(df, 'CAP')			
		# CCAP 流通股本
		df = self.valuation_df.loc[:,:,'circulating_cap']
		self.to_pickle(df, 'CCAP')
		# PE 市盈率
		df = self.valuation_df.loc[:,:,'pe_ratio']
		self.to_pickle(df, 'PE')
		# PB 市净率
		df = self.valuation_df.loc[:,:,'pb_ratio']
		self.to_pickle(df, 'PB')
		# PS 市销率
		df = self.valuation_df.loc[:,:,'ps_ratio']
		self.to_pickle(df, 'PS')
		# PCF 市现率
		df = self.valuation_df.loc[:,:,'pcf_ratio']
		self.to_pickle(df, 'PCF')

	def run_before_trading_start(self):
		pass

	def run_after_trading_end(self):
		self.init(update=True)
		self.cal_factor()

if __name__ == '__main__':
	d = FactorPV()
	d.cal_factor()














