from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
import numpy as np
import pickle
import time
import os, sys
sys.path.append(os.getcwd()[:2]+'/Miki/system/data')
from dataTables import VALUATION, INDICATOR, STOCKINFO, CAPITAL_CHANGE, FORCAST, \
					   INCOME, INCOME_PARENT, INCOME_FINANCE, INCOME_FINANCE_PARENT, \
					   CASHFLOW, CASHFLOW_PARENT, CASHFLOW_FINANCE, CASHFLOW_FINANCE_PARENT, \
					   BALANCE, BALANCE_PARENT, BALANCE_FINANCE, BALANCE_FINANCE_PARENT
from dataMap import income_map_dict, cashflow_map_dict, balance_map_dict, valuation_map_dict        
from query import Query
from dataGlovar import DataPath
from baseFactor import BaseFactor


class FactorFinance(BaseFactor):
	# 财务因子
	def __init__(self, mode='sim_trade'):
		super(FactorFinance, self).__init__()
		self.mode = mode
		self.query = Query(mode=mode)
		self.init()

	def init(self, update=False):
		start_date = pd.to_datetime('2005-01-01').date()

		income_df = self.query.get_fundamentals(self.query.query(INCOME))
		income_df = income_df[(income_df.report_type==0)&(income_df.start_date>=start_date)]
		income_df = income_df.rename(columns=income_map_dict)
		income_df.index = income_df.loc[:,'报告期'].values
		group_df = income_df.groupby(by='股票代码')
		self.income_df = {}
		for code,df in group_df:
			df = df[~df.index.duplicated(keep='last')]
			self.income_df[code] = df
		self.income_df = pd.Panel(self.income_df)

		cashflow_df = self.query.get_fundamentals(self.query.query(CASHFLOW))
		cashflow_df = cashflow_df[(cashflow_df.report_type==0)&(cashflow_df.start_date>=start_date)]
		cashflow_df = cashflow_df.rename(columns=cashflow_map_dict)
		cashflow_df.index = cashflow_df.loc[:,'报告期'].values
		group_df = cashflow_df.groupby(by='股票代码')
		self.cashflow_df = {}
		for code,df in group_df:
			df = df[~df.index.duplicated(keep='last')]
			self.cashflow_df[code] = df
		self.cashflow_df = pd.Panel(self.cashflow_df)

		balance_df = self.query.get_fundamentals(self.query.query(BALANCE))
		balance_df = balance_df[(balance_df.report_type==0)&(balance_df.start_date>=start_date)]
		balance_df = balance_df.rename(columns=balance_map_dict)
		balance_df.index = balance_df.loc[:,'报告期'].values
		group_df = balance_df.groupby(by='股票代码')
		self.balance_df = {}
		for code,df in group_df:
			df = df[~df.index.duplicated(keep='last')]
			self.balance_df[code] = df
		self.balance_df = pd.Panel(self.balance_df)
		
	def cal_factor(self):
		data_dict = defaultdict(list)
		security_list = list(set(self.income_df.items)&set(self.cashflow_df.items)&set(self.balance_df.items))		
		for security in security_list:
			income_df, cashflow_df, balance_df = self.income_df[security], self.cashflow_df[security], self.balance_df[security]
			income_df.index = income_df['公告日期'].values
			cashflow_df.index = cashflow_df['公告日期'].values
			balance_df.index = balance_df['公告日期'].values
			# 可能多个财报数据同时发布
			income_df = income_df[~income_df.index.duplicated(keep='last')]
			cashflow_df = cashflow_df[~cashflow_df.index.duplicated(keep='last')]
			balance_df = balance_df[~balance_df.index.duplicated(keep='last')]

			# 债务总资产比  负债合计/总资产
			series = balance_df['负债合计']/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor1'].append(series)	
			# 股东权益与固定资产比率  股东权益/(固定资产+工程物资+在建工程)
			series = balance_df['所有者权益（或股东权益）合计']/(balance_df['固定资产']+balance_df['工程物资']+balance_df['在建工程']).astype('float')
			data_dict['factor2'].append(series)		
			# 非流动资产比率  非流动资产合计/总资产
			series = balance_df['非流动资产合计']/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor3'].append(series)			
			# 非流动负债与总资产之比  非流动负债合计/总资产
			series = balance_df['非流动负债合计']/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor4'].append(series)
			# 有形净值债务率  负债合计/(股东权益-商誉-无形资产)
			series = balance_df['负债合计']/(balance_df['所有者权益（或股东权益）合计']-balance_df['商誉']-balance_df['无形资产']).astype('float')
			data_dict['factor5'].append(series)
			# 无形资产比率 (无形资产+研发支出+商誉)/总资产
			series = (balance_df['无形资产']+balance_df['研发支出']+balance_df['商誉'])/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor6'].append(series)
			# 长期借款与资产总计之比 长期借款/总资产
			series = balance_df['长期借款']/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor7'].append(series)
			# 股东权益比率  股东权益/总资产
			series = balance_df['所有者权益（或股东权益）合计']/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor8'].append(series)
			# 固定资产比率 (固定资产+工程物资+在建工程)/总资产
			series = (balance_df['固定资产']+balance_df['工程物资']+balance_df['在建工程'])/balance_df['负债和所有者权益（或股东权益）合计'].astype('float')
			data_dict['factor9'].append(series)

		day_list = self.query.get_time_list(start='2005-01-01', end=datetime.now(), unit=240)
		day_df = pd.DataFrame(index=[i.date() for i in day_list])

		for name in data_dict:
			df = pd.concat(data_dict[name], axis=1)
			df.columns = security_list
			df = df.replace([np.inf,-np.inf], np.nan)
			df = pd.concat([day_df, df], axis=1).fillna(method='ffill')
			self.to_pickle(df, name)

	def run_before_trading_start(self):
		pass

	def run_after_trading_end(self):
		self.init(update=True)
		self.cal_factor()

if __name__ == '__main__':
	f = FactorFinance()
	f.cal_factor()














