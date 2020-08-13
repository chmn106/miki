import time
import os, sys
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pickle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import bcolz
from dataApi import DataApi
from dataTables import *
from query import Query
from dataGlovar import DataPath
import warnings
warnings.filterwarnings("ignore")


class DataSQL(object):
	# 财务数据模块
	'''MySQL安装
		1.管理员身份运行cmd，f:直接cd到f盘
		2.mysqld --initialize --console 这时会在控制台输出一个临时密码，把它存储起来
		3.mysqld install 安装mysql
		4.net start mysql 启动mysql服务
		5.mysql -u root -p 登陆
		6.alter user 'root'@'localhost' identified by '123456'; 改密码
	   Redis安装
	    1.redis-server.exe --service-install redis.windows.conf --loglevel verbose 默认服务安装
	    2.redis-server.exe --service-start
	      redis-server.exe --service-stop
	      redis-server.exe --service-uninstall
	'''
	def __init__(self, mode='sim_trade'):
		self.dataApi = DataApi()
		self.mode = mode

	def init_connect(self):
		engine = create_engine("mysql+pymysql://root:123456@localhost:3306/finance?charset=utf8")
		self.engine = engine.connect()
		self.session = sessionmaker(bind=self.engine)()
		table_list = self.session.execute('show tables;').fetchall()
		self.table_list = [i[0] for i in table_list]

	def update_finance_data(self):
		# 更新业绩预告、公司利润表、现金流量表、资产负债表、财务指标、市值数据等数据
		self.init_connect()
		table_dict = {'forcast':FORCAST,
					  'indicator':INDICATOR,
					  'valuation':VALUATION,
					  'capital_change':CAPITAL_CHANGE,
					  'income':INCOME,'cashflow':CASHFLOW,'balance':BALANCE,
					  'income_parent':INCOME_PARENT,'cashflow_parent':CASHFLOW_PARENT,'balance_parent':BALANCE_PARENT,
					  'income_finance':INCOME_FINANCE,'cashflow_finance':CASHFLOW_FINANCE,'balance_finance':BALANCE_FINANCE,
					  'income_finance_parent':INCOME_FINANCE_PARENT,'cashflow_finance_parent':CASHFLOW_FINANCE_PARENT,'balance_finance_parent':BALANCE_FINANCE_PARENT}

		df_info = self.dataApi.get_finance_data(types='stock_info', limit=None)
		df_info.to_sql('stock_info', self.engine, if_exists='replace', index=False)
		df_info = df_info.set_index('code')
		self.session.commit()

		df = self.dataApi.get_finance_data(types='company_info', limit=None)
		df.to_sql('company_info', self.engine, if_exists='replace', index=False)
		self.session.commit()

		for name in table_dict:
			security_list = self.dataApi.get_all_securities(types=['stock'])
			table = table_dict[name]
			if name in self.table_list:
				if name in ['indicator','valuation']:
					q = self.session.query(table.company_id, table.date)
					local_df = pd.read_sql(q.statement, q.session.bind)
					local_df = local_df.groupby('company_id').apply(lambda x:x.sort_values('date').iloc[-1,:])
					local_df = local_df.set_index('company_id')
				else:
					q = self.session.query(table.company_id, table.pub_date)
					local_df = pd.read_sql(q.statement, q.session.bind)					
					local_df = local_df.groupby('company_id').apply(lambda x:x.sort_values('pub_date').iloc[-1,:])
					local_df = local_df.set_index('company_id')
					df = self.dataApi.get_finance_data(end=datetime.now()-timedelta(days=10), types=name, get_recent_date=True)
					security_list = set(security_list)&set(df['code'].values)
			else:
				local_df = None

			for security in security_list:
				if security not in df_info.index:
					continue
				company_id = int(df_info.loc[security,'company_id'])
				if local_df is not None and company_id in local_df.index:
					if name in ['indicator','valuation']:
						df = self.dataApi.get_finance_data(company_id, code=security, end=datetime.now(), types=name, limit=10)
						df = df[df.date>local_df.loc[company_id,'date']]
					else:
						df = self.dataApi.get_finance_data(company_id, code=security, end=datetime.now(), types=name, limit=10)
						df = df[df.pub_date>local_df.loc[company_id,'pub_date']]
				else:
					df = self.dataApi.get_finance_data(company_id, code=security, end=datetime.now(), types=name, limit=10000)					
					if len(df)==0:
						print('{} not have {} data'.format(security, name))
						continue
				if len(df)>0:
					if 'company_id' not in df.columns:
						df['company_id'] = company_id						
					df.to_sql(name, self.engine, if_exists='append', index=False)
					self.session.commit()
					if name in ['indicator','valuation']: 
						print('{} {} {} {}'.format(name, security, df['date'].iloc[0], df['date'].iloc[-1]))
					else:
						print('{} {} {} {}'.format(name, security, df['pub_date'].iloc[0], df['pub_date'].iloc[-1]))

	def run_before_trading_start(self):
		self.update_finance_data()

	def run_every_minute(self):
		pass

	def run_after_trading_end(self):
		pass

if __name__ == '__main__':	
	d = DataSQL(mode='backtest')












