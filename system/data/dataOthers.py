import pandas as pd 
import numpy as np 
from datetime import datetime, timedelta 
import time
import bcolz
import os, sys
import multiprocessing
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import shutil
from query import Query
from dataBcolz import DataBcolz
from dataTables import *
from dataGlovar import DataPath
import warnings
warnings.filterwarnings("ignore")


class DataOthers(object):
	# 存储旧数据，生成dataUnit数据
	def __init__(self):
		self.query = Query()

	def init_connect(self):
		engine = create_engine("mysql+pymysql://root:123456@localhost:3306/finance?charset=utf8")
		self.engine = engine.connect()
		self.session = sessionmaker(bind=self.engine)()
		table_list = self.session.execute('show tables;').fetchall()
		self.table_list = [i[0] for i in table_list]

	def show_data(self, security, unit='1m'):
		# 检查Bcolz某只股票的数据
		path = DataBcolz.get_path(security, unit)
		ctable = bcolz.open(path, mode='r')	
		df = ctable.todataframe()
		df.index = [datetime.utcfromtimestamp(i) for i in df['date']]			
		del df['date']
		print(df)
		return df

	def show_data_sql(self, security, table=VALUATION):
		# 检查SQL某只股票的数据
		self.init_connect()
		if table in [VALUATION,INDICATOR]:
			q = self.session.query(table.code, table.date).filter(table.code==security)
			df = pd.read_sql(q.statement, q.session.bind)
		elif table == CAPITAL_CHANGE:
			q = self.session.query(table.change_date, table.share_total).filter(table.code==security)
			df = pd.read_sql(q.statement, q.session.bind).T					
		else:
			q = self.session.query(table).filter(table.code==security)
			df = pd.read_sql(q.statement, q.session.bind).T	
		print(df)

	def trim_data(self):
		# 删除，修正数据
		param_list = ['stock1m','stock1d','index1m','index1d']
		for param in param_list:
			security_list = []
			for root, dirs, files in os.walk(DataPath+'/{}'.format(param)):
				for name in dirs:
					if name.endswith('XSHG') or name.endswith('XSHE'):
						security_list.append(name)

			for security in security_list:
				data_path = DataBcolz.get_path(security, unit=param[-2:])
				path = data_path+'/date'
				if not os.path.exists(path):
					continue
				date = datetime.utcfromtimestamp(bcolz.open(path, mode='r')[-1]).date()
				if date == pd.to_datetime('2020-06-24').date():
					table = bcolz.open(data_path, mode='a')
					if param[-2:] == '1m':
						table.trim(240)
					else:
						table.trim(1)
					table.flush()
					print(security)

	@staticmethod
	def get_stocks(df_index, start, end, security):
		path1d = DataBcolz.get_path(security, unit='1d')
		path1m = DataBcolz.get_path(security, unit='1m')
		if not os.path.exists(path1d):
			return None, None
		ctable1 = bcolz.open(path1m, mode='r')	
		ctable2 = bcolz.open(path1d, mode='r')	
		df1 = ctable1.todataframe()[['date','open','high','low','close','volume','factor']]
		df2 = ctable2.todataframe()[['date','high_limit','low_limit','paused']]
		df1['date'] = df1['date'].apply(lambda x:datetime.utcfromtimestamp(x))
		df2['date'] = df2['date'].apply(lambda x:datetime.utcfromtimestamp(x))
		df1 = df1.set_index('date')
		df2 = df2.set_index('date')
		df_index = df_index[(df_index.index>=df1.index[0])&(df_index.index<=df1.index[-1])]
		df = pd.concat([df1, df2], axis=1)
		df = df.fillna(method='bfill')
		df = pd.concat([df_index, df], axis=1)
		df['paused'] = df['paused'].fillna(1)
		df['volume'] = df['volume'].fillna(0)
		df = df.fillna(method='ffill')
		df = df.fillna(0) # 由于有些股票一直停牌到2005，导致没有数据
		df = df[(df.index>=start)&(df.index<=end)]
		if len(df) == 0:
			return None, None
		return security, df

	def generate_dataUnit(self):
		# 生成dataUnit数据		
		date_array = np.array(bcolz.open(DataPath+'/index1m/01/000001.XSHG/date', mode='r'))
		date_array = np.array(list(map(lambda x:datetime.utcfromtimestamp(x), date_array)))		
		df_index = pd.DataFrame(index=date_array)
		security_list = self.query.get_all_securities()
		date_list = []
		for i in range(20):
			year = 2005 + i
			start = '{}-01-01 00:00:00'.format(year)				
			end = '{}-01-01 00:00:00'.format(year+1)
			if pd.to_datetime(start)>datetime.now():
				continue
			date_list.append([start,end])
		for [start, end] in date_list:
			print('{} {} start !'.format(start,end))
			result = list()
			pool = multiprocessing.Pool(8)
			for security in security_list:
				result.append(pool.apply_async(DataOthers.get_stocks, args=(df_index,start,end,security)))
			pool.close()
			pool.join()
			df_all = {}
			for res in result:
				security, df = res.get()
				if security is not None:
					df_all[security] = df
			df_all = pd.Panel(df_all)
			df_all = df_all.groupby(lambda x:x.strftime('%Y-%m'))
			for name, df in df_all:
				with open(DataPath+'/cache/query/{}.pkl'.format(name), 'wb') as f:
					pickle.dump(df, f)

	def save_old_data(self, data_df, param):
		# 从csv或其它数据来源导入数据，bcolz存储格式(field, value)，日级数据的时间格式是'xxxx-xx-xx 15:00:00'
		# param为'stock1m','stock1d','index1m','index1d'
		if param[-2:] == '1d':
			field_list = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']
		else:
			field_list = ['date','factor','open','high','low','close','volume']
		# dataframe格式分钟级、日级数据
		if len(data_df)>0:				
			data_df = data_df[field_list]
			array = data_df.values.transpose((1,0))
			path = DataPath+'/{}/{}/{}'.format(param, security[4:6], security)
			if not os.path.exists(path):
				os.makedirs(path, exist_ok=True)
				table = bcolz.ctable(rootdir=path,
									 columns=list(array),
									 names=field_list,
									 mode='w')
				table.flush()
			print('{} {} {}'.format(security, datetime.utcfromtimestamp(array[0,0]), datetime.utcfromtimestamp(array[0,-1])))

if __name__ == '__main__':
	d = DataOthers()




