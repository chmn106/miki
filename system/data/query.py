import pandas as pd 
import numpy as np
import time
from datetime import datetime, timedelta
import os, sys
import redis
import pickle
import bcolz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dataBcolz import DataBcolz
from dataGlovar import DataPath
from dataTables import *
import warnings
warnings.filterwarnings("ignore")

'''
	1.数据调用模块，后期会采用c++替代某些功能，也欢迎大家贡献代码
	
'''

class BasicContext(object):
	def __init__(self):
		self.run_params = {'mode':'backtest'}
		self.current_dt = pd.to_datetime(datetime.now())
		
class QuerySQL(object):
	def __init__(self):
		self.session = None
		
	def init_connect(self):
		engine = create_engine("mysql+pymysql://root:123456@localhost:3306/finance?charset=utf8")
		engine.connect()
		self.session = sessionmaker(bind=engine)()

	@property	
	def query(self):
		if self.session is None or not self.session.is_active:
			self.init_connect()
		return self.session.query

	def get_fundamentals(self, query):
	    return pd.read_sql(query.statement, query.session.bind)

class Query(QuerySQL):
	# 数据查询接口 
	def __init__(self, mode='backtest', context=BasicContext()):
		QuerySQL.__init__(self)
		self.redis_conn = redis.StrictRedis(host='127.0.0.1')
		self.mode = mode
		self.context = context
		self.todayData_field = ['date','factor','open','high','low','close','volume','high_limit','low_limit','paused']
		self.init_cache()

	def init_cache(self):
		self.stock_info = None
		self.all_trade_days = None
		self.factor_data = {}

	def get_pre_trade_day(self, date):
		if self.all_trade_days is None:
			self.all_trade_days = self.get_all_trade_days()
		pre_day = self.all_trade_days[self.all_trade_days<date][-1]
		return pre_day

	def get_factor_data(self, name, end, ascending, start=None, limit=None):
		# 获取因子值 --- ascending False 最小值得分最高
		if name in self.factor_data:
			df = self.factor_data[name]
		else:
			path = DataPath+'/cache/factor/{}.pkl'.format(name)
			if not os.path.exists(path):
				return None
			df = pd.read_pickle(path)
			df.index = pd.to_datetime(df.index)
			self.factor_data[name] = df
		if start is not None:
			start,end = pd.to_datetime(start),pd.to_datetime(end)
			df = df[(df.index>=start)&(df.index<=end)]
		else:
			end = pd.to_datetime(end)
			df = df[df.index<=end][-limit:]
		df = df.apply(lambda x:x.rank(ascending=ascending, method='dense'), axis=1)
		return df

	def get_all_securities(self, types=['stock'], date=None, include_delist=False):
		# 获取代码列表
		if self.stock_info is None:
			self.stock_info = pd.read_pickle(DataPath+'/pickle/stock_info.pkl')
		df = self.stock_info[self.stock_info.type.isin(types)]
		if date:
			if include_delist:
				df = df[df.start_date<=date]
			else:
				df = df[(df.start_date<=date)&(df.end_date>=date)]
		security_list = df.index.values.tolist()
		return security_list
	
	def get_security_info(self):
		# 获取所有股票信息
		if self.stock_info is None:
			self.stock_info = pd.read_pickle(DataPath+'/pickle/stock_info.pkl')
		return self.stock_info

	def get_all_trade_days(self, until_now=False):
		# 获取所有交易日
		with open(DataPath+'/pickle/all_trade_days.pkl', 'rb') as f:
			all_trade_days = pickle.load(f)
		if until_now:
			all_trade_days = [i for i in all_trade_days if i<=datetime.now().date()]
		return np.array(all_trade_days)

	def get_time_list(self, start, end, unit):
		# 获取交易时间列表
		def func(x):
			if x.hour<=11:
				today_counts = (x.hour-9)*60+x.minute-30
			else:
				today_counts = (x.hour-13+2)*60+x.minute
			return today_counts
		start,end = pd.to_datetime(start),pd.to_datetime(end)
		date = self.get_bcolz('000001.XSHG', outcols='date', start=start, end=end)
		date = [datetime.utcfromtimestamp(i) for i in date]
		if end.date() >= datetime.now().date():
			date += DataBcolz.get_today_date_list()
		series = pd.Series(date, index=date).drop_duplicates()
		if len(series) == 0:
			return []
		if unit in ['1W','1M']:
			if unit == '1W':
				group_s = series.groupby(lambda x:x.year+x.week*0.001)
			else:
				group_s = series.groupby(lambda x:x.year+x.month*0.001)
			series = group_s.apply(lambda x:x[-1])
			return series.values
		else:
			series = series.apply(func)
			loc_index = series.values%unit==0
			return series[loc_index].index
			
	def get_price(self, security_list, field_list, start, end, unit='1m', fq=None, return_df=False):
		security_list = [security_list] if type(security_list)!=list else security_list
		assert set(field_list).issubset(set(['factor','open','high','low','close','volume','money'])), 'wrong field_list'
		start,end = pd.to_datetime(start).date(),pd.to_datetime(end)
		data_dict = self.get_basic(security_list, field_list, end, unit, fq=fq, start=start)
		input_unit = '1m' if unit[-1]=='m' else '1d'
		data_dict = self.process_basic(data_dict, unit, input_unit, end, field_list, return_df=return_df)
		return data_dict

	def history(self, security_list, field_list, limit, unit='1m', fq=None, return_df=False, end=None):
		security_list = [security_list] if type(security_list)!=list else security_list
		assert set(field_list).issubset(set(['factor','open','high','low','close','volume','money'])), 'wrong field_list'		
		# 注意datetime.now()是中国大陆时间戳，和UTC时间戳相差8个小时，用pd.to_datetime进行转换
		end = pd.to_datetime(self.context.current_dt) if end is None else end
		today_counts = (end.hour-9)*60+end.minute-30 if end.hour<=11 else (end.hour-13+2)*60+end.minute		
		if unit == '1W':
			count = limit * 5
		elif unit == '1M':
			count = limit * 25
		else:
			count = ((limit*int(unit[:-1]))//240+1) * 240 + today_counts

		data_dict = self.get_basic(security_list, field_list, end, unit=unit, fq=fq, limit=count)
		input_unit = '1m' if unit[-1]=='m' else '1d'
		data_dict = self.process_basic(data_dict, unit, input_unit, end, field_list, limit=limit, return_df=return_df)
		return data_dict

	def get_basic(self, security_list, field_list, end, unit, fq, start=None, limit=None):
		# 获取基础数据
		def aggregate(data, field_list):
			data = np.array(data)
			temp = []
			for i,field in enumerate(field_list):
				if field in ['date','factor','close']:
					temp.append(data[:,-1,i])
				elif field == 'open':
					temp.append(data[:,0,i])
				elif field == 'high':
					temp.append(data[:,:,i].max(axis=1))
				elif field == 'low':
					temp.append(data[:,:,i].min(axis=1))
				elif field == 'volume':
					temp.append(data[:,:,i].sum(axis=1))
			data = np.stack(temp, axis=1)[:,np.newaxis,:]
			return data
		outcols = [i for i in ['date','factor','open','high','low','close','volume'] if i in ['date','factor']+field_list]		
		p_index = [outcols.index(i) for i in outcols if i in ['open','high','low','close']]
		v_index = [outcols.index(i) for i in outcols if i == 'volume']
		c_index = [outcols.index(i) for i in outcols if i == 'close']
		day_start = pd.to_datetime(end.strftime('%Y-%m-%d')+' 09:31:00')
		day_end = pd.to_datetime(end.strftime('%Y-%m-%d')+' 15:00:00')		
		end_timestamp = end.timestamp()
		data_dict, today_data = {},{}

		if self.context.run_params['mode'] == 'sim_trade':
			count = 0
			while count<60:
				data, today_stocks = DataBcolz.get_today_data(self.redis_conn)
				now_time = datetime.utcfromtimestamp(data[-1,0,0])
				if now_time.time() == datetime.now().time():
					break
				else:
					time.sleep(1)
					count += 1
			data = data[data[:,0,0]<=end_timestamp]
			if len(data)>0:
				not_paused = (data[:,:,-1]==0).transpose((1,0)).all(axis=1)
				not_paused = np.array(range(len(today_stocks)))[not_paused]
				data = data[:,not_paused,:]
				stocks = [today_stocks[i] for i in not_paused]
				data = data[:,:,[self.todayData_field.index(i) for i in outcols]].transpose((1,0,2))
				if unit in ['1W','1M'] or unit[-1]=='D':
					data = aggregate(data, outcols)
				today_data = dict(zip(stocks, data))
		else:
			if unit in ['1W','1M'] or unit[-1]=='D':
				if end != day_end:
					temp1,temp2 = [],[]
					for security in security_list:	
						data = self.get_bcolz(security, outcols, start=day_start, end=end, unit='1m')
						if data is None or len(data)==0:
							continue
						temp1.append(data)
						temp2.append(security)
					if len(temp1)>0:
						temp1 = aggregate(temp1, outcols)
						today_data = dict(zip(temp2, temp1))

		# 注意end的含义
		end = None if self.context.run_params['mode']=='sim_trade' and start is None else end	
		unit = '1d' if unit in ['1W','1M'] or unit[-1]=='D' else '1m'
		for security in security_list:
			data = self.get_bcolz(security, outcols, start=start, end=end, limit=limit, unit=unit)
			if data is None or len(data)==0:
				continue
			if security in today_data:
				_today_data = today_data[security]
				if _today_data[0,0] > data[-1,0]:
					data = np.append(data, _today_data, axis=0)
			# 成交量需要除权
			data[:,v_index] = data[:,v_index] * data[:,1:2]
			if 'money' in field_list:
				money = data[:,c_index] * data[:,v_index]
				data = np.concatenate([data, money], axis=1)
			if fq == 'post':
				factor = data[:,1:2]/data[0,1]
				data[:,p_index] = (data[:,p_index] * factor).round(2)
			elif fq == 'pre':
				factor = data[:,1:2]/data[-1,1]
				data[:,p_index] = (data[:,p_index] * factor).round(2)
			data_dict[security] = data
		return data_dict

	def process_basic(self, input_dict, output_unit, input_unit, end, field_list, limit=None, return_df=True):
		# 基础数据转换
		outcols = [i for i in ['date','factor','open','high','low','close','volume','money'] if i in ['date','factor']+field_list]		
		return_id = [outcols.index(i) for i in field_list] 
		p_index = [outcols.index(i) for i in outcols if i in ['open','high','low','close']]		

		data_dict_list = []	
		output_unit_list = [output_unit] if type(output_unit)==str else output_unit
		for output_unit in output_unit_list:
			unit_int = int(output_unit[:-1])		
			data_dict = {}
			if (output_unit in ['1W','1M'] or output_unit[-1]=='D') and input_unit=='1d':
				# N日线、周线、月线
				for security in input_dict:				
					data = input_dict[security]			
					if output_unit in ['1W','1M']:
						df = pd.DataFrame(data, columns=outcols)
						df['date'] = df['date'].apply(lambda x:datetime.utcfromtimestamp(x))
						df.index = df['date'].values					
						if output_unit == '1W':
							group_df = df.groupby(lambda x:x.year+x.week*0.001)
						elif output_unit == '1M':
							group_df = df.groupby(lambda x:x.year+x.month*0.01)
						temp = {}
						for field in outcols:
							if field in ['date','close','factor']:
								temp[field] = group_df[field].apply(lambda x:x[-1])
							elif field == 'open':
								temp[field] = group_df[field].apply(lambda x:x[0])
							elif field == 'high':
								temp[field] = group_df[field].apply(lambda x:x.max())
							elif field == 'low':
								temp[field] = group_df[field].apply(lambda x:x.min())
							elif field in ['volume','money']:
								temp[field] = group_df[field].apply(lambda x:x.sum())							
						data = pd.DataFrame.from_dict(temp)
						data = data[outcols]
						if return_df:
							data = data.set_index('date')
							data = data[field_list]
						else:
							data['date'] = data['date'].apply(lambda x:x.timestamp())
							data = data.values
							data = data[:,return_id]
					else:
						if unit_int>1:
							data = self.reshape(data, field_list=outcols, unit=unit_int)
						if return_df:
							data = pd.DataFrame(data, columns=outcols)
							data['date'] = data['date'].apply(lambda x: datetime.utcfromtimestamp(x))
							data = data.set_index('date')
							data = data[field_list]
						else:
							data = data[:,return_id]
					if limit is not None:
						data = data[-limit:]
					data_dict[security] = data
			else:									
				# 分钟线
				for security in input_dict:
					data = input_dict[security]
					if unit_int > 1:
						data = self.reshape(data, field_list=outcols, unit=unit_int)				
					if return_df:
						data = pd.DataFrame(data, columns=outcols)
						data['date'] = data['date'].apply(lambda x: datetime.utcfromtimestamp(x))
						data = data.set_index('date')
						data = data[field_list]
					else:
						data = data[:,return_id]
					if limit is not None:
						data = data[-limit:]
					data_dict[security] = data
			data_dict_list.append(data_dict)
		data_dict_list = data_dict_list[0] if len(data_dict_list)==1 else data_dict_list
		return data_dict_list

	def reshape(self, input_array, field_list, unit, limit=None):
		list_of_array = [input_array[i:i+unit,:] for i in range(0, input_array.shape[0], unit)]		
		data = []
		for i,field in enumerate(field_list):
			if field in ['date','factor','close']:
				d = np.array(list(map(lambda x:x[-1,i], list_of_array)))
				data.append(d)
			elif field == 'open':
				d = np.array(list(map(lambda x:x[0,i], list_of_array)))
				data.append(d)						
			elif field == 'high':
				d = np.array(list(map(lambda x:x[:,i].max(axis=0), list_of_array)))
				data.append(d)
			elif field == 'low':
				d = np.array(list(map(lambda x:x[:,i].min(axis=0), list_of_array)))
				data.append(d)
			elif field in ['volume','money']:
				d = np.array(list(map(lambda x:x[:,i].sum(axis=0), list_of_array)))
				data.append(d)
		array = np.stack(data, axis=1)		
		return array

	@staticmethod
	def get_bcolz(security, outcols, start=None, end=None, limit=None, unit='1m', path=None):
		outcols = [outcols] if type(outcols)!=list else outcols
		if path is None:
			path = DataBcolz.get_path(security, unit)
			if not os.path.exists(path):
				print('{} not exists data path'.format(security))
				return None

		if end is None and limit is not None:
			ctable = bcolz.open(path, mode='r')
			columns = ctable.names
			index = [columns.index(i) for i in outcols]
			ctable = ctable[-limit:]
			array = np.array(list(map(lambda x:list(x), ctable)))
			array = array[:,index]
		elif end and limit is not None:
			end = pd.to_datetime(end).timestamp()
			ctable = bcolz.open(path, mode='r')
			ctable = ctable.fetchwhere('date<={}'.format(end), outcols=outcols)[-limit:]
			array = np.array(list(map(lambda x:list(x), ctable)))			
		elif start and end:
			end = pd.to_datetime(end).timestamp()
			start = pd.to_datetime(start).timestamp()
			ctable = bcolz.open(path, mode='r')
			ctable = ctable.fetchwhere('(date<={})&(date>={})'.format(end, start), outcols=outcols)
			array = np.array(list(map(lambda x:list(x), ctable)))
		else:
			raise Exception('wrong params for get_bcolz')
		return array

	def run_before_trading_start(self):
		pass

	def run_every_minute(self):
		pass

	def run_after_trading_end(self):
		self.init_cache()

if __name__ == '__main__':
	class Context(object):
		def __init__(self):
			self.run_params = {'mode':'backtest'}
			self.current_dt = pd.to_datetime('2021-06-25 00:00:00')
	q = Query(context=Context())












