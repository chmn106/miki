import pandas as pd
import numpy as np
import os, pickle, pyecharts, yaml


class TradeFunction(object):
	# 实现常见函数
	def __init__(self):
		super(TradeFunction, self).__init__()	
	@staticmethod	
	def to_pickle(data, path):
		with open(path, 'wb') as f:
			pickle.dump(data, f)
	@staticmethod
	def from_pickle(path):
		with open(path, 'rb') as f:
			data = pickle.load(f)
		return data
	@staticmethod	
	def to_yaml(data, path):
		with open(path, 'wb') as f:
			f.write(yaml.dump(data, encoding='utf-8', allow_unicode=True))
	@staticmethod
	def from_yaml(path):
		with open(path, encoding='utf-8') as f:
			data = yaml.load(f, Loader=yaml.FullLoader)
		return data
	@staticmethod
	def calculate(value_history, order_history, begin_cash, end_cash, print_result):
		# 结果计算
		def func(x):
		    return 1-x[-1]/(x.max()+1e-5)
		date_array, value_array, _ = np.array(value_history).transpose((1,0))
		total_commission,total_slipcost,profitRate_array,profit_array = 0,0,[],[]
		for order in order_history:
			total_commission += order.commission
			total_slipcost += order.slipcost
			if not order.is_buy:
				rate = order.profit/(order.filled*order.price-order.profit)
				profit_array.append(order.profit)
				profitRate_array.append(rate)
		total_counts = len(order_history)
		profit_array = np.array(profit_array)
		profitRate_array = np.array(profitRate_array)
		# 默认变量赋值
		yl_times,yl_avg,ks_times,ks_avg,yk_ratio,max_yl_rate,max_ks_rate = 0,0,0,0,0,0,0
		max_profitRate,avg_profitRate,win_rate = 0,0,0
		if len(profit_array)>0:
			avg_profitRate = profitRate_array.mean()
			max_profitRate = profitRate_array.max()
			yl_array = profit_array[profit_array>0]
			ks_array = profit_array[profit_array<=0]
			win_rate = len(yl_array) / max(len(profit_array),1)
			if len(yl_array) > 0:
				max_yl_rate = yl_array.max()/yl_array.sum()
				yl_times = len(yl_array)
				yl_avg = yl_array.mean()
			if len(ks_array) > 0:
				max_ks_rate = ks_array.min()/ks_array.sum()
				ks_times = len(ks_array)
				ks_avg = ks_array.mean()
				yk_ratio = abs(yl_avg/ks_avg)

		retrace_array = np.array(list(map(lambda x: func(value_array[:x]), range(1,len(value_array)+1))))
		max_retrace = retrace_array.max()
		max_retrace_date = date_array[retrace_array.argmax()].date()
		max_return = value_array.max()/value_array[0]-1
		min_return = value_array.min()/value_array[0]-1
		value_array = value_array/value_array[0]-1
		years = max((date_array[-1]-date_array[0]).days, 1)/365
		annual_rate = (end_cash/begin_cash)**(1/years)-1
		message = '''
					 首个交易日：{} 
					 最后交易日：{} 
					 起始资金：{:.2f}
					 结束资金：{:.2f}
					 总收益率：{:.2%}  
					 年化收益率：{:.2%}       			      			
					 总交易次数：{}
					 总佣金：{:.2f}
					 平均每笔佣金：{:.2f}
					 总滑点：{:.2f}
					 平均每笔滑点：{:.2f}
					 胜率：{:.2%}
					 最高收益率：{:.2%}
					 最低收益率：{:.2%}
					 最大回撤率：{:.2%}
					 最大回撤发生时间：{}  
					 盈利次数：{}
					 亏损次数：{}
					 平均每笔盈利：{:.2f}
					 平均每笔亏损：{:.2f}
					 盈亏比：{:.2f}
					 平均每笔收益率：{:.2%}
					 单笔最大收益率：{:.2%}
					 最大收益占比：{:.2%}
					 最大亏损占比：{:.2%}
				  '''.format(date_array[0].date(), \
				  			 date_array[-1].date(), \
				  			 begin_cash, \
				  			 end_cash, \
				  			 end_cash/begin_cash-1, \
				  			 annual_rate, \
				  			 total_counts, \
				  			 total_commission, \
				  			 total_commission/max(1,total_counts), \
				  			 total_slipcost, \
				  			 total_slipcost/max(1,total_counts), \
				  			 win_rate, \
				  			 max_return, \
				  			 min_return, \
				  			 max_retrace, \
				  			 max_retrace_date, \
				  			 yl_times, \
				  			 ks_times, \
				  			 yl_avg, \
				  			 -ks_avg, \
				  			 yk_ratio, \
				  			 avg_profitRate, \
				  			 max_profitRate, \
				  			 max_yl_rate, \
				  			 max_ks_rate)
		if print_result:
			print(message)
		return value_array,date_array,max_retrace,max_retrace_date,max_return,min_return,annual_rate
	@staticmethod
	def plot_value(value_history, order_history, begin_cash, end_cash, save_path, print_result, open_file=False):
		# 显示资金曲线
		os.makedirs(save_path, exist_ok=True)
		beta_array = np.array(value_history)[:,-1]
		value_array,date_array,max_retrace,max_retrace_date,max_return,min_return,annual_rate = TradeFunction.calculate(value_history, \
			order_history=order_history, begin_cash=begin_cash, end_cash=end_cash, print_result=print_result)
		
		date_list = [i.strftime('%Y-%m-%d %H:%M:%S') for i in date_array]
		subtitle = "最大回撤 {} {:.2%},  最高收益率 {:.2%} 最低收益率 {:.2%},  目前收益率 {:.2%},  年化收益率 {:.2%}".format(max_retrace_date,max_retrace,max_return,min_return,value_array[-1],annual_rate)
		beta = beta_array/beta_array[0]-1
		alpha = value_array - beta

		line = pyecharts.charts.Line()
		line.add_xaxis(date_list)
		line.add_yaxis("策略", value_array.astype('float').round(3).tolist())
		line.add_yaxis('Beta', beta.astype('float').round(3).tolist(), is_symbol_show=False)
		line.add_yaxis("Alpha", alpha.astype('float').round(3).tolist(), is_symbol_show=False)	
		line.set_global_opts(title_opts=pyecharts.options.TitleOpts(title='资金曲线',subtitle=subtitle),
							 xaxis_opts=pyecharts.options.AxisOpts(is_scale=True),
							 yaxis_opts=pyecharts.options.AxisOpts(is_scale=True,splitarea_opts=pyecharts.options.SplitAreaOpts(is_show=True, areastyle_opts=pyecharts.options.AreaStyleOpts(opacity=1))),
							 datazoom_opts=[pyecharts.options.DataZoomOpts()])    
		line.render('{}/result.html'.format(save_path))
		if open_file:
			os.startfile(os.path.abspath('{}/result.html'.format(save_path)))
	@staticmethod
	def plot_portfolio(context, now_time, save_path, display_name=None):
		# 显示持仓
		os.makedirs(save_path, exist_ok=True)
		portfolioDict = context.portfolioDict
		cashForPortfolio = context.cashForPortfolio # 未分配到账户的资金
		total_value = context.total_value
		preDayValue = context.preDayValue
		starting_cash = context.starting_cash
		headers = ['股票代码','股票名称','持仓数量','开仓时间','收益率','资金占比','总资金占比','策略池']
		rows = []
		hold_value,hold_num = 0,0
		for key in portfolioDict:
			portfolio = portfolioDict[key]
			hold_num += len(portfolio.positions)
			portfolio_value = portfolio.total_value
			for security in portfolio.positions.index:
				position = portfolio.positions.loc[security,:]
				if display_name is not None and security in display_name:
					name = display_name[security]
				else:
					name = security
				position_amount = position.amount
				position_value = position.value				
				profit = (position.price-position.avg_cost)*position.amount*position.multiplier*position.side
				init_time = position.init_time.strftime('%Y-%m-%d %H:%M:%S')
				returns_rate = '{:.2%}'.format(profit/position_value)
				hold_rate1 = '{:.2%}'.format(position_value/portfolio_value)
				hold_rate2 = '{:.2%}'.format(position_value/total_value)
				hold_value += position_value
				rows.append([security, name, position_amount, init_time, returns_rate, hold_rate1, hold_rate2, key])
		day_return = total_value/preDayValue-1	
		position_rate = hold_value/total_value
		now_time = now_time.strftime('%Y-%m-%d %H:%M:%S')
		title = '{} 当天收益率：{:.2%} 总资产：{:.0f} 持仓比率：{:.2%} 持股{}只'.format(now_time, day_return, total_value, position_rate, hold_num)
		
		table = pyecharts.components.Table()
		table.add(headers, rows)
		table.set_global_opts(title_opts=pyecharts.options.ComponentTitleOpts(title=title))
		table.render('{}/持仓.html'.format(save_path))




















