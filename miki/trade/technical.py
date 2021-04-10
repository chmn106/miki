import numpy as np 
import pandas as pd
import os, sys
from datetime import datetime
from functools import reduce


class Technical(object):
	# 三维数组格式计算技术指标
	def __init__(self):
		pass
	def cal_sma(self, array, params):
		# SMA(X,N1,N2): Y=((N1-N2)*Y'+X*N2)/N1, array: 2dims
		data = [array[:,0]]
		for i in range(1,array.shape[1]):
			x = array[:,i]
			y = data[-1]
			y = ((params[0]-params[1])*y+params[1]*x)/params[0]
			data.append(y)
		array = np.stack(data, axis=-1)
		return array
	def cal_ema(self, array, params):
		# EMA(X,N): Y=(X*2+(N-1)*Y')/(N+1), 相当于SMA(X,N+1,2)
		params = (params+1, 2)
		array = self.cal_sma(array, params)
		return array		
	def cal_ma(self, array, params):
		# MA(X,N) array: 2dims
		array = self.rolling_window(array, params)
		array = array.mean(axis=-1)
		return array
	def rolling_window(self, array, window):
		# array: 2dims
		shape = array.shape[:1] + (array.shape[-1] - window + 1, window)
		strides = array.strides + (array.strides[-1],)
		return np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)
	def get_technical_data(self, array, types, params=None, stocks=None):
		array = array[:,:,1:].astype('float') if array.shape[-1]==6 else array
		# open,high,low,close,volume
		if types == 'kdj':
			(p1,p2,p3) = params
			h_n = self.rolling_window(array[:,:,1], p1)
			h_n = h_n.max(axis=-1)
			l_n = self.rolling_window(array[:,:,2], p1)
			l_n = l_n.min(axis=-1)
			c_n = array[:,p1-1:,3]
			rsv = (c_n-l_n)/(h_n-l_n)*100
			k = self.cal_sma(rsv, params=(p2,1))
			d = self.cal_sma(k, params=(p3,1))
			j = 3*k-2*d
			array = np.stack([k,d,j], axis=-1).astype('int')
		elif types == 'macd':
			(p1,p2,p3) = params
			c = array[:,:,3]
			dif = self.cal_ema(c, p1) - self.cal_ema(c, p2)
			dea = self.cal_ema(dif, p3)
			macd = (dif-dea)*2
			array = np.stack([dif,dea,macd], axis=-1).round(4)
		elif types == 'boll':
			length = array.shape[1]
			win = self.rolling_window(array[:,:,3], params)
			std = win.std(axis=-1)
			mid = win.mean(axis=-1)
			up = mid+2*std
			down = mid-2*std
			array = np.stack([up, mid, down], axis=-1).round(4)			
		elif types == 'ma':
			assert len(array.shape)==2, 'input array of MA should be 2 dims !'
			length = array.shape[1]
			array = self.cal_ma(array, params).round(2)
			array = np.append(np.zeros((array.shape[0], length-array.shape[1])), array, axis=1)
		elif types == 'avg_ma':
			length = array.shape[1]
			array = array[:,:,:4].mean(axis=2)
			array = self.cal_ma(array, params).round(2)
			array = np.append(np.zeros((array.shape[0], length-array.shape[1])), array, axis=1)
		elif types == 'is_red':
			array = array[:,:,0]<array[:,:,3]
		elif types == 'hlc_rate':
			high_array = array[:,1:,1]
			low_array = array[:,1:,2]
			pre_close_array = array[:,:-1,3]
			close_array = array[:,1:,3]
			open_array = array[:,1:,0]
			h_pc = high_array/pre_close_array-1
			l_pc = low_array/pre_close_array-1
			c_dh = close_array/high_array-1
			c_dl = close_array/low_array-1
			c_pc = close_array/pre_close_array-1
			o_pc = open_array/pre_close_array-1
			c_o = close_array/open_array-1
			array = np.stack([h_pc, l_pc, c_dh, c_dl, c_pc, o_pc, c_o], axis=-1).round(4)
		elif types == 'ma_gap':
			assert array.shape[1]>=params, 'ma_gap param {} < array length {}'.format(params, array.shape[1])
			length = array.shape[1]
			avg = array[:,:,:4].mean(axis=-1)
			array = self.cal_ma(avg, params).round(2)
			array = avg[:,-array.shape[1]:]/array-1
		else:
			raise Exception('unknow types {}'.format(types))		
		return array
