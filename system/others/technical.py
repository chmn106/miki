import numpy as np 
import pandas as pd
import os, sys
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

	def get_technical_data(self, array, types, params=None):
		assert array.shape[-1]==5, 'input array should be open,high,low,close,volume'

		if types == 'kdj':
			length = array.shape[1]
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
			array = np.stack([k,d,j], axis=-1).round(2)
			array = np.append(np.zeros((array.shape[0], length-array.shape[1], array.shape[2])), array, axis=1)
		elif types == 'macd':
			length = array.shape[1]
			(p1,p2,p3) = params
			c = array[:,:,3]
			dif = self.cal_ema(c, p1) - self.cal_ema(c, p2)
			dea = self.cal_ema(dif, p3)
			macd = (dif-dea)*2
			array = np.stack([dif,dea,macd], axis=-1).round(4)
			array = np.append(np.zeros((array.shape[0], length-array.shape[1], array.shape[2])), array, axis=1)
		elif types == 'boll':
			length = array.shape[1]
			win = self.rolling_window(array[:,:,3], params)
			mid = win.mean(axis=-1)
			std = win.std(axis=-1)
			up = mid+2*std
			down = mid-2*std
			array = np.stack([up,mid,down], axis=-1).round(4)			
			array = np.append(np.zeros((array.shape[0], length-array.shape[1], array.shape[2])), array, axis=1)
		else:
			raise Exception('unknow types {}'.format(types))		
		return array

if __name__ == '__main__':
	t = Technical()


		