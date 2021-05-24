import numpy as np 


class Technical(object):
	# 三维数组格式计算技术指标
	def __init__(self):
		pass
	def _rolling_window(self, array, window):
		# array: 2dims
		shape = array.shape[:1] + (array.shape[-1] - window + 1, window)
		strides = array.strides + (array.strides[-1],)
		return np.lib.stride_tricks.as_strided(array, shape=shape, strides=strides)
	def SMA(self, array, p1, p2):
		# SMA(X,N1,N2): Y=((N1-N2)*Y'+X*N2)/N1, array: 2dims
		data = [array[:,0]]
		for i in range(1,array.shape[1]):
			x = array[:,i]
			y = data[-1]
			y = ((p1-p2)*y+x*p2)/p1
			data.append(y)
		array = np.stack(data, axis=-1)
		return array
	def EMA(self, array, p1):
		# EMA(X,N): Y=(X*2+(N-1)*Y')/(N+1), 相当于SMA(X,N+1,2)
		array = self.SMA(array, p1+1, 2)
		return array		
	def MA(self, array, p1):
		# MA(X,N)
		assert array.shape[1]>p1, 'length should > {}'.format(p1)
		array = self._rolling_window(array, p1)
		array = array.mean(axis=-1)
		return array
	def KDJ(self, array, p1, p2, p3):
		assert array.shape[1]>p1, 'length should > {}'.format(p1)
		h_n = self._rolling_window(array[:,:,1], p1)
		h_n = h_n.max(axis=-1)
		l_n = self._rolling_window(array[:,:,2], p1)
		l_n = l_n.min(axis=-1)
		c_n = array[:,p1-1:,3]
		rsv = (c_n-l_n)/(h_n-l_n)*100
		k = self.SMA(rsv, p2, 1)
		d = self.SMA(k, p3, 1)
		j = 3*k-2*d
		return k,d,j
	def MACD(self, array, p1, p2, p3):
		assert array.shape[1]>p2, 'length should > {}'.format(p2)
		close = array[:,:,3]
		dif = self.EMA(close, p1) - self.EMA(close, p2)
		dea = self.EMA(dif, p3)
		macd = (dif-dea)*2
		return dif,dea,macd
	def BOLL(self, array, p1):
		assert array.shape[1]>p1, 'length should > {}'.format(p1)
		win = self._rolling_window(array[:,:,3], p1)
		std = win.std(axis=-1)
		mid = win.mean(axis=-1)
		up = mid+2*std
		down = mid-2*std
		return up,mid,down			


