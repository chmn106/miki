import pandas as pd
import numpy as np
import os, sys
sys.path.append(os.getcwd()[:2]+'/Miki/system/data')
from dataGlovar import DataPath


class BaseFactor(object):
	# 因子模块
	def __init__(self):
		super(BaseFactor, self).__init__()

	def to_pickle(self, df, name):
		os.makedirs(DataPath+'/cache/factor', exist_ok=True)
		path = DataPath+'/cache/factor/{}.pkl'.format(name)
		if os.path.exists(path):
			old_df = pd.read_pickle(path)
			df = df[df.index>old_df.index[-1]]
			df = pd.concat([old_df,df], axis=0)
		df.to_pickle(path)

	def from_pickle(self, name):
		path = DataPath+'/cache/factor/{}.pkl'.format(name)
		if os.path.exists(path):
			df = pd.read_pickle(path)
			return df
		else:
			raise Exception('not exists {}'.format(name))












