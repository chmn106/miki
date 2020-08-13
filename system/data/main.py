from datetime import datetime 
import time
import sys, os
import subprocess
from dataBcolz import DataBcolz
from dataSQL import DataSQL
from basicSchedule import BasicSchedule
import win32com.client
import win32api


'''
	1.数据调度总模块，实盘运行

'''
class MikiData(BasicSchedule):
	def __init__(self):
		super(MikiData, self).__init__()
		self.dataBcolz = DataBcolz()
		self.dataSQL = DataSQL()

	def run_before_trading_start(self):
		# 盘前运行或者盘后测试运行
		self.dataBcolz.run_before_trading_start()
		print('run dataBcolz before_trading_start success')
		self.dataSQL.run_before_trading_start()
		print('run dataSQL before_trading_start success')

	def run_every_minute(self):
		self.dataBcolz.run_every_minute()

	def run_after_trading_end(self):
		# 盘后运行
		self.dataBcolz.run_after_trading_end()
		print('run dataBcolz after_trading_end success')		
		self.dataSQL.run_after_trading_end()
		print('run dataSQL after_trading_end success')

def notice(alert_str='程序运行完成!', alert_times=5):
	speak_out = win32com.client.Dispatch('SAPI.SPVOICE')
	for i in range(alert_times):
		speak_out.Speak(alert_str)
		time.sleep(0.5)

if __name__ == '__main__':
	try:
		m = MikiData()
		m.run()
	except Exception as e:
		print(e)
		notice('数据程序运行出错', 100)


	