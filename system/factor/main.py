from datetime import datetime 
import sys, os
sys.path.extend([os.getcwd()[:2]+'/Miki/system/data', 
				 os.getcwd()[:2]+'/Miki/system/factor'])
from basicSchedule import BasicSchedule
from factorFinance import FactorFinance
from factorPV import FactorPV


class MikiFactor(BasicSchedule):
	def __init__(self):
		super(MikiFactor, self).__init__()
		self.factorFinance = FactorFinance(mode='sim_trade')
		self.factorPV = FactorPV(mode='sim_trade')

	def run_before_trading_start(self):
		self.factorFinance.run_before_trading_start()
		print('run factorFinance before_trading_start success')
		self.factorPV.run_before_trading_start()
		print('run factorPV before_trading_start success')

	def run_every_day(self):
		self.factorFinance.run_every_minute()
		self.factorPV.run_every_minute()

	def run_after_trading_end(self):
		self.factorFinance.run_after_trading_end()
		print('run factorFinance after_trading_end success')		
		self.factorPV.run_after_trading_end()
		print('run factorPV after_trading_end success')

if __name__ == '__main__':
	m = MikiFactor()
	m.run()


















