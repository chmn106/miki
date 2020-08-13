import os
from system.trade.system import System
import warnings
warnings.filterwarnings("ignore")

run_params = {'start_date':'2007-01-01 00:00:00',
			  'end_date':'2020-01-01 00:00:00',
		      'frequency':'1m',
		      'starting_cash':100000, 
		      'mode':'backtest',
		      'strategy':'strategyTest'}

def remove_old_file(path):
	if os.path.exists(path):
	    for i in os.listdir(path):
	        path_file = os.path.join(path,i)
	        if os.path.isfile(path_file):
	            os.remove(path_file) 

if __name__ == '__main__':
	remove_old_file('./log/{}/{}'.format(run_params['mode'], run_params['strategy']))
	remove_old_file('./img/{}/{}'.format(run_params['mode'], run_params['strategy']))

	system = System(run_params=run_params, load_context=False, target_stocks=None)
	system.run_backtest()











