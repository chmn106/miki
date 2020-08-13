import os
from system.trade.system import System


run_params = {'start_date':'2005-01-01 00:00:00',
			  'end_date':'2100-01-01 00:00:00',
			  'frequency':'1m',
		      'starting_cash':100000, 
		      'mode':'sim_trade',
		      'strategy':'strategy'}

def remove_old_file(path):
	if os.path.exists(path):
	    for i in os.listdir(path):
	        path_file = os.path.join(path,i)
	        if os.path.isfile(path_file):
	            os.remove(path_file) 

if __name__ == '__main__':
	load_context = True
	if not load_context:
		remove_old_file('./log/{}/{}'.format(run_params['mode'], run_params['strategy']))
		remove_old_file('./img/{}/{}'.format(run_params['mode'], run_params['strategy']))

	system = System(run_params=run_params, load_context=load_context)
	system.run_simtrade()
	











