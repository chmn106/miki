import os, logging

class Logger(object):
	# 日志函数
	def __init__(self, path, clear_log=True):		
		self.info_logger = None				
		self.debug_logger = None				
		self.warn_logger = None				
		self.error_logger = None		
		self.log_path = path
		if os.path.exists(self.log_path):
			if clear_log:
				for file in os.listdir(self.log_path):
					with open(os.path.join(self.log_path, file), 'wb') as f:
						f.truncate()
		else:
			os.makedirs(self.log_path, exist_ok=True)
	def init(self, name, log_file):
		handler = logging.FileHandler(log_file, mode='a')        
		logger = logging.getLogger(name)
		logger.setLevel(logging.INFO)
		logger.addHandler(handler)
		return logger					
	def info(self, message):
		if self.info_logger is None:
			self.info_logger = self.init(name='info', log_file=self.log_path+'/info.log')
		self.info_logger.info(message)
		print(message)
	def debug(self, message):
		if self.debug_logger is None:
			self.debug_logger = self.init(name='debug', log_file=self.log_path+'/debug.log')
		self.debug_logger.info(message)
	def warn(self, message):
		if self.warn_logger is None:
			self.warn_logger = self.init(name='warn', log_file=self.log_path+'/warn.log')
		self.warn_logger.info(message)
		print(message)
	def error(self, message):
		if self.error_logger is None:
			self.error_logger = self.init(name='error', log_file=self.log_path+'/error.log')
		self.error_logger.info(message)
		print(message)


