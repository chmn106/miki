import logging
import sys, os
from datetime import datetime


class Logger(object):
	# 日志模块
	def __init__(self, log_path):		
		self.info_logger = None				
		self.debug_logger = None				
		self.warn_logger = None				
		self.error_logger = None		
		self.now_time = str(datetime.now())
		self.log_path = sys.path[0]+'/log/{}'.format(log_path)
		os.makedirs(self.log_path, exist_ok=True)

	def update(self, now_time):
		self.now_time = str(now_time)
		
	def setup_logger(self, name, log_file):
		handler = logging.FileHandler(log_file, mode='a')        
		logger = logging.getLogger(name)
		logger.setLevel(logging.INFO)
		logger.addHandler(handler)
		return logger
					
	def info(self, message):
		if self.info_logger is None:
			self.info_logger = self.setup_logger(name='info', log_file=self.log_path+'/info.log')
		self.info_logger.info(self.now_time+' '+message)
		print(message)

	def debug(self, message):
		if self.debug_logger is None:
			self.debug_logger = self.setup_logger(name='debug', log_file=self.log_path+'/debug.log')
		self.debug_logger.info(self.now_time+' '+message)

	def warn(self, message):
		if self.warn_logger is None:
			self.warn_logger = self.setup_logger(name='warn', log_file=self.log_path+'/warn.log')
		self.warn_logger.info(self.now_time+' '+message)
		print(message)

	def error(self, message):
		if self.error_logger is None:
			self.error_logger = self.setup_logger(name='error', log_file=self.log_path+'/error.log')
		self.error_logger.info(self.now_time+' '+message)
		print(message)



