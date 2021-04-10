

Miki量化框架  
====
	  1.采用python语言实现的量化框架，原则尽量以简洁的语言实现金融交易等功能，系统架构清晰，方便二次开发，理论支持
	    股票、期货、期权等，目前只支持股票。 
	  2.如果遇到问题，欢迎提交issue、代码，交流QQ群：1042883511。  

框架架构：  
----
	1.api为下单接口实现  
	2.data为数据存储、接收、提取等功能的实现，实盘模式需要运行main.py文件  
	3.strategy为策略实现模块，  
	  1.新建py文件实现策略功能，   
	    initialize 实现策略的初始化，
	    before_trading_start 每天开盘前运行，  
	    handle_data 每分钟运行，
	    after_trading_end 每天收盘后运行，
	    after_backtest_end 回测结束后运行。  
	4.trade主要包含：
	   1.context 上下文会话模块
	   2.logger 日志模块
	   3.order 下单模块
	   4.system 主引擎模块
	   5.strategyVar 全局变量模块
	   6.types 数据类型模块		
































