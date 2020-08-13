

Miki量化框架  
	1.目前量化领域，保密性几乎成为行业的标准，这同时也导致大量的重复性开发，Miki量化框架的宗旨就是希望能减少
	  这种重复性造轮子的情况。  
	2.完全采用python语言实现的量化框架，原则尽量以简洁的语言实现金融的交易等功能，系统架构清晰，方便二次开发，支持股票、期货、期权等。  
	3.如果遇到问题，欢迎提交issue、代码，QQ群：1042883511。  


框架架构：  
	1.system/api为下单接口实现  
	2.system/data为数据存储、接收、提取等功能的实现，实盘模式main.py文件开启  
	3.system/strategy为策略实现模块，新建py文件实现策略功能，  
	  开头通过 from system.trade.strategyVar import * 引入全局变量，具体查看system/trade/strategyVar.py  
	  策略主要有 initialize, before_trading_start, handle_data, after_trading_end, after_backtest_end这些函数  
	  initialize实现策略的初始化，before_trading_start每天开盘前运行，handle_data每推送一个数据运行一次，  
	  after_trading_end每天收盘后运行，after_backtest_end回测结束后运行  
	4.system/trade主要包含context、logger、order等模块，  
	  context为上下文会话，属性具体查看system/trade/context.py文件  
	  order为下单模块，实盘模式下单后通过publish_order发布到redis，下单接口通过订阅redis进行下单  
	  others.yaml为一些配置，ChangeDict为公司股票更名信息，Multiplier为期货合约单位  
	  dataGenerator.py为数据推送引擎  
	5.运行前，建议配备一块1T-2T的固态硬盘，通过system/data/dataOthers的save_old_data存储行情数据到本地，存储  
	  会是G:\StockData\stock1m\03\000603.XSHE这种文件格式，再通过generate_dataUnit函数cache推送数据，系统通过读取cache文件进行数据推送。system/data/dataSQL的update_finance_data会自动从api读取财务数据存储到本地。  
	6.阅读源码也可以对系统架构更熟悉  


项目捐赠：支付宝账号：15012463435



























