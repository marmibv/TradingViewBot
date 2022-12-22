from datetime import datetime, timedelta
from math import trunc
from random import random
from StockAPI import StockAPI
from StockAPI import OrderStatus, OptionOrder, OptionContract, Position, TradePlatformApi
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.order_state import *
from ibapi.order_condition import *
from ibapi.common import *
import threading
import logging

logger = logging.getLogger('IBKR')

class IBKRTradeApi(StockAPI, TradePlatformApi, EWrapper, EClient):
	def __init__(self):
		EClient.__init__(self, self)
		self.subscribers = {}
		self.orders = {}
		self.ibkrOrders = {}
		self.url = f"127.0.0.1"
		self._authenticated = threading.Event()
		self._orderEvent = threading.Event()
		self.evOptionContractDetails = threading.Event()
		self.evTickPrice = threading.Event()
		self.task = None
		self.marketDataRequestId = 1

	def GetAsyncCoroutine(self):
		return self.task

	@classmethod
	async def withInit(cls):
		self = cls()
		await self.Startup()
		return self

	async def Startup(self):
		self.disconnect()
		await self.Connect()
		self.task = threading.Thread(target=self.run, daemon=True)
		self.task.start()

	async def Connect(self):
		logger.debug('Connecting to IBKR streaming...')
		self.connect(self.url, 7497, int(random()*10000))
		logger.debug('Connected to IBKR streaming.')
		await self._authenticate()

	def Shutdown(self):
		self.cancelAccountSummary()
		self.cancelAccountUpdatesMulti()
		self.cancelCalculateImpliedVolatility()
		self.cancelCalculateOptionPrice()
		self.cancelFundamentalData()
		self.cancelHeadTimeStamp()
		self.cancelHistogramData()
		self.cancelHistoricalData()
		self.cancelMktData()
		self.cancelMktDepth()
		self.cancelNewsBulletins()
		self.cancelOrder()
		self.cancelPnL()
		self.cancelPnLSingle()
		self.cancelPositions()
		self.cancelPositionsMulti()
		self.cancelRealTimeBars()
		self.cancelScannerSubscription()
		self.cancelTickByTickData()
		self.disconnect()

	# IBKR EWrapper callback
	# Called with next valid order ID (permanently unique monotonically increasing order ID from IBKR)
	def nextValidId(self, orderId: int):
		super().nextValidId(orderId)
		self.nextValidOrderId = orderId
		logger.error(f"NextValidId: {orderId}")
		# we can start now
		self._authenticated.set()

	# Callback to receive a (partial) list of open orders
	def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
		super().openOrder(orderId, contract, order, orderState)
		logger.debug(f"OpenOrder. PermId: {order.permId}, ClientId: {order.clientId}, OrderId: {orderId}, Account: {order.account}, Symbol: {contract.symbol}")
			# "SecType:", contract.secType,
			#   "Exchange:", contract.exchange, "Action:", order.action, "OrderType:", order.orderType,
			#   "TotalQty:", order.totalQuantity, "CashQty:", order.cashQty, 
			#   "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status)

		order.contract = contract
		self.orders[orderId] = order

	# End of list of open orders
	def openOrderEnd(self):
		super().openOrderEnd()
		logging.debug(f'Received {len(self.ibkrOrders)} openOrders')

	# Real time update of an open order.  ie: partial fill, completed, cancelled.
	def orderStatus(self, orderId: OrderId, status: str, filled: float, remaining: float, avgFillPrice: float, permId: int,
					parentId: int, lastFillPrice: float, clientId: int, whyHeld: str, mktCapPrice: float):
		super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
		logger.debug("OrderStatus. Id:", orderId, "Status:", status, "Filled:", filled, "Remaining:", remaining, "AvgFillPrice:", avgFillPrice,
			  "PermId:", permId, "ParentId:", parentId, "LastFillPrice:", lastFillPrice, "ClientId:", clientId, "WhyHeld:", whyHeld, "MktCapPrice:", mktCapPrice)
		if orderId in self.orders:
			ibkrOrder = self.orders[orderId]
			symbol = ibkrOrder.contract.localSymbol[:3] + '-' + ibkrOrder.contract.localSymbol[4:]
			if symbol in self.subscribers:
				subscriber = self.subscribers[symbol]
				if filled >= ibkrOrder.totalQuantity:
					os = OrderStatus(symbol, float(filled), ibkrOrder.action, float(avgFillPrice), status='filled')
					subscriber(os)
		# TODO else conditions?
		# TODO Handle cancel order, failed order, all the things that can go wrong

	def tickPrice(self, reqId: TickerId, tickType, price: float, attrib: TickAttrib):
		self.tickInfo = {
			'reqId': reqId,
			'tickType': tickType,
			'price': price,
			'attrib': attrib
		}
		self.evTickPrice.set()
		return super().tickPrice(reqId, tickType, price, attrib)

	def position(self, account, contract: Contract, pos, avgCost):
		pos = Position(contract.tradingClass, pos, avgCost, avgCost)
		self.positions.append(pos)
		logger.debug(pos)

	def contractDetails(self, reqId: int, contractDetails):
		self.optionContractDetails = contractDetails
		self.evOptionContractDetails.set()

	async def SubscribeToTrades(self):
		self._authenticated.wait()
		logger.debug(f'Subscribed to IBKR trades - NO-OP')
		# Nothing to do, IBKR api is automatically subscribed to trade updates via EWrapper/EClient

	async def UnsubscribeFromTrades(self):
		self._authenticated.wait()
		logger.debug(f'Unsubscribed from IBKR trades - NO-OP')
		# Nothing to do

	async def _authenticate(self):
		logger.debug('Authenticating to IBKR local endpoint - NO-OP')
		# Nothing to do here, IBKR authentication is handled external to this application, through the IB Gateway or the TWS client
		# Local connection to the API gateway running on local machine is by whitelisted IP, no authentication required.
		logger.debug('Authenticated to IBKR streaming.')

	def SubscribeTickerOrders(self, ticker, subscriber):
		self.subscribers[ticker] = subscriber

	def UnsubscribeTickerOrders(self, ticker):
		del self.subscribers[ticker]

	async def ReSubscribe(self):
		await self.SubscribeToTrades()

	def GetTradeableStocks(self):
		assert('IBKR-GetShortableStocks Not supported')
		return None
		
	def GetShortableStocks(self):
		assert('IBKR-GetShortableStocks Not supported')
		return None

	def GetOpenOrders(self):
		self.reqAllOpenOrders()
		# Data will be returned in callbacks openOrder and orderStatus, need to make this an async
		return None

		# Return data expected as follows:
		# orders = []
		# for oo in oos:
		# 	orders.append(Order(oo.symbol, oo.side, oo.type, oo.limit_price, oo.qty, oo.status, oo.client_order_id, nativeID=oo.id))
		# return orders

	def GetOpenOrderFor(self, symbol):
		order = None
		oos = self.GetOpenOrders()
		if oos:
			for oo in oos:
				if oo.symbol == symbol:
					order = oo
		return order
	
	def UpdateOrderPrice(self, ticker, newLimit):
		oo = self.GetOpenOrderFor(ticker)
		if oo:
			self.api.replace_order(oo.nativeOrderID, limit_price=newLimit)
		else:
			logger.error(f'Unable to update order for: {ticker} must already be closed')

	async def GetCurrentPrice(self, symbol):
		return 282

	async def GetCurrentOptionPrice(self, symbol, right, strike, expiry):
		contract = self._createOptionContract(symbol, right, strike, expiry)
		self.reqMktData(self.marketDataRequestId,  contract, '', True, False, '')
		self.marketDataRequestId += 1
		if self.evTickPrice.wait(30):
			self.evTickPrice.clear()
			contract.price = self.tickInfo['price']
		else:
			# Timeout
			pass
		return contract

	async def GetCurrentPrices(self, symbols):
		prices = []
		for symbol in symbols:
			contract = self._createOptionContract(symbol, 'call', 282, '20221102')
			self.reqMktData(self.marketDataRequestId,  contract, '', True)
			self.marketDataRequestId += 1
			self.evTickPrice.wait()
			self.evTickPrice.clear()
			prices[symbol] = self.tickInfo.price
		return []

	async def GetCurrentPositions(self):
		self._authenticated.wait()
		self.positions = []
		try:
			self.reqPositions()
		except Exception:
			logger.exception("Failed to get current positions")
		return self.positions

	async def GetOptionContractDetails(self, symbol, right, strike, targetDay):
		contracts = []
		contractAtm = self._createOptionContract(symbol, right, strike, targetDay)
		contractItm = self._createOptionContract(symbol, right, strike - 1, targetDay)

		self.reqContractDetails(self.nextValidOrderId, contractItm)
		self.evOptionContractDetails.wait()
		self.evOptionContractDetails.clear()
		if hasattr(self.optionContractDetails, 'ask'):
			# TODO - create an optionOrder object from optionContractDetails
			contracts.append(self.optionContractDetails.ask)

		self.reqContractDetails(self.nextValidOrderId, contractAtm)
		self.evOptionContractDetails.wait()
		self.evOptionContractDetails.clear()
		if hasattr(self.optionContractDetails, 'ask'):
			# TODO - create an optionOrder object from optionContractDetails
			contracts.append(self.optionContractDetails.ask)
		return contracts

	async def GetCurrentOptionPrices_Atm_Closest(self, symbol, right, close):
		contracts = []
		self._authenticated.wait()
		today = datetime.now()
		dayOfWeek = today.weekday() #0 = Monday, 6 = Sunday
		hourOfDay = today.hour # What time zone is this in?  Local?
		# QQQ, SPY has options on days 0, 1, 2, 3, 4 (M, T, W, Th, F)
		# Other tickers usually day 4 (F)
		targetDay = today
		if (symbol == 'QQQ' or symbol == 'SPY') and dayOfWeek < 4:
			if hourOfDay > 10: # If after 10am on 0DTE contracts, go to next contract
				targetDay = today + timedelta(days = 1)
		elif dayOfWeek == 0:
			targetDay = today + timedelta(days = 4)
		elif dayOfWeek == 1:
			targetDay = today + timedelta(days=3)
		elif dayOfWeek == 2:
			targetDay = today + timedelta(days=2)
		elif dayOfWeek == 3:
			targetDay = today + timedelta(days=1)
		elif dayOfWeek == 4:
			if hourOfDay > 10: # If after 10am on Friday (0DTE), go to monday's contract
				targetDay = today + timedelta(days = 3)
		elif dayOfWeek == 5: # Saturday - go to monday
			targetDay = today + timedelta(days = 2)
		elif dayOfWeek == 6: # Sunday - go to monday
			targetDay = today + timedelta(days = 1)
   
		strike = trunc(close)
		itmContract = await self.GetCurrentOptionPrice(symbol, right, strike, targetDay)
		atmContract = await self.GetCurrentOptionPrice(symbol, right, strike - 1 if right == 'call' else strike + 1, targetDay)
		contracts.append(itmContract)
		contracts.append(atmContract)

		return contracts

	def GetCurrentPositionFor(self, symbol):
		self._authenticated.wait()
		position = None
		pss = self.GetCurrentPositions()
		for ps in pss:
			if ps.symbol == symbol:
				position = ps
		return position

	def _createOptionContract(self, symbol, right, strike, date):
		contract = Contract()
		contract.symbol = symbol
		contract.multiplier = 100
		contract.secType = "OPT"
		contract.exchange = "SMART"
		contract.currency = 'USD'
		contract.lastTradeDateOrContractMonth = date if type(date) is str else date.strftime("%Y%m%d")
		contract.strike = strike
		contract.right = 'C' if right == 'call' else 'P'
		return contract

	#Function to create FX Order contract
	def _createForexContract(self, symbol):
		contract = Contract()
		contract.symbol = symbol[:3]
		contract.secType = 'CASH'
		contract.exchange = 'IDEALPRO'
		contract.currency = symbol[4:]
		return contract

	def _createCryptoContract(self, symbol):
		contract = Contract()
		contract.symbol = ''
		return contract

	def _createOrder(self, action: str, price: float, qty: int, kind: str):
		# Create order object
		order = Order()
		order.action = action.upper()
		order.totalQuantity = qty
		order.orderType = 'LMT' if kind == 'limit' else 'MKT'
		order.lmtPrice = price
		return order

	def _createMarketOrder(self, action, qty):
		# Create order object
		order = Order()
		order.action = action.upper()
		order.totalQuantity = qty
		order.orderType = 'MKT'
		order.eTradeOnly = False
		order.firmQuoteOnly = False
		return order

	def PlaceStockOrder(self, oo):
		self._authenticated.wait()
		retval = None
		contract = self._createForexContract(oo.symbol)
		order = self._createOrder(oo.side, oo.price, oo.quantity, oo.kind)
		self.placeOrder(self.nextValidOrderId, contract, order)
		self.orders[self.nextValidOrderId] = oo
		self.nextValidOrderId += 1
		return retval

	async def SellMarketOptionOrder(self, symbol, right, strike, expiry, quantity):
		mktContract = self._createOptionContract(symbol, right, strike, expiry)
		mktOrder = self._createMarketOrder('sell', quantity)
		status = self.placeOrder(self.nextValidOrderId, mktContract, mktOrder)
		oo = OptionOrder(symbol, right, 'sell', expiry, strike, 0)
		oo.quantity = quantity
		self.orders[self.nextValidOrderId] = oo
		self.nextValidOrderId += 1
		return status

	async def BuyMarketOptionOrder(self, optionOrder:OptionOrder, close: float):
		self._authenticated.wait()
		retval = None
		contracts = await self.GetCurrentOptionPrices_Atm_Closest(optionOrder.symbol, optionOrder.right, close)
		for contract in contracts:
			quantity = trunc(optionOrder.allocation / (contract.price * 100))
			if quantity > 0:
				mktContract = self._createOptionContract(optionOrder.symbol, optionOrder.right, contract.strike, contract.lastTradeDateOrContractMonth)
				mktOrder = self._createMarketOrder(optionOrder.side, quantity)
				status = self.placeOrder(self.nextValidOrderId, mktContract, mktOrder)
				self.orders[self.nextValidOrderId] = optionOrder
				self.nextValidOrderId += 1
				optionOrder.status = 'success'
				optionOrder.quantity = quantity
				optionOrder.price = contract.price
				retval = optionOrder
			else:
				optionOrder.status = 'insufficient funds'
				optionOrder.quantity = 0
				optionOrder.price = contract.price
				logger.error(optionOrder)
		else:
			optionOrder.status = 'no options found'
			optionOrder.quantity = 0
			optionOrder.price = None
			logger.error(optionOrder)
		return optionOrder
