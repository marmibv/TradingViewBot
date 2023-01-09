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
		self.tickInfo = {}

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
		self.connect(self.url, 7496, int(random()*10000))
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
			if ibkrOrder.contract:
				symbol = ibkrOrder.contract.localSymbol[:3] + '-' + ibkrOrder.contract.localSymbol[4:]
				if filled >= ibkrOrder.totalQuantity:
					self.lastOrder = OrderStatus(symbol, float(filled), ibkrOrder.action, float(avgFillPrice), orderId, status='filled')
					self._orderEvent.set()
					if symbol in self.subscribers:
						subscriber = self.subscribers[symbol]
						subscriber(self.lastOrder)
		# TODO else conditions?
		# TODO Handle cancel order, failed order, all the things that can go wrong

	def tickPrice(self, reqId: TickerId, tickId: int, price: float, attrib: TickAttrib):
		# Field definitions:
		# 1 - Bid Price
		# 2 - Ask Price
		# 4 - Last Price
		# 6 - High of day
		# 7 - Low of day
		# 9 - Close of last day
		tickIdLookup = {
			'1': 'bid',
			'2': 'ask',
			'4': 'last',
			'6': 'hod',
			'7': 'lod',
			'9': 'close'
		}
		# For most things we care only about the ask
		requestId = f'{reqId}'
		tickId = f'{tickId}'
		if tickId in tickIdLookup:
			if requestId not in self.tickInfo:
				self.tickInfo[requestId] = {}
			req = self.tickInfo[requestId]
			tickType = tickIdLookup[tickId]
			req[tickType] = price
			tickInfo = {
				'reqId': reqId,
				'tickType': tickId,
				'price': price,
				'attrib': attrib
			}
			logger.debug(tickInfo)
			# The ticks come in order, any of these indicate that we have enough information to proceed
			if tickType == 'last' or tickType == 'close' or tickType == 'hod' or tickType == 'lod':
				self.evTickPrice.set()
		return super().tickPrice(reqId, tickId, price, attrib)

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
		# TODO
		# Data will be returned in callbacks openOrder and orderStatus, need to make this an async
		return None


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

	async def GetCurrentPositions(self):
		self._authenticated.wait()
		self.positions = []
		try:
			self.reqPositions()
		except Exception:
			logger.exception("Failed to get current positions")
		return self.positions


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
		contract.right = 'C' if (right.upper() == 'CALL') else 'P'
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
		order.outsideRth = True
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

	def _createLimitOrder(self, action, qty, limit):
		# Create order object
		order = Order()
		order.action = action.upper()
		order.totalQuantity = qty
		order.orderType = 'LMT'
		order.lmtPrice = limit
		order.eTradeOnly = False
		order.firmQuoteOnly = False
		order.outsideRth = True
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

	async def SellOptionsAtMarket(self, symbol, right, strike, expiry, quantity):
		retval = None
		mktContract = self._createOptionContract(symbol, right, strike, expiry)
		if self.AfterHours():
			contract = await self.GetCurrentOptionPrice(symbol, right, strike, expiry)
			limit = (contract.bidPrice + contract.askPrice) / 2
			order = self._createLimitOrder('sell', quantity, limit)
		else:
			order = self._createMarketOrder('sell', quantity)
		oo = OptionOrder(symbol, right, 'sell', expiry, strike, 0)
		oo.quantity = quantity
		self.orders[self.nextValidOrderId] = oo
		self.nextValidOrderId += 1
		self.placeOrder(self.nextValidOrderId, mktContract, order)
		if self._orderEvent.wait(30):
			self._orderEvent.clear()
			oo.price = self.lastOrder.price
			oo.status = 'success'
			retval = oo
		else:
			logger.error(f'Unable to place order {oo}')
		return retval

	def AfterHours(self):
		now = datetime.now()	# TODO - figure out time zones for this thing, perhaps by ticker Local time, PST
		afterHours =  now.hour > 16 or now.hour < 9 or (now.hour == 9 and now.minute >= 30)
		return afterHours

	async def BuyAtmOptionsAtMarket(self, symbol, right, close, allocation):
		retval = None
		self._authenticated.wait()
		contracts = await self.GetOptionContractsAtmClosest(symbol, right, close)
		for contract in contracts:
			quantity = trunc(allocation / (contract.price * 100))
			oo = OptionOrder(symbol, right, 'BUY', contract.lastTradeDateOrContractMonth, contract.strike, allocation, quantity=quantity)
			if quantity > 0:
				self.orders[self.nextValidOrderId] = oo
				self.nextValidOrderId += 1
				order = None
				timeout = None
				if self.AfterHours():
					order = self._createLimitOrder('BUY', quantity, contract.askPrice)
					timeout = 240
				else:
					order = self._createMarketOrder('BUY', quantity)
					timeout = 120	 # Wait 2 minutes for a market buy
				self.placeOrder(self.nextValidOrderId, contract, order)
				if self._orderEvent.wait(timeout):
					self._orderEvent.clear()
					oo.status = 'success'
					oo.quantity = quantity
					oo.price = self.lastOrder.price
					oo.expiry = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d')
					retval = oo
				else:
					logger.error(f'Timed out trying to execute order {oo}, but did not cancel order.')
				# We've placed an order, get out of loop
				break
			else:
				oo.status = 'insufficient funds'
				oo.quantity = 0
				oo.price = contract.price
				logger.error(oo)
		else:
			oo = OptionOrder(symbol, right, 'BUY', None, None, allocation, quantity=0)
			oo.status = 'no options found'
			oo.price = None
			logger.error(oo)
		return retval

	def _roundTo(self, x, minIncrement):
		return int(minIncrement * trunc(float(x) / minIncrement))

	# Get a few options contracts ATM or ITM and closest expiry available
	# Return them in nearest strike and expiry order
	async def GetOptionContractsAtmClosest(self, symbol, right, close):
		contracts = []
		self._authenticated.wait()
		today = datetime.now()
		dayOfWeek = today.weekday()  # 0 = Monday, 6 = Sunday
		hourOfDay = today.hour  # What time zone is this in?  Local?
		# QQQ, SPY has options on days 0, 1, 2, 3, 4 (M, T, W, Th, F)
		# Other tickers usually day 4 (F)
		optionsSymbols = {
			'default': {'optionTick': 1, 'hasDailies': False, 'hasWeeklies': True},
			'QQQ': {'optionTick': 1, 'hasDailies': True, 'hasWeeklies': True},
			'SPY': {'optionTick': 1, 'hasDailies': True, 'hasWeeklies': True},
			'SPX': {'optionTick': 5, 'hasDailies': True, 'hasWeeklies': True},
			'XSP': {'optionTick': 1, 'hasDailies': True, 'hasWeeklies': True},
			'NANOS': {'optionTick': 1, 'hasDailies': True, 'hasWeeklies': True},
			'NDX': {'optionTick': 5, 'hasDailies': True, 'hasWeeklies': True},
			'XND': {'optionTick': 1, 'hasDailies': True, 'hasWeeklies': True}
		}
		targetDay = today
		optionsConfig = optionsSymbols[symbol] if symbol in optionsSymbols else optionsSymbols['default']
		if (optionsConfig['hasDailies']):
			# TODO Get time zone to be time zone of exchange for ticker, too much craziness with local/exchange times
			if hourOfDay >= 11:  # If after 1pm on 0DTE contracts, go to next contract just in case we have to hold to next day for algo
				if dayOfWeek == 4:
					targetDay = today + timedelta(days=3)
				elif dayOfWeek == 5:  # Saturday - go to monday
					targetDay = today + timedelta(days=2)
				elif dayOfWeek == 6:  # Sunday - go to monday
					targetDay = today + timedelta(days=1)
				else:
					targetDay = today + timedelta(days=1)
			else:
				targetDay = today
		elif (optionsConfig['hasWeeklies']):
			if dayOfWeek == 0:
				targetDay = today + timedelta(days=4)
			elif dayOfWeek == 1:
				targetDay = today + timedelta(days=3)
			elif dayOfWeek == 2:
				targetDay = today + timedelta(days=2)
			elif dayOfWeek == 3:
				targetDay = today + timedelta(days=1)
			elif dayOfWeek == 4:
				if hourOfDay > 10:  # If after 10am on Friday (0DTE), go to monday's contract
					targetDay = today + timedelta(days=3)
			elif dayOfWeek == 5:  # Saturday - go to monday
				targetDay = today + timedelta(days=2)
			elif dayOfWeek == 6:  # Sunday - go to monday
				targetDay = today + timedelta(days=1)
		else: # Monthlies only
			logger.debug('No weekly options avail, no trade')

		strikeIncrement = optionsConfig['optionTick']
		strike = self._roundTo(close, strikeIncrement)

		# TODO - pull a few strikes, find the first one (up or down) that doesn't have a crazy spread, and has some bid/ask sizes
		# sometimes we get raped on options that have a $1 or more spread.
		# Or maybe just switch to 1DTE sometime around 2pm - that's when the 0DTEs seem to dry up.
		# Go first strike OTM, not ATM, the ATM and better options have crazy spreads on SPX/XSP close to end of day.
		strike = strike + strikeIncrement if right == 'CALL' else strike - strikeIncrement
		atmContract = await self.GetCurrentOptionPrice(symbol, right, strike, targetDay)
		otmContract = await self.GetCurrentOptionPrice(symbol, right, strike + strikeIncrement if right == 'CALL' else strike - strikeIncrement, targetDay)
		contracts.append(atmContract)
		contracts.append(otmContract)
		return contracts

	async def GetCurrentOptionPrice(self, symbol, right, strike, expiry):
		contract = self._createOptionContract(symbol, right, strike, expiry)
		self.evTickPrice.clear()
		self.reqMktData(self.marketDataRequestId,  contract, '', True, False, '')
		reqId = f'{self.marketDataRequestId}'
		self.marketDataRequestId += 1
		waitingForTickInfo = True
		while waitingForTickInfo:
			if self.evTickPrice.wait(5):
				self.evTickPrice.clear()
				if reqId in self.tickInfo:
					tickInfo = self.tickInfo[reqId]
					contract.askPrice = tickInfo['ask'] if 'ask' in tickInfo else contract.askPrice if hasattr(contract, 'askPrice') else None
					contract.bidPrice = tickInfo['bid'] if 'bid' in tickInfo else contract.bidPrice if hasattr(contract, 'bidPrice') else None
					contract.lastPrice = tickInfo['last'] if 'last' in tickInfo else contract.lastPrice if hasattr(contract, 'lastPrice') else None
					if 'last' in tickInfo:
						contract.price = tickInfo['last']
						waitingForTickInfo = False
					elif 'ask' and 'bid' in tickInfo:
						contract.price = (tickInfo['ask'] + tickInfo['bid']) / 2
					elif 'ask' in tickInfo:
						contract.price = tickInfo['ask']
					elif 'bid' in tickInfo:
						contract.price = tickInfo['bid']
					else:
						# Got no useful price info, force 0 quantity by listing huge premium
						contract.price = 9999999999
			else:
				# Timeout - probably bad ticker ID
				waitingForTickInfo = False
		# TODO Grab min tick setting and do this rounding correctly
		contract.price = round(contract.price, 2) if (hasattr(contract, 'price') and contract.price > 0) else 9999999999.0
		return contract

	async def GetOptionContractDetails(self, symbol, right, strike, targetDay):
		contracts = []
		right = right.upper()
		contractAtm = self._createOptionContract(symbol, right, strike, targetDay)
		contractItm = self._createOptionContract(symbol, right, strike - 1, targetDay)

		self.reqContractDetails(self.nextValidOrderId, contractItm)
		self.evOptionContractDetails.wait()
		self.evOptionContractDetails.clear()
		if hasattr(self.optionContractDetails, 'ask'):
			contracts.append(self.optionContractDetails.ask)

		self.reqContractDetails(self.nextValidOrderId, contractAtm)
		self.evOptionContractDetails.wait()
		self.evOptionContractDetails.clear()
		if hasattr(self.optionContractDetails, 'ask'):
			contracts.append(self.optionContractDetails.ask)
		return contracts
