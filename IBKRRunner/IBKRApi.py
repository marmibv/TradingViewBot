from datetime import datetime, timedelta
import asyncio
from math import trunc
from random import random
from StockAPI import StockAPI
from StockAPI import OrderStatus, OptionOrder, Position, TradePlatformApi
import pytz
import contextlib
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import *
from ibapi.order_state import *
from ibapi.order_condition import *
from ibapi.common import *
from ibapi.tag_value import TagValue
import threading
import logging

log = logging.getLogger('MAIN')

indexes = ['SPX', 'NDX', 'XSP', 'XND']

class IBKRTradeApi(StockAPI, TradePlatformApi, EWrapper, EClient):
	def __init__(self, isLive):
		EClient.__init__(self, self)
		self.subscribers = {}
		self.orders = {}
		self.ibkrOrders = {}
		self.url = f"127.0.0.1"
		self.port = 7496 if isLive else 7497
		self._authenticated = asyncio.Event()
		self._orderEvent = asyncio.Event()
		self._errorEvent = asyncio.Event()
		self.evOptionContractDetails = asyncio.Event()
		self.evTickPrice = asyncio.Event()
		self.task = None
		self.marketDataRequestId = 1
		self.tickInfo = {}
		self.errors = []
		# Tick Type definitions: 1 - Bid Price,  2 - Ask Price, 4 - Last Price,  6 - High of day, 7 - Low of day,  9 - Close of last day
		self.tickIdLookup = {
			'1': 'bid',
			'2': 'ask',
			'4': 'last',
			'6': 'hod',
			'7': 'lod',
			'9': 'close'
		}

	def GetAsyncCoroutine(self):
		return self.task

	@classmethod
	async def withInit(cls, isLive):
		self = cls(isLive)
		return self if  await self.Startup() else None

	async def Startup(self):
		self.disconnect()
		connected = await self.Connect()
		if not connected:
			log.error('Aborting IBKRApi startup, no TWS or IBGateway connection made')
		return connected

	async def Connect(self):
		connected = False
		log.oper('Connecting to IBKR streaming...')
		self.connect(self.url, self.port, int(random()*10000))
		if len(self.errors) <= 0:
			self.task = threading.Thread(target=self.run, daemon=True)
			self.task.start()
			if await self.event_wait(self._authenticated, 10):
				log.oper('Connected to IBKR streaming.')
				connected = True
			else:
				log.error('FAILED TO AUTHENTICATE WITH TWS')
		else:
			log.error(f'FAILED TO CONNECT TO TWS on port {self.port}')
		return connected

	async def event_wait(self, evt, timeout):
		# suppress TimeoutError because we'll return False in case of timeout
		with contextlib.suppress(asyncio.TimeoutError):
			await asyncio.wait_for(evt.wait(), timeout)
		return evt.is_set()
	
	def error(self, id, errCode, errMsg, orderReject = None):
		log.error(f'IBAPI Error: {id}, {errCode}, {errMsg}, {orderReject}')
		error = {'id': id, 'errCode': errCode, 'errMsg': errMsg}
		self.errors.append(error)
		self._errorEvent.set()
		# HACK - should just trigger error event and have people that care wait on either error or order events
		self._orderEvent.set()

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
		log.error(f"NextValidId: {orderId}")
		# we can start now
		self._authenticated.set()

	# Callback to receive a (partial) list of open orders
	def openOrder(self, orderId: OrderId, contract: Contract, order: Order, orderState: OrderState):
		super().openOrder(orderId, contract, order, orderState)
		log.debug(f"OpenOrder. OrderId: {orderId}, Symbol: {contract.symbol}")
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
		log.debug(f'OrderStatus - id: {orderId}, status: {status}, filled qty: {filled}, remain qty: {remaining}, avg fill price: {avgFillPrice}')
		if orderId in self.orders:
			ibkrOrder = self.orders[orderId]
			if ibkrOrder.contract:
				if ibkrOrder.contract.secType == 'OPT':
					parts = ibkrOrder.contract.localSymbol.split(' ')
					symbol = parts[0] + '-' + parts[1]
				else:
					symbol = ibkrOrder.contract.localSymbol[:3] + '-' + ibkrOrder.contract.localSymbol[4:]
				status = 'filled' if remaining == 0 else 'partial'
				self.lastOrder = OrderStatus(symbol, float(filled), ibkrOrder.action, float(avgFillPrice), orderId, status=status)
				self._orderEvent.set()
				if symbol in self.subscribers:
					subscriber = self.subscribers[symbol]
					subscriber(self.lastOrder)
			else:
				log.error('Received order status without contract info')
		# TODO else conditions?
		# TODO Handle cancel order, failed order, all the things that can go wrong

	def tickPrice(self, reqId: TickerId, tickId: int, price: float, attrib: TickAttrib):
		# For most things we care only about the ask
		requestId = f'{reqId}'
		tickId = f'{tickId}'
		if tickId in self.tickIdLookup:
			if requestId not in self.tickInfo:
				self.tickInfo[requestId] = {}
			req = self.tickInfo[requestId]
			tickType = self.tickIdLookup[tickId]
			req[tickType] = price
			tickInfo = {
				'reqId': reqId,
				'tickType': tickId,
				'price': price,
				'attrib': attrib
			}
			log.debug(tickInfo)
			# The ticks come in order, any of these indicate that we have enough information to proceed
			if tickType == 'last' or tickType == 'close' or tickType == 'hod' or tickType == 'lod':
				self.evTickPrice.set()
		return super().tickPrice(reqId, tickId, price, attrib)

	def position(self, account, contract: Contract, pos, avgCost):
		pos = Position(contract.tradingClass, pos, avgCost, avgCost)
		self.positions.append(pos)
		log.debug(pos)

	def contractDetails(self, reqId: int, contractDetails):
		self.optionContractDetails = contractDetails
		self.evOptionContractDetails.set()

	async def SubscribeToTrades(self):
		await self.event_wait(self._authenticated, None)
		log.debug(f'Subscribed to IBKR trades - NO-OP')
		# Nothing to do, IBKR api is automatically subscribed to trade updates via EWrapper/EClient

	async def UnsubscribeFromTrades(self):
		await self.event_wait(self._authenticated, None)
		log.debug(f'Unsubscribed from IBKR trades - NO-OP')
		# Nothing to do

	async def _authenticate(self):
		log.debug('Authenticating to IBKR local endpoint - NO-OP')
		# Nothing to do here, IBKR authentication is handled external to this application, through the IB Gateway or the TWS client
		# Local connection to the API gateway running on local machine is by whitelisted IP, no authentication required.
		if await self.event_wait(self._authenticated, 30):
			log.debug('Authenticated to IBKR streaming.')
		else:
			log.error('Failed authentication with TWS or IBGateway')

	def SubscribeTickerOrders(self, ticker, subscriber):
		self.subscribers[ticker] = subscriber

	def UnsubscribeTickerOrders(self, ticker):
		del self.subscribers[ticker]

	async def ReSubscribe(self):
		await self.SubscribeToTrades()

	def GetTradeableStocks(self):
		assert('IBKR-GetTradeableStocks Not supported')
		return None
		
	def GetShortableStocks(self):
		assert('IBKR-GetShortableStocks Not supported')
		return None

	def GetOpenOrders(self):
		self.reqAllOpenOrders()
		# TODO
		# Data will be returned in callbacks openOrder and orderStatus, need to make this an async
		return None

	async def GetCurrentPositions(self):
		await self.event_wait(self._authenticated, None)
		self.positions = []
		try:
			self.reqPositions()
		except Exception:
			log.exception("Failed to get current positions")
		return self.positions

	async def GetCurrentPrice(self, symbol):
		await self.event_wait(self._authenticated, None)
		contract = Contract()
		contract.symbol = symbol
		contract.secType = 'IND' if symbol in indexes else 'STK'
		contract.exchange = 'CBOE' if symbol in indexes else 'SMART'
		contract.currency = "USD"
		self.reqMktData(self.marketDataRequestId,  contract, '', True, False, '')
		reqId = f'{self.marketDataRequestId}'
		self.marketDataRequestId += 1
		waitingForTickInfo = True
		while waitingForTickInfo:
			if await self.event_wait(self.evTickPrice, 5):
				self.evTickPrice.clear()
				if reqId in self.tickInfo:
					tickInfo = self.tickInfo[reqId]
					contract.askPrice = tickInfo['ask'] if 'ask' in tickInfo else contract.askPrice if hasattr(contract, 'askPrice') else None
					contract.bidPrice = tickInfo['bid'] if 'bid' in tickInfo else contract.bidPrice if hasattr(contract, 'bidPrice') else None
					contract.lastPrice = tickInfo['last'] if 'last' in tickInfo else contract.lastPrice if hasattr(contract, 'lastPrice') else None
					contract.closePrice = tickInfo['close'] if 'close' in tickInfo else contract.closePrice if hasattr(contract, 'closePrice') else None
					# The best to have is the 'last' price, if we get that, we're done and we can return
					# If we timeout without receiving the 'last', take 'ask' + 'bid' / 2 if we have both 'ask' and 'bid'
					# else 
					if 'last' in tickInfo:
						contract.price = contract.lastPrice
						waitingForTickInfo = False
					elif 'close' in tickInfo:
						contract.price = contract.closePrice
						waitingForTickInfo = False
					elif 'ask' and 'bid' in tickInfo:
						contract.price = (contract.askPrice + contract.bidPrice) / 2
					elif 'ask' in tickInfo:
						contract.price = contract.askPrice
					elif 'bid' in tickInfo:
						contract.price = contract.bidPrice
					else:
						# Got no useful price info, force 0 quantity by listing huge premium
						contract.price = 9999999999
			else:
				# Timeout - probably bad ticker ID
				waitingForTickInfo = False
		return contract.price if hasattr(contract, 'price') else None

	async def GetCurrentPositionFor(self, symbol):
		await self.event_wait(self._authenticated, None)
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
		order.lmtPriceOffset = 0
		order.outsideRth = True
		return order

	def _createMarketOrder(self, action, qty, afterHours=False):
		# Create order object
		order = Order()
		order.action = action.upper()
		order.totalQuantity = qty
		order.orderType = 'MKT'
		order.eTradeOnly = False
		order.firmQuoteOnly = False
		if not afterHours:
			order.algoStrategy = 'Adaptive'
			order.algoParams = []
			order.algoParams.append(TagValue('adaptivePriority', 'Normal'))
		return order

	def _createTrailStopOrder(self, action, qty, trailPerc, afterHours=False):
		order = Order()
		order.action = action.upper()
		order.orderType = 'TRAIL'
		order.totalQuantity = qty
		order.trailingPercent = trailPerc
		order.eTradeOnly = False
		if not afterHours:
			order.algoStrategy = 'Adaptive'
			order.algoParams = []
			order.algoParams.append(TagValue('adaptivePriority', 'Normal'))
		return order

	def _createStopLossOrder(self, action, qty, price, afterHours=False):
		order = Order()
		order.action = action.upper()
		order.orderType = 'STP'
		order.auxPrice = price
		order.totalQuantity = qty
		order.eTradeOnly = False
		if not afterHours:
			order.algoStrategy = 'Adaptive'
			order.algoParams = []
			order.algoParams.append(TagValue('adaptivePriority', 'Normal'))
		return order

	def _createLimitOrder(self, action, qty, limit, afterHours = False):
		# Create order object
		order = Order()
		order.action = action.upper()
		order.totalQuantity = qty
		order.orderType = 'LMT'
		order.lmtPrice = limit
		order.lmtPriceOffset = 0
		order.eTradeOnly = False
		order.firmQuoteOnly = False
		if not afterHours:
			order.algoStrategy = 'Adaptive'
			order.algoParams = []
			order.algoParams.append(TagValue('adaptivePriority', 'Normal'))
		order.outsideRth = afterHours
		return order

	async def PlaceStockOrder(self, oo):
		await self.event_wait(self._authenticated, None)
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
			# TODO Get this from min tick definition on the ticker or underlying
			limit = round((contract.bidPrice + contract.askPrice) / 2, 2)
			order = self._createLimitOrder('sell', quantity, limit, afterHours=True)
		else:
			order = self._createMarketOrder('sell', quantity)
		oo = OptionOrder(symbol, right, 'sell', expiry, strike, 0)
		oo.quantity = quantity
		self.orders[self.nextValidOrderId] = oo
		self.nextValidOrderId += 1
		self.placeOrder(self.nextValidOrderId, mktContract, order)
		if await self.event_wait(self._orderEvent, None):
			self._orderEvent.clear()
			self._errorEvent.clear()
			if hasattr(self, 'lastOrder'):
				oo.price = self.lastOrder.price
				oo.status = 'success'
				retval = oo
			else:
				log.error('Error ocurred while waiting for order to complete')
				retval = None
		else:
			log.error(f'Timed out when placing order: {oo.__dict__}')
		return retval

	def AfterHours(self):
		now = datetime.now(pytz.timezone('America/New_York'))
		afterHours =  now.hour > 16 or now.hour < 9 or (now.hour == 9 and now.minute >= 30)
		return afterHours

	async def BuyAtmOptionsAtMarket(self, symbol, right, allocation, stop=None, trailStopPerc=None):
		await self.event_wait(self._authenticated, None)
		retval = None
		log.oper(f'Fetching market price for {symbol}')
		close = await self.GetCurrentPrice(symbol)
		if close:
			log.oper(f'Looking for suitable contracts to buy {symbol}-{right} near {close}')
			contracts = await self.GetOptionContractsAtmClosest(symbol, right, close)
			for contract in contracts:
				log.oper(f'Contract found: {symbol}-{right}-{contract.lastTradeDateOrContractMonth}-{contract.strike} at {contract.price}')
				quantity = trunc(allocation / (contract.price * 100))
				oo = OptionOrder(symbol, right, 'BUY', contract.lastTradeDateOrContractMonth, contract.strike, allocation, quantity=quantity)
				if quantity > 0:
					self.orders[self.nextValidOrderId] = oo
					self.nextValidOrderId += 1
					order = None
					timeout = None
					if self.AfterHours():
						order = self._createLimitOrder('BUY', quantity, contract.askPrice, afterHours=True)
						timeout = 240
					else:
						order = self._createMarketOrder('BUY', quantity)
						timeout = 120	 # Wait 2 minutes for a market buy
					# TODO Log order
					self.placeOrder(self.nextValidOrderId, contract, order)
					# If a stop is specified, enter a stop market order
					if stop:
						# Place stoploss at market order with adaptive
						stopOrder = self._createStopLossOrder('SELL', quantity, contract.price * (1-stop))
						self.placeOrder(self.nextValidOrderId, contract, stopOrder)
					if trailStopPerc:
						trailOrder = self._createTrailStopOrder('SELL', quantity, trailStopPerc)
						self.placeOrder(self.nextValidOrderId, contract, trailOrder)
					if await self.event_wait(self._orderEvent, None):
						self._orderEvent.clear()
						self._errorEvent.clear()
						if hasattr(self, 'lastOrder'):
							oo.status = 'SUCCESS'
							oo.quantity = quantity
							oo.price = self.lastOrder.price
							oo.expiry = datetime.strptime(contract.lastTradeDateOrContractMonth, '%Y%m%d')
							retval = oo
						else:
							log.erro('Error submitting order')
					else:
						log.error(f'Timed out trying to execute order {oo}, but did not cancel order.')
					# We've placed an order, get out of loop
					break
				else:
					retval = oo
					oo.price = 99999999
					oo.status = 'INSUFFICIENT FUNDS'
					log.error(f'Insufficient funds to purchase {symbol}-{right}-{oo.expiry}-{oo.strike} for ${contract.price}')
			if len(contracts) <= 0:
				retval = oo
				oo.price = 99999999
				oo.status = 'NO OPTIONS FOUND'
				log.error(f'No contracts found for {symbol}-{right}-{oo.expiry}-{oo.strike}')
		else:
			log.error(f'Could not submit order, did not receive current market price for {symbol}')
		return retval

	def _roundTo(self, x, minIncrement):
		return int(minIncrement * trunc(float(x) / minIncrement))

	# Get a few options contracts ATM or ITM and closest expiry available
	# Return them in nearest strike and expiry order
	async def GetOptionContractsAtmClosest(self, symbol, right, close):
		await self.event_wait(self._authenticated, None)
		contracts = []
		today = datetime.now(pytz.timezone('America/New_York'))
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
			# Time is in timezone of exchange, NYSE usually
			if hourOfDay > 13:  # If after 1pm on 0DTE contracts, go to next contract just in case we have to hold to next day for algo
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
				if hourOfDay > 13:  # If after 1pm on Friday (0DTE), go to monday's contract
					targetDay = today + timedelta(days=3)
			elif dayOfWeek == 5:  # Saturday - go to monday
				targetDay = today + timedelta(days=2)
			elif dayOfWeek == 6:  # Sunday - go to monday
				targetDay = today + timedelta(days=1)
		else: # Monthlies only
			log.debug('No weekly options avail, no trade')

		strikeIncrement = optionsConfig['optionTick']
		strike = self._roundTo(close, strikeIncrement)

		# TODO - pull a few strikes, find the first one (up or down) that doesn't have a crazy spread, and has some bid/ask sizes
		# sometimes we get raped on options that have a $1 or more spread.
		# Or maybe just switch to 1DTE sometime around 2pm - that's when the 0DTEs seem to dry up.
  
		# If XSP go first strike OTM, not ATM, the ATM and better options have crazy spreads on XSP close to end of day.
		# First ATM put will be the result of the rounding, no need to subtract anything, calls need to go up one strikeIncrement
		if symbol == 'XSP':
			strike = strike + strikeIncrement if right == 'CALL' else strike - strikeIncrement
		else:
			strike = strike + strikeIncrement if right == 'CALL' else strike
   
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
			if await self.event_wait(self.evTickPrice, 10):
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
		contract.price = contract.price if (hasattr(contract, 'price') and contract.price > 0) else 999999
		return contract

	async def GetOptionContractDetails(self, symbol, right, strike, targetDay):
		contracts = []
		right = right.upper()
		contractAtm = self._createOptionContract(symbol, right, strike, targetDay)
		contractItm = self._createOptionContract(symbol, right, strike - 1, targetDay)

		self.reqContractDetails(self.nextValidOrderId, contractItm)
		await self.event_wait(self.evOptionContractDetails, 10)
		self.evOptionContractDetails.clear()
		if self.optionContractDetails and hasattr(self.optionContractDetails, 'ask'):
			contracts.append(self.optionContractDetails.ask)

		self.reqContractDetails(self.nextValidOrderId, contractAtm)
		await self.event_wait(self.evOptionContractDetails, 10)
		self.evOptionContractDetails.clear()
		if self.optionContractDetails and hasattr(self.optionContractDetails, 'ask'):
			contracts.append(self.optionContractDetails.ask)
		return contracts
