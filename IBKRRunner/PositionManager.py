import asyncio
import contextlib
from math import trunc
from platform import platform
from time import time
from uuid import uuid4
from xml.sax import default_parser_list
from xmlrpc.client import Boolean
from enum import Enum
import logging
from Order import Order
from TradePlatforms.KucoinTradeApi import KucoinTradeApi
from TradePlatforms.StockAPI import OrderStatus, Position, StockAPI, StockTrade
from Database import DataAccessLayer

log = logging.getLogger('POSM')
log.setLevel(logging.INFO)

class PositionState(Enum):
	Unknown = 0
	Short = 1
	Long = 2
	Neutral = 3
	WaitingForBuyToComplete = 4
	WaitingForSellToComplete = 5
	WaitingForCoverToComplete = 6
	WaitingForShortToComplete = 7
	WaitingForCancelToComplete = 8
	WaitingForCancelForUpdate = 9

# vscode-fold=#
# This guy is in charge of managing a Position = A unique symbol/algo/timeframe combination
'''
	# Trade Cycle example with $100 and 3x leverage
	#--------------------------------
	# TradeableCash = $100
	# Leverage = 3x

	# Buy at $10
	#-------------
	# OrderValue = TradeableCash * Leverage = $100 * 3 = $300
	# ShareCount = OrderValue / SharePrice = $300 / $10 = 30
	# MarginOwed = OrderValue - TradeableCash = $300 - $100 = $200
	# CashInAccount = $0
	# TradeableCash = $0

	# Sell at $11
	#-------------
	# OrderValue = ShareCount * SharePrice = 30 * 11 = $330
	# TradeableCash = OrderValue - MarginOwed = $330 - $200 = $130
	# MarginOwed = $0
	# ShareCount = 0
	# CashInAccount = $130

	# Short at $11
	#---------------
	# OrderValue = TradeableCash * Leverage = $130 * 3 = $390
	# ShareCount = - OrderValue / SharePrice = $390 / $11 = -35.4
	# CashInAccount = OrderValue + CashInAccount = $390 + $130 = $520
	# TradeableCash = $0

	# Cover at $10
	#---------------
	# OrderValue  = abs(ShareCount) * SharePrice = 35.45 * $10 = $354.50
	# TradeableCash = CashInAccount - OrderValue = $520 - $354.50 = $165.50
	# CashInAccount = TradeableCash = $165.50

	# So only the buy and sell involve the leverage calculations, the others are 'covering' either a long or a short position
	# Well, that was some work, but I feel so much better about it now NVS - 2022.9.5
'''

class PositionManager:
	# vscode-fold=#
	def __init__(self, userId, algoId, symbol, timeframe, tradePlatform: KucoinTradeApi):
		# Useful singletongs
		self.dal:DataAccessLayer = DataAccessLayer('a', 'b')

		# Immutable things
		self.uuid:str = uuid4().hex
		self.userId:str = userId
		self.algoId:str = algoId
		self.symbol:str = symbol
		self.timeframe:str = timeframe
		self.tradePlatform:KucoinTradeApi = tradePlatform

		# Current positions and prices from exchange/broker
		self.platformPrices :dict= {}
		self.platformPositions:dict = {}

		# The actual position and state of the state machine
		self.leverage:float = 1.0
		self._allocation:float = 0.0
		self._shareCount:float = 0.0
		self.tradeableCash:float = 0.0
		self.state:PositionState = PositionState.Unknown

		# Order watcher info
		self.retryCounter:int = 0
		self.secondCounter:int = 0
		self.resubmit: Boolean = True
		self.nextTimeMarket:Boolean = False
		self.tickAdjust:float = 0.0

	@classmethod
	async def withInit(cls, userId: str, algoPositionDefinition, tradePlatform: StockAPI, platformPositions: dict, platformPrices: dict):
		self = cls(userId, algoPositionDefinition.algoId, algoPositionDefinition.symbol, algoPositionDefinition.timeframe, tradePlatform)
		await self.Startup(algoPositionDefinition, platformPositions, platformPrices)
		return self

	async def Startup(self, algoPositionDefinition, platformPositions, platformPrices):
		log.oper(f'Starting up Algo PositionManager for: {self.symbol}:{self.algoId}:{self.timeframe}')
		self.SetPlatformInfo(algoPositionDefinition, platformPrices, platformPositions)
		self.symbolSettings = await self.dal.GetSymbolSettings(self.symbol)
		assert self.symbolSettings is not None
		self.dal.SubscribeToAlgoSignals(self.algoId, self.symbol, self.timeframe, self._onAlgoSignal)
		#await self.LoadPositionFromPlatform('startup')

	async def Shutdown(self):
		# TODO Close any open positions?
		self.dal.UnsubscribeFromAlgoSignals(self.algoId, self.symbol, self.timeframe)
		log.oper(f'Shutting down {self.symbol}:{self.algoId}:{self.timeframe}')

	def SetPlatformInfo(self, algoPositionDefinition, platformPrices, platformPositions):
		self.allocation = algoPositionDefinition.allocation
		self.leverage = algoPositionDefinition.leverage or 1.0
		self.platformPrices = platformPrices
		self.platformPositions = platformPositions

	# vscode-fold=#
	async def UpdateAlgoPositionDefinition(self, algoPositionDefinition, platformPositions, platformPrices, action):
		self.SetPlatformInfo(algoPositionDefinition, platformPrices, platformPositions)

		startupPosition = 'long'
		await self.LoadPositionFromPlatform(action)
		if action == 'startup':
			# ping the signal server to find out last signal given for this algo (long or short) and update
			newestAction, newestSignal = await self.dal.GetNewestAlgoSignal(self.algoId, self.symbol, self.timeframe)
			if newestAction and newestSignal:
				if newestAction == 'cover-and-buy':
					# Need to switch to LONG positions
					startupPosition = 'long'
				elif newestAction == 'sell-and-short':
					startupPosition = 'short'
				elif newestAction == 'startup':
					startupPosition = newestSignal
		else:
			startupPosition = None
			
		strippedSymbol = self.symbol.split('-')[0]
		symbolPrice = self.platformPrices[strippedSymbol]
		positionValue = symbolPrice * self.shareCount
		expectedPositionValue = (self.allocation * self.leverage) if startupPosition == 'long' else -(self.allocation * self.leverage)
		minTradeValue = self.symbolSettings.minTradeQuantity * symbolPrice
		valueDiff = expectedPositionValue - positionValue

		if abs(valueDiff) > minTradeValue:
			# Don't do anything if we are less than the minimum trade value away from target position
			shareCountDiff = abs((expectedPositionValue - positionValue) / symbolPrice)
			# We have less than $0 tradeable cash left after receiving updated allocation, means we are either too long or too short, need to correct
			log.oper(f'Reallocating {self.symbol}:{self.algoId}:{self.timeframe} from ${positionValue:.2f} to ${expectedPositionValue}')
			if expectedPositionValue > 0:
				# We are supposed to be long, let's see where we are at
				if self.state == PositionState.Neutral:
					await self.Buy(symbolPrice, 'reallocate') # We are neutral, buy up to our allocation
				elif self.state == PositionState.Short:
					# We are short but need to be long, let the code do it's normal thing
					await self.Cover(symbolPrice, 'reallocate')
				elif self.state == PositionState.Long:
					# We are already long, let's see if we need to adjust up or down
					if expectedPositionValue < positionValue:
						# Need to go less long
						await self.SellNoShort(symbolPrice, 'reallocate', shareCountDiff)
					else:
						# Need to go more long
						await self.Buy(symbolPrice, 'reallocate')
				else:
					log.error('Bad position state on update')
			elif expectedPositionValue < 0:
				# We are supposed to be short, let's see where we are at
				if self.state == PositionState.Neutral:
					# We are neutral, so short and let the logic figure out amount
					await self.Short(symbolPrice, 'reallocate')
				elif self.state == PositionState.Long:
					# We are long, so go through normal process to reverse position
					if self.shareCount < self.symbolSettings.minTradeQuantity:
						# Not enough shares to sell to get to neutral, so just go short right away
						await self.Short(symbolPrice, 'reallocate')
					else:
						# Sell first, then short.
						await self.Sell(symbolPrice, 'reallocate')
				elif self.state == PositionState.Short:
					# We are already short, let's see if we need to adjust up or down
					if expectedPositionValue < positionValue:
						# Need more short
						await self.Short(symbolPrice, 'reallocate', shareCountDiff)
					else:
						# Need to be less short
						await self.CoverNoBuy(symbolPrice, 'reallocate', shareCountDiff)
				else:
					log.error('Bad position state on update')
			else:
				# We are expected to be neutral
				await self.ClosePosition()
		else:
			log.oper('Reallocation too small, doing nothing')


	def _getStrippedSymbol(self, symbol):
		return  symbol.split('-')[0]

	# vscode-fold=#
	async def LoadPositionFromPlatform(self, action):
		strippedSymbol = self._getStrippedSymbol(self.symbol)
		position = self.platformPositions[strippedSymbol]
		self.lastPrice = self.platformPrices[strippedSymbol] if strippedSymbol in self.platformPrices else 0.0
		if position:
			positionQuantity = position.quantity
			self.shareCount = positionQuantity
			if positionQuantity > 0:
				# We are long, so tradeable cash should be somewhere close to zero
				self.tradeableCash = self.allocation - ((positionQuantity * self.lastPrice)/self.leverage)
				await self.ChangeState(PositionState.Long, action, None)
			elif positionQuantity < 0:
				# We are short, so tradeable cash should be negative and cash in account should be positive the value of what we sold
				self.tradeableCash = (self.allocation - ((abs(positionQuantity) * self.lastPrice)/self.leverage))
				await self.ChangeState(PositionState.Short, action, None)
			else:
				self.tradeableCash = self.allocation
				await self.ChangeState(PositionState.Neutral, action, None)
		else:
			self.tradeableCash = self.allocation
			self.shareCount = 0.0
			await self.ChangeState(PositionState.Neutral, action, None)

	@property
	def shareCount(self): return self._shareCount

	@shareCount.setter
	def shareCount(self, value): self._shareCount = value

	@property
	def allocation(self): return self._allocation

	@allocation.setter
	def allocation(self, value): self._allocation = value

	async def GetCurrentOptionPrices_Atm_Closest(self, side):
		prices = []
		symbol = self._getStrippedSymbol(self.symbol)
		prices = await self.tradePlatform.GetCurrentOptionPrices_Atm_Closest(symbol, side)
		return prices

	# vscode-fold=#
	async def GetCurrentPrice(self):
		platformPrice = None
		# How to do this for Options?
		if 'OPT' in self.symbol:
			platformPrices = await self.tradePlatform.GetCurrentOptionPrices(self.symbol)
		else:
			strippedSymbol = self._getStrippedSymbol(self.symbol)
			platformPrices = await self.tradePlatform.GetCurrentPrices([strippedSymbol])
			platformPrice = platformPrices[strippedSymbol]
		return platformPrice

	# vscode-fold=#
	async def ClosePosition(self):
		limit = await self.GetCurrentPrice()
		if self.state == PositionState.Long:
			await self.SellNoShort(limit, 'closing-position')
		elif self.state == PositionState.Short:
			await self.CoverNoBuy(limit, 'closing-position')

	async def GetCurrentValue(self):
		price = await self.GetCurrentPrice()
		return self.shareCount * price

	async def ChangeState(self, newState, action, orderStatus):
		self.state = newState
		if orderStatus:
			log.oper(f'{orderStatus.side.upper()} {orderStatus.quantity} of {self.symbol} at {orderStatus.price} {action}: Current position {self.symbol} is: [{self.shareCount}], Cash [{self.tradeableCash}], Position: [{self.state}]')
		await self.dal.LogPositionChange(self.algoId, self.symbol, self.timeframe, action, self.state.name, self.shareCount, self.lastPrice, self.tradeableCash, 0.0)

	async def LogTrade(self, orderStatus):
		self.tradeInProgress.price = orderStatus.price  # TODO get average fill price, this may just be last fill
		self.tradeInProgress.endTime = time()
		await self.dal.LogTrade(self.userId, self.algoId, self.tradeInProgress)

	async def _onBuyCompleted(self, orderStatus:OrderStatus):
		orderValue = (orderStatus.quantity * orderStatus.price)
		commission = orderValue * self.symbolSettings.commission
		self.tradeableCash = self.tradeableCash - orderValue/self.leverage - commission
		self.shareCount += orderStatus.quantity
		await self.ChangeState(PositionState.Long, 'buy-completed', orderStatus)
		await self.LogTrade(orderStatus)

	async def _onSellCompleted(self, orderStatus:OrderStatus):
		orderValue = (abs(orderStatus.quantity) * orderStatus.price)
		commission = orderValue * self.symbolSettings.commission
		self.tradeableCash = self.tradeableCash + orderValue/self.leverage - commission
		self.shareCount -= orderStatus.quantity
		await self.ChangeState(PositionState.Neutral, 'sell-completed', orderStatus)
		await self.LogTrade(orderStatus)
		if hasattr(self, 'nextOrderFn'):
			await self.nextOrderFn(orderStatus.price, 'sell', None)

	async def _onShortCompleted(self, orderStatus:OrderStatus):
		orderValue = abs(orderStatus.quantity) * orderStatus.price
		commission = orderValue * self.symbolSettings.commission
		self.shareCount -= orderStatus.quantity
		self.tradeableCash = self.tradeableCash - orderValue/self.leverage - commission
		await self.ChangeState(PositionState.Short, 'short-completed', orderStatus)
		await self.LogTrade(orderStatus) 

	async def _onCoverCompleted(self, orderStatus:OrderStatus):
		orderValue = (abs(orderStatus.quantity) * orderStatus.price)
		commission = orderValue * self.symbolSettings.commission
		self.tradeableCash = self.tradeableCash + orderValue/self.leverage - commission
		self.shareCount += orderStatus.quantity
		await self.ChangeState(PositionState.Neutral, 'cover-completed', orderStatus)
		await self.LogTrade(orderStatus)
		if hasattr(self, 'nextOrderFn'):
			await self.nextOrderFn(orderStatus.price, 'buy', None)

	# vscode-fold=#
	def _adjustPrice(self, price):
		decimals = 0
		minTickAdjust = self.symbolSettings.minTickAdjust if 'minTickAdjust' in self.symbolSettings else .01
		while minTickAdjust < 1:
			minTickAdjust *= 10
			decimals += 1
		price *= (10 ** decimals) if decimals > 0 else 1
		price = round(price, 0)
		price /= (10 ** decimals) if decimals > 0 else 1
		return price if decimals > 0 else int(price)

	# Sample response from DB watcher
	# {'_id': 'ESPT:AVAX-USDT:5m:1661659782', 'action': 'sell-and-short', 'price': 19.642, 'algoInstanceId': 'ebf9338c825e4fce8dd29786d0b2cd20	'}
	async def _onAlgoSignal(self, signal):
		action = signal['action']
		price = signal['price']
		log.oper(f'Position manager {self.uuid} received signal to {action} {self.symbol} at {price}')
		if action == 'sell-and-short' or action == 'sell':
			if self.shareCount > self.symbolSettings.minTradeQuantity:
				await self.Sell(price, action, None)
			else:
				await self.Short(price, action, None)
		elif action == 'cover-and-buy' or action == 'buy':
			if self.shareCount < -self.symbolSettings.minTradeQuantity:
				await self.Cover(price, action, None)
			else:
				await self.Buy(price, action)
		elif action == 'cancel-open':
			if self.openOrder:
				oo = self.openOrder
				status = await oo.Cancel()
				if status == 'filled':
					self.onOrderDoneFn(OrderStatus(self.symbol, oo.quantity, oo.side, oo.price, oo.clientOrderId, 'filled'))
				else:
					log.error(f'Could not complete cancel: {status}')
			else:
				log.error('Received cancel but order was already done')
		elif action == 'close':
			self.ClosePosition()
		else:
			log.error(f'Unknown algo action: {action}')

	# vscode-fold=#
	def _orderSideToPositionState(self, side):
		state = PositionState.Unknown
		if side == 'buy':
			state = PositionState.WaitingForBuyToComplete
		elif side == 'sell':
			state = PositionState.WaitingForSellToComplete
		elif side == 'short':
			state = PositionState.WaitingForShortToComplete
		elif side == 'cover':
			state = PositionState.WaitingForCoverToComplete
		return state

	async def Buy(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else (self.tradeableCash * self.leverage)/limit
		await self._submitOrder('buy', sc, limit, reason, None, self._onBuyCompleted)

	async def Cover(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else abs(self.shareCount)
		await self._submitOrder('buy', sc, limit, reason, self.Buy, self._onCoverCompleted)

	async def CoverNoBuy(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else abs(self.shareCount)
		await self._submitOrder('buy', sc, limit, reason, None, self._onCoverCompleted)

	async def SellNoShort(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else self.shareCount
		await self._submitOrder('sell', sc, limit, reason, None, self._onSellCompleted)

	async def Sell(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else self.shareCount
		await self._submitOrder('sell', sc, limit, reason, self.Short, self._onSellCompleted)

	async def Short(self, limit, reason, shareCount=None):
		sc = shareCount if shareCount else (self.tradeableCash * self.leverage)/limit
		await self._submitOrder('sell', sc, limit, reason, None, self._onShortCompleted) 
		
	async def _submitOrder(self, side, shareCount, limit, reason, fnNext, fnDone):
		self.nextOrderFn = fnNext; self.onOrderDoneFn = fnDone
		shareCount = round(shareCount, self.symbolSettings.maxQuantityDecimals)
		shareCount = shareCount if self.symbolSettings.fractional else trunc(shareCount)
		if shareCount > 0 and shareCount >= (self.symbolSettings.minTradeQuantity):
			while True:
				limit = self._adjustPrice(limit)
				self.tradeInProgress = StockTrade(self.symbol, side, shareCount, limit)
				self.openOrder = await Order.withState(self.userId, self.algoId, side, reason, self.symbol, shareCount, 'limit', limit, self.symbolSettings.minTickAdjust, self.tradePlatform)
				await self.ChangeState(self._orderSideToPositionState(side), reason, OrderStatus(self.symbol, shareCount, side, limit, self.openOrder.clientOrderId, 'requested'))
				try:
					orderStatus = await asyncio.wait_for(asyncio.shield(self.openOrder.Submit()), timeout=3)
				except asyncio.TimeoutError:
					await self.ChangeState(self._orderSideToPositionState(side), 'timeout-cancel', OrderStatus(self.symbol, shareCount, side, limit, self.openOrder.clientOrderId, 'requested'))
					orderStatus = await self.openOrder.Cancel()
					if orderStatus.status == 'filled' or orderStatus.status == 'done':
						# Order filled while we were trying to cancel, proceed as expected and then exit the retry while loop
						if fnDone:
							await fnDone(orderStatus)
						break
					elif orderStatus.status == 'cancelled':
						limit = await self.GetCurrentPrice()
						# Let it roll around to the While True loop to resumbit order
					elif orderStatus.status == 'partial':
						# Have to account for the quantity change for next go around
						shareCount -= orderStatus.partialFillQuantity
						shareCount = round(shareCount, self.symbolSettings.maxQuantityDecimals)
						shareCount = shareCount if self.symbolSettings.fractional else trunc(shareCount)
					else:
						log.error(f'Unexpected order state {orderStatus.status} in _submitOrder')
				else:
					# order completed somewhat normally and hence returned an order status
					if orderStatus.status == 'filled' or orderStatus.status == 'done':
						if fnDone:
							await fnDone(orderStatus)
						break
					else:
						log.error(f'Unexpected order state {orderStatus.status} in _submitOrder')
		else:
			log.error(f'Bad sharecount {shareCount} to {side} symbol {self.symbol}')
			await self.ChangeState(PositionState.Neutral, reason, OrderStatus(self.symbol, shareCount, side, limit, None, 'share-count-low'))
			# Still need to execute next steps in process even if this one failed, ie move to a buy from a cover or a short from a sell.
			# Make a fake order status to help the next function calculate cost of a nothing trade
			os = OrderStatus(self.symbol, 0.0, side, limit, 'DEADBEEF', 'share-count-low')
			self.tradeInProgress = StockTrade(self.symbol, side, 0.0, 0.0)
			if fnDone:
				await fnDone(os)