import enum as enum
import os
from symtable import Symbol
import time
from uuid import uuid4

class Interval(enum.auto):
	MINUTE = 1
	FIVEMINUTE = 2
	FIFTEENMINUTE = 3
	HOUR = 4
	DAY = 5
	MONTH = 6

class TradeAction(enum.auto):
	BUY = 1
	SELL = 2
	STOPUP = 3
	STOPDN = 4



class Trade:
	def __init__(self, symbol:str, type:str, quantity:float, price:float) -> None:
		self.symbol = symbol
		self.type = type
		self.quantity = quantity
		self.price = price
		self._id = uuid4().hex
		self.startTime = time.time()
		self.endTime = None # set this before logging completed trade

class OrderStatus:
	def __init__(self, symbol:str, count:float, side:str, price:float, orderId:str, status=None):
		self.ver = '1.0'
		self.orderId = orderId
		self.symbol = symbol
		self.quantity = count
		self.side = side
		self.price = price
		self.status = status

class CandleData:
	def __init__(self, open: float, high: float, low: float, close: float, volume: float, ts: int, dt=None):
		self.ver = '1.0'
		self.high = high
		self.low = low
		self.open = open
		self.close = close
		self.volume = volume
		self.timestampSeconds = ts
		self.dt = dt

class Position:
	def __init__(self, symbol, quantity, pricePaid, costBasis):
		self.ver = '1.0'
		self.symbol = symbol
		self.quantity = quantity
		self.pricePaid = pricePaid
		self.costBasis = costBasis

class StockAPI:
	def __init__(self, objName):
		self.type = objName

		self.i = 0

	def GetCurrentPositions(self):
		pass

	def GetCurrentPosition(self, symbol):
		pass

	def BuyMarket(self, symbol, count):
		pass

	def SellMarket(self, symbol, count):
		pass

	def BuyLimit(self, symbol, count, price):
		pass

	def SellLimit(self, symbol, count, price):
		pass

	def GetStockMostRecentCandleData(self, symbol, interval):
		pass

	def CheckOpenOrders(self):
		# What to do?
		# Order has been open for more than 30s?
		# Cancel and resubmit at slightly higher/lower price
		# Order still fails to settle?
		self.i += 10


	def PlaceOrder(self, oo):
		pass