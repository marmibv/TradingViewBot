import logging
from time import time
import asyncio
from motor import motor_asyncio as AsyncMongoClient
from Exceptions import TradeBotRunnerRestartException
from TradePlatforms.StockAPI import StockTrade as Trade
from munch import DefaultMunch as munch

logger = logging.getLogger('DALA')

class DataAccessLayer():

	def __new__(cls, userId, algoSignalsCollection):
		if not hasattr(cls, 'instance'):
			cls.instance = super(DataAccessLayer, cls).__new__(cls)
			cls.instance.userId = userId
			cls.instance.algoSignalCallbacks = {}
			cls.instance.algoSignalsCollection = algoSignalsCollection
			cls.instance.tradedSymbolCallback = None
			cls.instance.tradePlatformsCallback = None
			CONNECTION_STRING = "mongodb://localhost:27017"
			cls.instance.dbClient = AsyncMongoClient.AsyncIOMotorClient(CONNECTION_STRING)
			cls.instance.algoBotDb = cls.instance.dbClient['AlgoBot']
			cls.instance.tradeBotDb = cls.instance.dbClient[f'TradeBot-{userId}']
			cls.instance.sharedTradeBotDb = cls.instance.dbClient['TradeBot-General']
			cls.instance.coroutines = []
			cls.instance.coroutines.append(asyncio.ensure_future(cls.instance.WatchAlgoSignalsCollection()))
			cls.instance.coroutines.append(asyncio.ensure_future(cls.instance.WatchTradedSymbolsCollection()))
			cls.instance.coroutines.append(asyncio.ensure_future(cls.instance.WatchTradePlatformsCollection()))
		return cls.instance

	def __init__(self, userId, algoSignalsCollection) -> None:
		self.userId = self.userId
		self.algoSignalsCollection = self.algoSignalsCollection
		self.dbClient = self.dbClient
		self.algoBotDb = self.algoBotDb
		self.tradeBotDb = self.tradeBotDb
		self.sharedTradeBotDb = self.sharedTradeBotDb
		self.coroutines = self.coroutines
		self.algoSignalCallbacks = self.algoSignalCallbacks
		self.algoSignalsCollection = self.algoSignalsCollection
		self.tradedSymbolCallback = self.tradedSymbolCallback
		self.tradePlatformsCallback = self.tradePlatformsCallback

	def GetAsyncCoroutines(self):
		return self.coroutines

	async def Shutdown(self):
		for coro in self.coroutines:
			coro.cancel()
			
	async def TruncateAlgoCollection(self, col):
		try:
			self.algoBotDb.drop_collection(col)
		except:
			logger.error('Collection doesnt exist')

	async def TruncateTradeCollection(self, col):
		try:
			self.tradeBotDb[col].drop_collection(col)
		except:
			logger.error('Collection doesnt exist')

	'''
	This structure looks like
	{
		'ESPT:AVAX-USDT:5m': <callback fn>
	}
	'''
	def makeAlgoSignalKey(self, algoId, symbol, timeframe):
		return f'{algoId}:{symbol}:{timeframe}'
		
	def UnsubscribeFromAlgoSignals(self, algoId, symbol, timeframe):
		lookup = self.makeAlgoSignalKey(algoId, symbol, timeframe)
		self.algoSignalCallbacks.pop(lookup, None)

	def SubscribeToAlgoSignals(self, algoId, symbol, timeframe, callback):
		lookup = self.makeAlgoSignalKey(algoId, symbol, timeframe)
		self.algoSignalCallbacks[lookup] = callback

	def SetAlgoSignalsCollection(self, coll):
		self.algoSignalsCollection = coll
		logger.oper(f'!!!IMPORTANT!!! Setting signals connection: [{self.algoSignalsCollection}]')

	def GetAlgoSignalsCollection(self):
		logger.oper(f'!!!IMPORTANT!!! Reading signals connection: [{self.algoSignalsCollection}]')
		return self.algoBotDb[self.algoSignalsCollection]

	async def WatchAlgoSignalsCollection(self):
		while True:
			try:
				async with self.GetAlgoSignalsCollection().watch() as change_stream:
					async for change in change_stream:
						docKey = change['documentKey']['_id']
						keys = docKey.split(':')
						lookup = f'{keys[0]}:{keys[1]}:{keys[2]}'
						if lookup in self.algoSignalCallbacks:
							if 'fullDocument' in change: # Only send updates on new rows being inserted
								await self.algoSignalCallbacks[lookup](change['fullDocument'])
			except Exception as e:
				logger.error(f'WatchAlgoSignalsCollection threw exception {e}')

	# Returns cover-and-buy or sell-and-short
	async def GetNewestAlgoSignal(self, algoId, symbol, timeframe):
		action = None
		async for row in self.GetAlgoSignalsCollection().find({'symbol': symbol, 'algoId': algoId, 'timeframe': timeframe}).limit(1).sort([('$natural', -1)]):
			rowm = munch.fromDict(row)
			action = rowm.action
			signal = rowm.signal
		return action, signal
	
	async def LogAlgoSignal(self, algoId, symbol, timeframe, action, signal, price, tier, ts):
		id = self.makeAlgoSignalKey(algoId, symbol, timeframe)
		row = {'_id': id, 'action': action, 'signal': signal, 'price': price, 'tier': tier, 'timestampSeconds': ts, 'algoId': algoId, 'symbol': symbol, 'timeframe': timeframe}
		await self.GetAlgoSignalsCollection().insert_one(row)

	def GetPositionAuditCollection(self): return self.tradeBotDb['PositionAudit']

	def GetTradesCollection(self): return self.tradeBotDb['Trades']

	def GetOrderAuditCollection(self): return self.tradeBotDb['OrderAudit']

	# Symbol settings are shared among all users
	def GetSymbolSettingsCollection(self): return self.sharedTradeBotDb['SymbolSettings']

	def GetTradedSymbolsCollection(self): return self.tradeBotDb['TradedSymbols']

	# These have user specifc auth and usage data, they belong in silo
	def GetTradePlatformsCollection(self): return self.tradeBotDb['TradePlatforms']

	def SubscribeToTradedSymbolsChanges(self, callback):
		self.tradedSymbolCallback = callback

	async def WatchTradedSymbolsCollection(self):
		while True:
			try:
				async with self.GetTradedSymbolsCollection().watch() as change_stream:
					async for change in change_stream:
						await self.tradedSymbolCallback(change)
			except Exception as e:
				logger.error(f'WatchTradedSymbolsCollection threw exception {e}')

	async def GetTradePlatforms(self):
		platforms = {}
		tps = await self.GetTradePlatformsCollection().find({}).to_list(length=100)
		for tp in tps:
			platforms[tp['_id']] = munch.fromDict(tp)
		return platforms

	def SubscribeToTradePlatformsChanges(self, callback):
		self.tradePlatformsCallback = callback

	async def WatchTradePlatformsCollection(self):
		while True:
			try:
				async with self.GetTradePlatformsCollection().watch() as change_stream:
					async for change in change_stream:
						await self.tradePlatformsCallback(change)
			except Exception as e:
				logger.error(f'WatchTradePlatformsCollection threw exception {e}')

	async def GetLastN(self, coll, criteria, N):
		results = []
		docs = await coll.count_documents({})
		if docs > 0:
			resp = coll.find(criteria).skip(docs - N)
			async for row in resp:
				results.append(row)
		return results

	async def GetPositionsFor(self, userId, algoId, symbol):
		positions = []
		resp = await self.GetLastN(self.GetPositionAuditCollection(), {'userId': userId, 'symbol': symbol, 'algo': algoId}, 1)
		for row in resp:
			positions.append(row)
		return positions

	async def GetTrades(self):
		return munch.fromDict(await self.GetTradesCollection.find({}).to_list(length=100))

	async def GetTradedSymbols(self):
		tradedSymbols = {}
		symbolDefs = await self.GetTradedSymbolsCollection().find({}).to_list(length=100)
		for symbolDef in symbolDefs:
			tradedSymbols[symbolDef['_id']] = munch.fromDict(symbolDef)
		return tradedSymbols

	async def GetSymbolSettings(self, symbol):
		settings = munch.fromDict(await self.GetSymbolSettingsCollection().find_one({'_id': symbol}))
		minTickAdjust = settings.minTickAdjust
		decimals = 0
		while minTickAdjust < 1:
			minTickAdjust *= 10
			decimals += 1
		settings.maxPriceDecimals = decimals

		minQtyAdjust = settings.minTradeQuantity
		decimals = 0
		while minQtyAdjust < 1:
			minQtyAdjust *= 10
			decimals += 1
		settings.maxQuantityDecimals = decimals

		return settings

	# await self.dal.LogOrderStatus(self.kind, self.side, self.sideType, self.quantity, self.symbol, self.price, self.state, self.userId, self.algo, self.clientOrderId)
	async def LogOrderStatus(self, kind, side, sideType, quantity, symbol, price, state, fillQuantity, fillPrice, userId, algoId, clientOrderId):
		r = {'state': state, 'kind': kind, 'side': side, 'quantity': quantity, 'symbol': symbol, 'price': price, 'fillQuantity': fillQuantity, 'fillPrice': fillPrice, 'sideType': sideType, 'userId': userId, 'algoId': algoId, '_id': f'{clientOrderId}:{time()}'}
		await self.GetOrderAuditCollection().insert_one(r)

	async def LogTrade(self, userId: str, algo: str, tradeInfo: Trade):
		tradeColl = self.GetTradesCollection()
		tradeInfo.userId = userId
		tradeInfo.algo = algo
		data = tradeInfo.__dict__
		await tradeColl.insert_one(data)

	async def LogPositionChange(self, algoId, symbol, timeframe, action, state, shareCount, lastPrice, cashValue, positionValue):
		posColl = self.GetPositionAuditCollection()
		r = {'_id': f'{symbol}:{algoId}:{timeframe}:{time()}', 'action': action, 'state': state, 'shareCount': shareCount, 'lastPrice': lastPrice, 'cashValue': cashValue, 'positionValue': positionValue}
		await posColl.insert_one(r)