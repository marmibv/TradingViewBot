import logging
import asyncio
from motor import motor_asyncio as AsyncMongoClient
from munch import DefaultMunch as munch

logger = logging.getLogger('DAL')

class DataAccessLayer(object):
	_dbClient = None
	_pygmalionDb = None
	algoSettingsCallbacks = {}
	algoSignalCallbacks = {}
	Timeframes = {'30s': 0.5, '1m': 1, '2m': 2, '3m': 3, '4m': 4, '5m': 5, '6m': 6, '7m': 7, '8m': 8, '9m': 9, '10m': 10, '15m': 15, '30m': 30, '45m': 45, '1h': 60, '2h': 120, '3h': 180, '4h': 240, '1d': 60*24, '1w': 60*24*7}

	def __new__(cls):
		if not hasattr(cls, 'instance'):
			cls.instance = super(DataAccessLayer, cls).__new__(cls)
		return cls.instance

	def __init__(self) -> None:
		# Provide the mongodb atlas url to connect python to mongodb using pymongo
		CONNECTION_STRING = "mongodb://localhost:27017"

		# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
		self._dbClient = AsyncMongoClient.AsyncIOMotorClient(CONNECTION_STRING)
		self._pygmalionDb = self._dbClient['AlgoBot']

	def GetAsyncCoroutines(self):
		coroutines = []
		coroutines.append(asyncio.ensure_future(self.WatchAlgoSettingsCollection()))
		coroutines.append(asyncio.ensure_future(self.WatchAlgoInstancesCollection()))
		return coroutines

	'''
	This structure looks like
	{
		'ESPT:AVAX-USDT:5m': <callback fn>
	}
	'''
	def SubscribeToAlgoSettingsChanges(self, algoId, symbol, timeframe, callback):
		lookup = f'{algoId}:{symbol}:{timeframe}'
		self.algoSettingsCallbacks[lookup] = callback

	def UnsubscribeFromAlgoSettingsChanges(self, algoId, symbol, timeframe):
		lookup = f'{algoId}:{symbol}:{timeframe}'
		self.algoSettingsCallbacks.pop(lookup, None)

	# Just an event, up to the receiver to pull the updated list from DB
	def SubscribeToAlgoInstancesChanged(self, callback):
		self.activeAlgosCallback = callback

	def UnsubscribeFromAlgoInstancesChanged(self):
		del self.activeAlgosCallback

	'''
	Typical update:
	{
		'_id': {'_data': '82630ACF3E000000022B022C0100296E5A100494FD514486F444CA90CC09ADCBE48EE4463C5F6964003C456D61536C6F706550657263656E745472656E643A5553442D4A5059000004'},
		'operationType': 'update',
		'clusterTime': Timestamp(1661652798,2),
		'ns': {'db': 'Pygmalion', 'coll': 'AlgoSettings'},
		'documentKey': {'_id': 'EmaSlopePercentTrend:USD-JPY'},
		'updateDescription': {	'updatedFields': {'sess2Len': 46},'removedFields': [], 'truncatedArrays': []}
	}
	'''
	async def WatchAlgoSettingsCollection(self):
		while True:
			try:
				async with self.GetAlgoSettingsCollection().watch() as change_stream:
					async for change in change_stream:
						docKey = change['documentKey']['_id']
						if docKey in self.algoSettingsCallbacks:
							await self.algoSettingsCallbacks[docKey]()
			except asyncio.exceptions.CancelledError:
				# keep propogating this exception
				raise
			except Exception as e: 
				logger.error(f'AlgoSettings Watcher threw exception {e}')

	async def WatchAlgoInstancesCollection(self):
		while True:
			try:
				async with self.GetAlgoInstancesCollection().watch() as change_stream:
					async for change in change_stream:
						if self.activeAlgosCallback:
							await self.activeAlgosCallback()
			except asyncio.exceptions.CancelledError:
				# keep propogating this exception
				raise
			except Exception as e:  # TODO there will be an exception that should cancel this while true, sigterm or something that indicates app is shutting down
				logger.error(f'AlgoInstances Watcher threw exception {e}')
				continue

	def GetAlgoInstancesCollection(self):
		return self._pygmalionDb['AlgoInstances']

	def GetDataProvidersCollection(self):
		return self._pygmalionDb['DataProviders']

	def GetAlgoSignalsCollection(self):
		return self._pygmalionDb['Signals']

	def GetAlgoStateCollection(self):
		return self._pygmalionDb['AlgoInstance_State']

	def GetAlgoSettingsCollection(self):
		return self._pygmalionDb['AlgoSettings']

	def GetSymbol_DataProvidersCollection(self):
		return self._pygmalionDb['Symbol_DataProviders']

	async def GetAlgoInstances(self):
		algos = {}
		async for row in self.GetAlgoInstancesCollection().find({}):
			id = row['_id']
			del row['_id']
			algos[id] = row
		return algos

	async def GetSymbol_DataProviders(self):
		symbolSettings = {}
		async for row in self.GetSymbol_DataProvidersCollection().find({}):
			symbolSettings[row['_id']] = row
		return symbolSettings

	async def GetDataProviderCredentials(self, providerId):
		credentials = []
		async for row in self.GetDataProvidersCollection().find({'_id': providerId}):
			credentials.append(row)
		return credentials[0] if len(credentials) > 0 else None

	async def GetSymbolSettings(self, symbol):
		symbolSettings = []
		symbolSettingsColl = self.GetSymbolSettingsCollection()
		async for row in symbolSettingsColl.find({'symbol': symbol}):
			r = row
			r.pop('_id')
			r1 = munch.fromDict(r)
			symbolSettings.append(r1)
		return symbolSettings[0] if len(symbolSettings) > 0 else None

	async def GetDataProviders(self):
		dataProviders = {}
		async for row in self.GetDataProvidersCollection().find({}):
			dataProviders[row['_id']] = row
		return dataProviders

	async def GetAlgoSettings(self, algoId, symbol, timeframe):
		algoSettingsColl = self.GetAlgoSettingsCollection()
		generalSettings = []
		docKey = f'{algoId}:{symbol}:{timeframe}'
		async for row in algoSettingsColl.find({'_id':docKey}):
			generalSettings.append(row)
		settings = generalSettings[0] if len(generalSettings) > 0 else None
		return settings

	# self.dal.LogAlgoSignal(self.algoId, action, price, self.timestampSeconds, self.algoInstanceId)
	async def LogAlgoState(self, algoId, symbol, timeframe, timestampSeconds, algoInstanceId, state):
		docKey = f'{algoId}:{symbol}:{timeframe}:{timestampSeconds}'
		r = {'_id': docKey, 'state': state, 'algoId': algoId, 'symbol': symbol, 'timeframe': timeframe, 'tsSeconds': timestampSeconds, 'algoInstanceId': algoInstanceId}
		await self.GetAlgoStateCollection().insert_one(r)

	# self.dal.LogAlgoSignal(self.algoId, action, price, self.timestampSeconds, self.algoInstanceId)
	async def LogAlgoSignal(self, algoId, symbol, timeframe, action, signal, price, tier, timestampSeconds, algoInstanceId):
		docKey = f'{algoId}:{symbol}:{timeframe}:{timestampSeconds}'
		r = {'_id':docKey, 'action':action, 'signal': signal, 'price':price, 'tier': tier, 'algoId': algoId, 'symbol': symbol, 'timeframe': timeframe, 'tsSeconds': timestampSeconds, 'algoInstanceId':algoInstanceId}
		await self.GetAlgoSignalsCollection().insert_one(r)