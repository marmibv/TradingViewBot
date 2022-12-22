from AsyncTimers import OneMinuteTimer, OneSecondTimer
from Database import DataAccessLayer
from Exceptions import TradeBotRunnerRestartException
from TradePlatforms.KucoinTradeApi import KucoinTradeApi
from PositionManager import PositionManager
import config
from config import Mode
import logging
import sys
import asyncio
from logging.handlers import TimedRotatingFileHandler

logging.basicConfig(filename="botmain.log", filemode='w', level=logging.ERROR)
logging.basicConfig(stream=sys.stdout, level=logging.ERROR)
logging.addLevelName(100, 'OPER')
lroot = logging.getLogger()
lroot.setLevel(logging.ERROR)

handlerStdout = logging.StreamHandler(sys.stdout)
handlerFile = TimedRotatingFileHandler('tradebotrunner.log', when="d", interval=1, backupCount=30)
handlerStdout.setLevel(logging.DEBUG)
handlerFile.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(asctime)s - %(levelname)s - %(message)s', '%m-%d %H:%M:%S')
handlerStdout.setFormatter(formatter)
handlerFile.setFormatter(formatter)
lroot.addHandler(handlerStdout)
lroot.addHandler(handlerFile)

log = logging.getLogger('TDBM')
log.setLevel(logging.DEBUG)

# Add an operation level event to root logger
logging.addLevelName(100, 'OPER')
def logOperationEvent(self, msg):
	self.log(100, msg)
logging.Logger.oper = logOperationEvent

niallUuid = 'niall' #'c223ce7719c946608737755c0813de4a'

alpacaLiveUrl = 'api.alpaca.markets'
alpacaLiveKey = 'AKV1BWTH21VE6W383HDG'
alpacaLiveSecret = 'M97ocTfgkot1eGNrIsG2T3mvk06xTKWyU1LvJ2dk'

alpacaPaperUrl = 'paper-api.alpaca.markets'
alpacaPaperKey = 'PKIC9LB4IQ7IRNMLTUBO'
alpacaPaperSecret = 'SOa2GTNE3IVzUoi3Ms329TCku5pRB6zO8D5sh0NQ'

krakenLiveKey = '8to/a6ncgbyf5YEqEUC0ATLMde/LaYvgxBkSFH23kuMdKSqobRpMbjSJ'
krakenLiveSecret = 'UGRp2Mt1WcILss9ScCPFNxk+mcaRZnHaDKU3s0bVPRNEAVq6p2gCGq/cr/ByzIoT9+3XQvdX+MhzwYFvYnhzQg=='
krakenKeyDesc = 'api-key-1660155904150'

kucoinSandboxPassphrase = 'DontTreadOnMe'
kucoinSandboxName = 'PygmalionSandbox'
kucoinSandboxKey = '62f59c3d37a609000198c5ad'
kucoinSandboxSecret = 'd1d3f83c-d8fd-40c0-918b-40a5d775e61a'
kucoinSandboxUrl = 'https://openapi-sandbox.kucoin.com'

kucoinLivePassphrase = 'DontTreadOnMe'
kucoinLiveName = 'Pygmalion'
kucoinLiveKey = '62f5ce168265e70001e229e5'
kucoinLiveSecret = 'b95ea404-95df-4810-ba54-7bb28570aa71'
kucoinLiveUrl = 'https://api.kucoin.com'

# No IBKR credentials - it relies on connections from localhost being safe and secure

polygonKey = 'MMpBq9kayv2wcHoDAQkDosIl6m3IY8gXArN2D_'

config.applicationMode = Mode.REAL
config.tradeMode = Mode.PAPER

class TradeBotRunner():
	def __init__(self, userId) -> None:
		# Set up singletons before anyone else gets a chance to instantiate them outside the main loop
		self.oneSecondTimer = OneSecondTimer()
		self.oneMinuteTimer = OneMinuteTimer()
		self.dal = DataAccessLayer(userId, 'Signals')
		self.allSymbolList = {}

		self.positionManagers = {}
		self.tradePlatforms = {}

		self.waitables = []
		self.userId = userId

	@classmethod
	async def withInit(cls, userId) -> None:
		self = cls(userId)
		await self.Initialize()
		return self

	async def Initialize(self):
		self.dal.SetAlgoSignalsCollection(config.signalSource)
		self.dal.SubscribeToTradedSymbolsChanges(self._onTradedSymbolsChanged)
		self.dal.SubscribeToTradePlatformsChanges(self._onTradePlatformsChanged)
		await self._onTradedSymbolsChanged(True)
		return self.waitables

	def GetWaitables(self):
		return self.waitables

	def SetAlgoSignalsCollection(self, signal):
		self.dal.SetAlgoSignalsCollection(signal)

	async def _onTradedSymbolsChanged(self, isStartup):
		self.oneMinuteTimer.Pause()
		self.oneSecondTimer.Pause()
		self.waitables = self.dal.GetAsyncCoroutines()

		# What to do?
		# Get new list of traded symbols
		newTradedSymbols = await self.LoadTradedSymbols()
		tradePlatformDefs = await self.LoadTradePlatforms()

		if len(newTradedSymbols) <= 0:
			log.oper('NO TRADED SYMBOLS DEFINED, nothing to do ATM. Wumpus is lonely.')

		#From that list of symbols, establish the new list of trade platforms
		newRequiredPlatforms = {}
		for algoDef in newTradedSymbols.values():
			newRequiredPlatforms[algoDef.tradePlatform] = algoDef.tradePlatform

		# Add/Remove/Update the trading platforms
		# Now establish the deltas of trade platforms and add/remove/udpate
		platformsToAdd = newRequiredPlatforms.keys() - self.tradePlatforms.keys()
		platformsToRemove = self.tradePlatforms.keys() - newRequiredPlatforms.keys()
		platformsToUpdate = newRequiredPlatforms.keys() & self.tradePlatforms.keys()
		for platformName in platformsToAdd:
			tradePlatformInstance = await self.StartupTradePlatform(tradePlatformDefs[platformName])
			self.tradePlatforms[platformName] = tradePlatformInstance
			self.waitables.append(tradePlatformInstance.GetAsyncCoroutine())

		for platformName in platformsToRemove:
			tradePlatformInstance = self.tradePlatforms.pop(platformName)
			await tradePlatformInstance.Shutdown()
			del tradePlatformInstance

		for platformName in platformsToUpdate:
			tradePlatformInstance = self.tradePlatforms[platformName]
			await tradePlatformInstance.UpdateSettings(tradePlatformDefs[platformName])
			self.waitables.append(tradePlatformInstance.GetAsyncCoroutine())

		# Now that we have the platforms all taken care of, let's do the position managers for each symbol/timeframe/algo
		# we are trading.
		# Get set of trading platforms necessary for those symbols
		# Add/Remove/Update the position managers for the symbols
		symbolsToAdd = newTradedSymbols.keys() - self.positionManagers.keys()
		symbolsToRemove = self.positionManagers.keys() - newTradedSymbols.keys()
		symbolsToUpdate = newTradedSymbols.keys() & self.positionManagers.keys()

		# Reset self.platformPositions and self.platformPrices
		await self.LoadPlatformPositions(newTradedSymbols)

		# For each new symbol, create a position manager and give it the correct tradePlatform
		for algoDef in symbolsToAdd:
			symbolDef = newTradedSymbols[algoDef]
			tradePlatformName = symbolDef.tradePlatform
			positionManager = await PositionManager.withInit(
                            userId=self.userId,
				    algoPositionDefinition = symbolDef,
                            tradePlatform=self.tradePlatforms[tradePlatformName],
                            platformPositions=self.platformPositions[tradePlatformName],
                            platformPrices=self.platformPrices[tradePlatformName])
			self.positionManagers[algoDef] = positionManager
			#await positionManager.LoadPositionFromPlatform()
			await positionManager.UpdateAlgoPositionDefinition(newTradedSymbols[algoDef], self.platformPositions[tradePlatformName], self.platformPrices[tradePlatformName], 'startup')

		for algoDef in symbolsToRemove:
			oldPositionManager = self.positionManagers.pop(algoDef)
			await oldPositionManager.Shutdown()
			del oldPositionManager

		for algoDef in symbolsToUpdate:
			symbolDef = newTradedSymbols[algoDef]
			tradePlatformName = symbolDef.tradePlatform
			# TODO This ends up submitting trades and doing all sorts of work, would be nice to be able to defer this to after execution loop has begun.
			await self.positionManagers[algoDef].UpdateAlgoPositionDefinition(newTradedSymbols[algoDef], self.platformPositions[tradePlatformName], self.platformPrices[tradePlatformName], 'update')

		# Re-enable the timers and get their coroutines back in the list
		self.waitables.append(self.oneSecondTimer.GetAsyncCoroutine())
		self.waitables.append(self.oneMinuteTimer.GetAsyncCoroutine())

	async def _onTradePlatformsChanged(self):
		# TODO - propogate change to affected trade platforms
		pass

	@staticmethod
	async def StartupTradePlatform(platformDef):
		log.oper(f'Starting up trade platform: {platformDef._id}')
		tradePlatformClassName = platformDef.className
		tradePlatformModule = __import__('TradePlatforms.' + tradePlatformClassName)
		classObj = getattr(getattr(tradePlatformModule, tradePlatformClassName), tradePlatformClassName)
		tradePlatformInstance = classObj(platformDef.key, platformDef.secret, platformDef.apiPassPhrase, platformDef.restApiUrl, platformDef.wsApiUrl, True)
		await tradePlatformInstance.Startup()
		return tradePlatformInstance

	async def LoadPlatformPositions(self, tradedSymbols):
		self.platformPositions = {}
		self.platformPrices = {}
		for symbolDef in tradedSymbols:
			tradePlatformName = tradedSymbols[symbolDef].tradePlatform
			if tradePlatformName not in self.platformPositions:
				self.platformPositions[tradePlatformName] = await self.tradePlatforms[tradePlatformName].GetCurrentPositions()
				self.platformPrices[tradePlatformName] = await self.tradePlatforms[tradePlatformName].GetCurrentPrices(self.allSymbolList.keys())

	async def LoadTradePlatforms(self):
		tradePlatformDefs = await self.dal.GetTradePlatforms()
		return tradePlatformDefs

	async def LoadTradedSymbols(self):
		self.allSymbolList = {}
		tradedSymbols = await self.dal.GetTradedSymbols()
		for symbolDef in tradedSymbols:
			self.allSymbolList[tradedSymbols[symbolDef].symbol.split('-')[0]] = tradedSymbols[symbolDef].symbol
		return tradedSymbols

def spinning_cursor():
	while True:
		for cursor in '|/-\\':
			yield cursor

spinner=spinning_cursor()

async def main():
	log.oper('Starting up TradeBotRunner')
	runner = await TradeBotRunner.withInit(niallUuid)
	waitables = runner.GetWaitables()
	log.oper('ENTERING MAIN LOOP')
	while waitables:
		try:
			sys.stdout.write('\b')
			await asyncio.wait(waitables, timeout=1.0)
			sys.stdout.write(next(spinner))
			sys.stdout.flush()
			waitables = runner.GetWaitables()
		except Exception as e:
			log.error(f'Something bad happened: {e}')
		
if __name__ == "__main__":
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
