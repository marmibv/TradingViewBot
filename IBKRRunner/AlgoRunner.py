# What are we doing here?
# Run long time, listen to database for additions/deletions of symbol/platform/timeframe combinations
# Which database?  Pygmalion.AlgoInstances

from multiprocessing.connection import wait
import sys
import asyncio
from Common.AsyncTimers import OneSecondTimer, OneSecondTimer
from Common.DataAccessLayer import DataAccessLayer

import logging

logging.basicConfig(filename="algorunner.log", filemode='w', level=logging.ERROR)
logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

# Add an operation level event to root logger
logging.addLevelName(100, 'OPER')
def logOperationEvent(self, msg):
	self.log(100, msg)
logging.Logger.oper = logOperationEvent

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

lroot = logging.getLogger()
lroot.setLevel(logging.ERROR)
lroot.addHandler(handler)

log = logging.getLogger('MAIN')
log.setLevel(logging.DEBUG)

algoInstances = {}
runningAlgos = {}

algoClassLookup = {
	'ESPT': 'EmaSlopePercentTrend',
	'ESPP': 'EmaSlopePercentPercent'
}

class Runner():
	def __init__(self) -> None:
		self.dataAccessLayer = DataAccessLayer()
		self.oneMinuteTimer = OneSecondTimer()
		self.oneSecondTimer = OneSecondTimer()
		self.dataProviderInstances = {}
		self.algoInstances = {}
		
	@staticmethod
	def GetRequiredDataProviders(algoInstances, symbolDataProviders):
		# Go through algo definitions, find out which data providers we need based on symbol settings and instantiate all of them
		requiredDataProviders = {}
		for algoInstance in algoInstances.values():
			if algoInstance['active']:
				symbol = algoInstance['symbol']
				symSettings = symbolDataProviders[symbol]  # TODO What if not defined?
				symbolProviders = symSettings['dataProviders']
				for provider in symbolProviders:
					if provider not in requiredDataProviders:
						requiredDataProviders[provider] = provider
		return requiredDataProviders

	@staticmethod
	async def StartupDataProvider(dataProviderName, dataProviders):
		dataProviderClassName = dataProviders[dataProviderName]['className']
		dataProviderModule = __import__('Common.DataProviders.' + dataProviderClassName)
		classObj = getattr(getattr(dataProviderModule.DataProviders, dataProviderClassName), dataProviderClassName)
		dataProviderInstance = classObj()
		await dataProviderInstance.Startup()
		return dataProviderInstance

	async def StartupAlgo(self, algoDefinition, symbolDataProviders):
		algoClass = algoClassLookup[algoDefinition['algoId']]
		algoModule = __import__('Algos.' + algoClass)
		classObj = getattr(getattr(algoModule, algoClass), algoClass)
		dpName = list(symbolDataProviders[algoDefinition['symbol']]['dataProviders'].keys())[0]
		providerInstance = self.dataProviderInstances[dpName]
		algoInstance = classObj(algoDefinition['symbol'], algoDefinition['timeframe'], providerInstance)
		await algoInstance.Startup()
		return algoInstance

	async def Startup(self):
		# What does startup/update look like?
		# Load active trade algos from DB
		# Start up appropriate data providers
		# Instantiate and start appropriate algos with appropriate data providers
		# Listen to ActiveAlgos for changes and add/remove algos as necessary
		# Re-authenticate with data providers regularly
		self.dataAccessLayer.SubscribeToAlgoInstancesChanged(self.onAlgoInstancesChanged)
		await self._onAlgoInstancesChanged(True)
		return self.waitables

	async def onAlgoInstancesChanged(self):
		await self._onAlgoInstancesChanged(False)

	async def _onAlgoInstancesChanged(self, isStartup):
		# Have to turn off timers during update otherwise we can get corruption in the subscriber lists
		self.oneMinuteTimer.Pause()
		self.oneSecondTimer.Pause()
		self.waitables = self.dataAccessLayer.GetAsyncCoroutines()
		dataProviders = await self.dataAccessLayer.GetDataProviders()
		symbolDataProviders = await self.dataAccessLayer.GetSymbol_DataProviders()
		updatedAlgoInstanceDefinitions = await self.dataAccessLayer.GetAlgoInstances()
		updatedDataProviders = self.GetRequiredDataProviders(updatedAlgoInstanceDefinitions, symbolDataProviders)

		if len(updatedAlgoInstanceDefinitions) <= 0:
			log.error('No Algo Instances defined')
		else:
			providersToAdd = updatedDataProviders.keys() - self.dataProviderInstances.keys()
			providersToRemove = self.dataProviderInstances.keys() - updatedDataProviders.keys()
			for providerName in providersToAdd:
				dataProviderInstance = await self.StartupDataProvider(providerName, dataProviders)
				self.dataProviderInstances[providerName] = dataProviderInstance
				self.waitables += dataProviderInstance.GetAsyncCoroutines()

			for instanceName in providersToRemove:
				instObj = self.dataProviderInstances.pop(instanceName, None)
				if instObj:
					await instanceName.Shutdown()

			# Algos require an up to date instance list of providers, so that has to be done before this
			# Compare against the reference list, creating a list of additions and deletions
			# Shut down the old algos and start up the new ones
			algosToAdd = updatedAlgoInstanceDefinitions.keys() - self.algoInstances.keys()
			algosToRemove = self.algoInstances.keys() - updatedAlgoInstanceDefinitions.keys()
			for instanceName in algosToAdd:
				algoInstanceObj = await self.StartupAlgo(updatedAlgoInstanceDefinitions[instanceName], symbolDataProviders)
				self.algoInstances[instanceName] = algoInstanceObj

			for instanceName in algosToRemove:
				instObj = self.algoInstances.pop(instanceName, None)
				if instObj:
					await instObj.Shutdown()

			self.waitables.append(self.oneMinuteTimer.GetAsyncCoroutine())
			self.waitables.append(self.oneSecondTimer.GetAsyncCoroutine())

			# Now that we have a new set of algos, trigger the top level runner to update the async task list
			# as long as this isn't just the startup routine
			if not isStartup:
				raise asyncio.exceptions.CancelledError('Updated algo instances')

	def GetWaitables(self):
		return self.waitables

def spinning_cursor():
	while True:
		for cursor in '|/-\\':
			yield cursor

spinner = spinning_cursor()

async def main():
	log.oper('Starting up AlgoRunner')
	runner = Runner()
	waitables = await runner.Startup()
	log.oper('MAIN LOOP')
	while waitables:
		try:
			sys.stdout.write('\b')
			await asyncio.wait(waitables, timeout=1.0)
			sys.stdout.write(next(spinner))
			sys.stdout.flush()
			waitables = runner.GetWaitables()
		except Exception as e:
			log.exception(f'Bad happened here: {e}')
		
if __name__ == "__main__":
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
