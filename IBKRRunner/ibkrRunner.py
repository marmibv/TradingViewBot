from motor import motor_asyncio as AsyncMongoClient
import json
from ib_insync import *
import asyncio
import config, logging, sys, math
from datetime import datetime, timedelta
from IBKRApi import IBKRTradeApi
from StockAPI import OptionOrder


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

# connect to Interactive Brokers
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

dbClient = AsyncMongoClient.AsyncIOMotorClient("mongodb://localhost:27017")
algoBotDb = dbClient['AlgoBot']
signalsDoc = algoBotDb['TVSignals']
tradeDb = dbClient['TradeBot-niall']
ordersTbl = tradeDb['OrderAudit']
positionsTbl = tradeDb['Positions']
positionsAuditTbl = tradeDb['PositionsAudit']
tradesTbl = tradeDb['Trades']

ibkrApi:IBKRTradeApi = None

# Sample data returned from trade signal
# {
# 	"phrase": "CrispyBlueDuck",
# 	"algo": "Bouncy",
# 	"ticker": "QQQ",
# 	"timeframe": "1m",
# 	"type": "option",
# 	"right": "call",
# 	"side": "buy",
# 	"strike": 272,
# 	"close": 272.3,
# 	"time": 123121111123,
# 	"reason": "Long Wick"
# }
async def onTradeSignal(signal):
      # What are we doing?
	symbol = signal['symbol']
	right = signal['right']
	side = signal['side']
	close = signal['close']

	await ordersTbl.insert_one(optionOrder.__dict__)
	if side == 'buy':
		optionOrder = OptionOrder(symbol, right, side, datetime.now(), math.trunc(close), config.allocation)
		order = await ibkrApi.BuyMarketOptionOrder(optionOrder, close)
		await positionsTbl.insert_one(optionOrder.__dict__)
		await positionsAuditTbl.insert_one(optionOrder.__dict__)

	elif side == 'sell':
		position = positionsTbl.find_one({'symbol': symbol, 'right': right, 'side': 'buy'})
		if position:
			status = await ibkrApi.SellMarketOptionOrder(position['symbol'], position['right'], position['strike'], position['exipiry'], position['quantity'])
			oo = OptionOrder(symbol, right, side, position['expiry'], position['strike'], 0)
			oo. quantity = position['quantity']
			await positionsAuditTbl.insert_one(oo.__dict__)
			await positionsTbl.delete_one({'symbol': symbol, 'right': right, 'side': 'buy'})

async def WatchAlgoSignalsCollection():
	while True:
		try:
			async with signalsDoc.watch() as change_stream:
				async for change in change_stream:
					if 'fullDocument' in change:  # Only send updates on new rows being inserted
						await onTradeSignal(change['fullDocument'])
		except Exception as e:
			log.error(f'WatchAlgoSignalsCollection threw exception {e}')

def spinning_cursor():
	while True:
		for cursor in '|/-\\':
			yield cursor

spinner = spinning_cursor()

async def main():
	global ibkrApi
	log.oper('Starting up TVBotRunner')
	waitables = [asyncio.ensure_future(WatchAlgoSignalsCollection())]
	ibkrApi = await IBKRTradeApi.withInit()
	log.oper('ENTERING MAIN LOOP')
	while waitables:
		try:
			sys.stdout.write('\b')
			await asyncio.wait(waitables, timeout=1.0)
			sys.stdout.write(next(spinner))
			sys.stdout.flush()
		except Exception as e:
			log.error(f'Something bad happened: {e}')

if __name__ == "__main__":
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
