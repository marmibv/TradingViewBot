from motor import motor_asyncio as AsyncMongoClient
import asyncio
import  logging, sys
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


dbClient = AsyncMongoClient.AsyncIOMotorClient("mongodb://localhost:27017")
algoBotDb = dbClient['AlgoBot']
signalsDoc = algoBotDb['TVSignals']
tradeDb = dbClient['TradeBot']
ordersTbl = tradeDb['OrderAudit']
positionsTbl = tradeDb['Positions']
positionsAuditTbl = tradeDb['PositionsAudit']
tradesTbl = tradeDb['Trades']
tradedSymbolsTbl = tradeDb['TradedSymbols']

ibkrApi:IBKRTradeApi = None

def MakePositionIdFromOption(algoId, symbol, timeframe, type, right, strike, expiry):
	optionTicker = f'{algoId}:{symbol}:{timeframe}:{type}:{right}:{strike}:{expiry}'
	return optionTicker

def onOrderUpdate(os):
	# OrderStatus(symbol, float(filled), ibkrOrder.action, float(avgFillPrice), status='filled')
	tradesTbl
	
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
	algoId = signal['algoId'].upper()
	timeframe = f'{signal["timeframe"]}m'
	type = signal['type'].upper()
	right = signal['right'].upper()
	side = signal['side'].upper()

	tradedSymbolDef = await tradedSymbolsTbl.find_one({'symbol': symbol, 'algoId': algoId, 'timeframe': timeframe, 'type': type})
	if (tradedSymbolDef):
		allocation = tradedSymbolDef['allocation']
		if side == 'BUY':
			log.oper(f'Attempting to buy {symbol} {right} options ATM, using allocation of ${allocation}')
			# BuyMarket... is blocking but async, we have full info after it returns
			order = await ibkrApi.BuyAtmOptionsAtMarket(symbol, right, allocation, stop=15)
			if order and order.status == 'SUCCESS':
				# Order has completed, let's record the order sepcifics from IBKR, then update our cash and positions
				oo = OptionOrder(symbol, right, side, order.expiry, order.strike, allocation, order.quantity)
				await ordersTbl.insert_one(oo.__dict__)

				# Record the position change to the audit log
				positionId = MakePositionIdFromOption(algoId, symbol, timeframe, type, right, order.strike, order.expiry)
				positionFields = {'algoId': algoId, 'timeframe': timeframe, 'symbol': symbol, 'type': type, 'right': right, 'strike': order.strike, 'expiry': order.expiry, 'quantity': order.quantity}
				auditPositionFields = positionFields.copy()
				auditPositionFields['id'] = positionId
				await positionsAuditTbl.insert_one(auditPositionFields)
	
				# Record the new or updated position
				positionFields['_id'] = positionId
				existingPosition = await positionsTbl.find_one({'_id': positionId})
				if existingPosition:
					newQuantity = existingPosition['quantity'] + order.quantity
					await positionsTbl.update_one({'_id': positionId}, {'$set': {'quantity': newQuantity, 'expiry': order.expiry, 'strike': order.strike}})
				else:
					await positionsTbl.insert_one(positionFields)
	
				# Record the current cash allocation for this algo instance
				# Update allocation available if in pdt exempt mode, else leave allocation changes to the user
				if 'pdtExempt' in tradedSymbolDef and tradedSymbolDef['pdtExempt']:
					allocation -= (order.quantity * order.price * 100)
					await tradedSymbolsTbl.update_one({'_id': tradedSymbolDef['_id']}, {'$set': {'allocation': allocation}})
			else:
				log.error(f'Failed to execute buy order for {symbol}:{right} with status: {order.status if order else "no order returned"}')

		elif side == 'SELL':
			# Sell all positions for this symbol, algo, timegrame, type and right (if option)
			# There may have been different strikes/expiries opened and perhaps not closed due to order issues, clean them all up since
			# we're getting a signal to sell.
			anyMatchingPositions = False
			log.oper(f'Attempting to sell {symbol} {right} options')
			async for position in positionsTbl.find({'symbol': symbol, 'algoId': algoId, 'timeframe': timeframe, 'type': type, 'right': right}):
				anyMatchingPositions = True
				if position and 'symbol' in position:
					symbol = position['symbol']; algoId = position['algoId']; timeframe = position["timeframe"]; type = position['type']
					right = position['right']; strike = position['strike']; expiry = position['expiry']; quantity = position['quantity']
					if (quantity > 0):
						log.oper(f'Found {symbol}-{right}-{expiry}-{strike} with quantity {quantity} to sell.')
						# SellMarket is blocking but async, so we will have all order info on return
						order = await ibkrApi.SellOptionsAtMarket(symbol, right, strike, expiry, quantity)
						if order and order.status == 'SUCCESS':
							# Order has completed, let's log the order, then update our cash and positions
							optionOrder = OptionOrder(symbol, right, side, order.expiry, order.strike, allocation, quantity=order.quantity)
							await ordersTbl.insert_one(optionOrder.__dict__)
		
							# Log the position change to the audit table
							positionId = MakePositionIdFromOption(algoId, symbol, timeframe, type, right, strike, expiry)
							positionFields = {'algoId': algoId, 'timeframe': timeframe, 'symbol': symbol, 'type': type, 'right': right, 'strike': order.strike, 'expiry': order.expiry, 'quantity': order.quantity}
							auditPositionFields = positionFields.copy()
							auditPositionFields['id'] = positionId
							positionsAuditTbl.insert_one(auditPositionFields)
		
							# Update quantity for this position definition - should probably just delete it, there will end up being a huge
							# number of zero positions in a short amount of time, nice to have it for a while though, so maybe purge after a day or two
							await positionsTbl.update_one({'_id': positionId}, {'$set': {'quantity': 0}})
		
							# Update allocation available if in pdt exempt mode, else leave allocation changes to the user
							if 'pdtExempt' in tradedSymbolDef and tradedSymbolDef['pdtExempt']:
								cashProceeds = order.quantity * order.price * 100
								await tradedSymbolsTbl.update_one({'_id': tradedSymbolDef['_id']}, {'$set': {'allocation': round(allocation + cashProceeds, 2)}})
						else:
							log.error(f'No response for execute sell order for {symbol}-{right}-{expiry}-{strike}, probably still open.')
					else:
						log.error(f'Found {symbol}-{right}-{expiry}-{strike} with zero quantity.  Nothing to sell.')
				else:
					log.error(f'Unable to find position for {symbol}-{algoId}-{timeframe}-{type}-{right}')
			if not anyMatchingPositions:
				log.error(f'Received sell alert for {symbol}-{algoId}-{timeframe}-{type}-{right} but there were no matching positions found.')
	else:
		log.error(f"No trade symbol definition for {symbol}-{algoId}-{timeframe}-{type}")

async def UpdatePositionsFromBroker():
      pass

async def WatchAlgoSignalsCollection():
	while True:
		try:
			async with signalsDoc.watch() as change_stream:
				async for change in change_stream:
					if 'fullDocument' in change:  # Only send updates on new rows being inserted
						await onTradeSignal(change['fullDocument'])
		except Exception as e:
			log.exception(f'WatchAlgoSignalsCollection threw exception {e}')

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
			# TODO - every minute or so, scan open positions on our broker and update our positions table
		except Exception as e:
			log.error(f'Something bad happened: {e}')

if __name__ == "__main__":
	try:
		asyncio.run(main())
	except Exception as e:
		print(e)
