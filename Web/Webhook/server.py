from flask import Flask, request
import json
import pymongo
import logging
import sys

logging.basicConfig(filename="webhook.log", filemode='w', level=logging.ERROR)
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

log = logging.getLogger('WEBHOOK')
log.setLevel(logging.DEBUG)

app = Flask("tvbotweb")
flaskLogger = logging.getLogger('werkzeug')
flaskLogger.setLevel(logging.ERROR)

client = pymongo.MongoClient("mongodb://localhost:27017")
algoBotDb = client['AlgoBot']
signalsDoc = algoBotDb['TVSignals']

symbolLookup = {
	'BNBUSDT': 'BNB-USDT'
}

def makeAlgoSignalKey(algoId, symbol, timeframe):
	return f'{algoId}:{symbol}:{timeframe}'

def LogAlgoSignal(algoId, symbol, timeframe, type, side, right, signal, close, tier, ts):
	id = f'{makeAlgoSignalKey(algoId, symbol, timeframe)}:{ts}'
	row = {'_id': id, 'type': type, 'side': side, 'right': right, 'signal': signal, 'close': close, 'tier': tier, 'timestampSeconds': ts, 'algoId': algoId, 'symbol': symbol, 'timeframe': timeframe}
	signalsDoc.insert_one(row)
 
@app.route("/webhook", methods=['POST'])
def webhook():
	try:
		postMsg = json.loads(request.data)
		# What are we doing?
		# Parse out the message and log it in the signals database
		if postMsg['phrase'] == 'CrispyBlueDuck':
			algoId = postMsg['algo'].upper()
			symbol = postMsg['ticker'].upper()
			timeframe = postMsg['timeframe']
			type = postMsg['type'].upper() # option, stock, future etc...
			side = postMsg['side'].upper() # buy/sell
			right = postMsg['right'].upper() # call/put
			signal = postMsg['reason'] # reason for signal
			close = postMsg['close'] # close at signal
			tier = 1
			ts = postMsg['time'] # timestamp of signal
			log.oper(f'Logging signal {algoId}:{symbol}:{timeframe}:{type} {side} {symbol} {right} at {close} because {signal} ({tier}:{ts})')

			# perform cross reference lookup, ie BNBUSDT->BNB-USDT
			symbol = symbolLookup[symbol] if symbol in symbolLookup else symbol

			# Log signal in DB
			LogAlgoSignal(algoId, symbol, timeframe, type, side, right, signal, close, tier, ts)
			return {'code': 200, 'message': 'hook successfully logged trade signal'}
		else:
			return {'code': 403, 'message': 'hook failed authentication'}
	except Exception as e:
		log.error(f'Exception occurred processing webhook contents {e}')
		return {'code': 500, 'message': 'unable to process json post content'}

# TradingView can only send to port 80 or 443 and we now listen externally for the signal event from just one TV account
#app.run(debug=False, host='0.0.0.0', port=80)
app.run(debug=True)
