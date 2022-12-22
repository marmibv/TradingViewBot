from flask import Flask, request
import json
import Database as Database
import pymongo
import logging
import sys

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

app = Flask("tvbotweb")
client = pymongo.MongoClient("mongodb://localhost:27017")
algoBotDb = client['AlgoBot']
signalsDoc = algoBotDb['TVSignals']

def makeAlgoSignalKey(algoId, symbol, timeframe):
	return f'{algoId}:{symbol}:{timeframe}'

def LogAlgoSignal(algoId, symbol, timeframe, type, side, right, signal, close, tier, ts):
	id = f'{makeAlgoSignalKey(algoId, symbol, timeframe)}:{ts}'
	row = {'_id': id, 'type': type, 'side': side, 'right': right, 'signal': signal, 'close': close, 'tier': tier, 'timestampSeconds': ts, 'algoId': algoId, 'symbol': symbol, 'timeframe': timeframe}
	signalsDoc.insert_one(row)
 
@app.route("/webhook", methods=['POST'])
def webhook():
	postMsg = json.loads(request.data)
	# What are we doing?
	# Parse out the message and log it in the signals database
	if postMsg['phrase'] == 'CrispyBlueDuck':
		algoId = postMsg['algo']
		symbol = postMsg['ticker']
		timeframe = postMsg['timeframe']
		type = postMsg['type'] # option, stock, future etc...
		side = postMsg['side']
		right = postMsg['right']
		signal = postMsg['reason']
		price = postMsg['close']
		tier = 1
		ts = postMsg['time']

		LogAlgoSignal(algoId, symbol, timeframe, type, side, right, signal, price, tier, ts)
		print(postMsg)
		return {
			'code': 200,
			'message': 'hook successfully logged trade signal'
		}
	else:
		return {
			'code': 403,
			'message': 'hook failed authentication'
		}

app.run(debug=True)