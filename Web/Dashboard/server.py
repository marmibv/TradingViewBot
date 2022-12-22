from flask import Flask, request, render_template
import json
from . import Database

app = Flask("tvbotweb", template_folder='./templates')
dal = Database.DataAccessLayer('TVTest', 'TVSignals')

@app.route("/")
def home():
	return render_template('home.html')

@app.route("/trades")
async def trades():
	trades = await dal.GetTrades()
	return render_template('trades.html', trades=trades)

@app.errorhandler()
@app.route("/webhook", methods=['POST'])
async def webhook():
	postMsg = json.loads(request.data)
	# What are we doing?
	# Parse out the message and log it in the signals database
	if postMsg['phrase'] == 'CrisplyBlueDuck':
		algoId = postMsg['algo']
		symbol = postMsg['ticker']
		timeframe = postMsg['timeframe']
		type = postMsg['type'] # option, stock, future etc...
		action = postMsg['side']
		signal = postMsg['reason']
		price = postMsg['close']
		tier = 1
		ts = postMsg['time']

		await dal.LogAlgoSignal(algoId, symbol, timeframe, action, signal, price, tier, ts)
		print(postMsg)
		return f"<p>Hey captain Hook!</p>{postMsg}"
	else:
		return '403'

