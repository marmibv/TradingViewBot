# Python 3 server example
from datetime import datetime
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler, HTTPServer
import math
import socketserver
from tracemalloc import start
from pymongo import MongoClient
import time

hostName = "0.0.0.0"
serverPort = 8132


class DataAccessLayer():
	def __init__(self) -> None:
		self.dbClient = MongoClient('mongodb://localhost:27017')
		self.algoBotDb = self.dbClient['AlgoBot']
		self.signalsColl = self.algoBotDb['Signals']

	def GetSignals(self, symbol, timeframe, startTime):
		signals = []
		cursor = self.signalsColl.find({'symbol': symbol, 'action': {"$ne": 'startup'}, 'timeframe': timeframe, 'tsSeconds': {"$gte": startTime}})
		for row in cursor:
			signals.append(row)
		return signals


class MyServer(BaseHTTPRequestHandler):
	def __init__(self, request: bytes, client_address: tuple[str, int], server: socketserver.BaseServer) -> None:
		self.dal = DataAccessLayer()
		super().__init__(request, client_address, server)

	def do_GET(self):
		startTime = 1663138800
		dtStart = datetime.fromtimestamp(startTime)
		parts = parse_qs(urlparse(self.path).query)
		base = (parts['symbol'][0] if 'symbol' in parts else 'AVAX').upper()
		timeframe = (parts['timeframe'][0] if 'timeframe' in parts else '5m')
		leverage = float(parts['leverage'][0] if 'leverage' in parts else '1')
		startingCash = float(parts['start'][0] if 'start' in parts else '100')
		commissionRate = float(parts['commission'][0] if 'commission' in parts else '0.0008')
		slippage = float(parts['slippage'][0] if 'slippage' in parts else '0.0001')

		lookup = {'AVAX': 'AVAX-USDT', 'ETH': 'ETH-USDT', 'ADA': 'ADA-USDT'}
		symbol = lookup[base] if base in lookup else 'AVAX-USDT'

		signals = self.dal.GetSignals(symbol, timeframe, startTime)
		nSignals = len(signals)

		self.send_response(200)
		self.send_header("Content-type", "text/html")
		self.end_headers()

		self.wfile.write(bytes(f"<html><head><title>AlgoBot {symbol} Signals</title></head>", "utf-8"))
		self.wfile.write(bytes("<body>", "utf-8"))
		self.wfile.write(bytes(f"<h2>AlgoBot Signals on {symbol}</h2>", "utf-8"))
		self.wfile.write(bytes(f"<p>Showing trades since {dtStart}. Starting cash: ${startingCash}, leverage: {leverage}, commission: {commissionRate*100:.2f}%, slippage: {slippage*100:.2f}%</p>", "utf-8"))

		self.wfile.write(bytes("<table><tr><th>DATE TIME</th><th>ACTION</th><th>SIGNAL</th><th>PRICE</th><th>QTY</th><th>Gain/Loss%</th><th>Gain/Loss</th><th>Drawdown</th><th>VALUE</th></tr>", "utf-8"))

		# Add leverage capability?
		qty = None
		portfolioValue = None
		wins = trades = 0
		firstSignal = signals[0]
		totalValue = startingCash
		maxValue = totalValue

		lastAction = None
		lastTotalValue = totalValue
		lastPortfolioValue = startingCash * leverage
		tsFirst = firstSignal['tsSeconds']
		dtNow = datetime.fromtimestamp(time.time())
		dtFirst = datetime.fromtimestamp(tsFirst)
		deltaTime = dtNow - (dtStart if dtStart > dtFirst else dtFirst)
		deltaDays = deltaTime.days + dtNow.hour / 24

		if firstSignal['action'] == 'sell-and-short':
			portfolioValue = 0
			qty = startingCash * leverage / firstSignal['price']
			lastAction = 'cover-and-buy'
		elif firstSignal['action'] == 'cover-and-buy':
			portfolioValue = 0
			qty = -(startingCash * leverage / firstSignal['price'])
			lastAction = 'sell-and-short'

		for signal in signals:
			price = signal['price']
			action = signal['action']
			sig = signal['signal']
			ts = signal['tsSeconds']
			dtSignal = datetime.fromtimestamp(ts)
			if action == 'sell-and-short' and lastAction == 'cover-and-buy':
				sellOrderValue = qty * (price - slippage*price)
				sellCommission = sellOrderValue * commissionRate
				portfolioValue = portfolioValue + sellOrderValue - sellCommission
				qty = portfolioValue/price
				totalValue += portfolioValue - lastPortfolioValue
				lastPortfolioValue = portfolioValue
				shortOrderValue = qty * (price - slippage*price)
				shortCommission = shortOrderValue * commissionRate
				portfolioValue = portfolioValue + shortOrderValue - shortCommission
				lastAction = action
			elif action == 'cover-and-buy' and lastAction == 'sell-and-short':
				coverOrderValue = qty * (price + slippage*price)
				coverCommission = coverOrderValue * commissionRate
				portfolioValue = portfolioValue - coverOrderValue - coverCommission
				qty = portfolioValue/price
				totalValue += portfolioValue - lastPortfolioValue
				lastPortfolioValue = portfolioValue
				buyOrderValue = qty * (price + slippage*price)
				buyCommission = buyOrderValue * commissionRate
				portfolioValue = portfolioValue - buyOrderValue - buyCommission
				lastAction = action
			else:
				signal = 'BAD'

			bWon = totalValue > lastTotalValue
			wins += 1 if bWon else 0
			trades += 1
			color = 'lightgreen' if bWon else 'lightpink'
			rowStyle = f'style="background-color:{color}"'
			gain = totalValue - lastTotalValue
			maxValue = max(totalValue, maxValue)
			drawdown = (maxValue - totalValue)/maxValue
			strDrawdown = f'{drawdown*100:.2f}%' if drawdown > 0 else ''

			if trades > nSignals - 40:
				self.wfile.write(bytes(f"<tr {rowStyle}><td>{dtSignal}</td><td style=\"color: {'red' if action == 'sell-and-short' else 'blue'}\">{'SELL' if action == 'sell-and-short' else 'BUY'}</td>\
				<td>{sig}</td><td>{price}</td><td>{qty:.3f}</td><td>{gain/totalValue*100:.2f}%</td><td>{gain:.2f}</td><td>{strDrawdown}</td><td>{totalValue:.2f}</td></tr>", "utf-8"))

			lastTotalValue = totalValue
		self.wfile.write(bytes("</table>", "utf-8"))

		overallGainPerc = (totalValue - startingCash)/startingCash


		dailyRR = math.pow(abs(overallGainPerc) + 1, 1/deltaDays)

		self.wfile.write(bytes(f"<h3>Win rate {(wins/trades *100.0):.2f}%, Overall Gain: {overallGainPerc*100:.2f}%, Daily RR: {'-' if overallGainPerc < 0 else ''}{(dailyRR-1)*100:.2f}%</h3>", "utf-8"))

		self.wfile.write(bytes("</body></html>", "utf-8"))


if __name__ == "__main__":
	webServer = HTTPServer((hostName, serverPort), MyServer)
	print("Server started http://%s:%s" % ('', serverPort))

	try:
		webServer.serve_forever()
	except KeyboardInterrupt:
		pass

	webServer.server_close()
	print("Server stopped.")
