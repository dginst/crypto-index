import API_request_copy as api 
from pymongo import MongoClient
import time

start = time.time()
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_geminitraw = db.geminitraw1
collection_bittrextraw = db.bittrextraw1
collection_bitflyertraw = db.bitflyertraw1
collection_coinbasetraw = db.coinbasetraw1
collection_bitstamptraw = db.bitstamptraw1
collection_itbittraw = db.itbittraw1
collection_poloniextraw = db.poloniextraw1
collection_krakentraw = db.krakentraw1

assets = ['BTC', 'ETH',['XRP','LTC','BCH','XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
fiat = ['EUR', 'USD', 'CAD', 'USDT', 'GBP', 'USDC','JPY']

urls = []

for Crypto in assets:
    for Fiat in fiat:

        urls.append(api.coinbase_ticker(Crypto, Fiat))
        urls.append(api.kraken_ticker(Crypto, Fiat))
        urls.append(api.poloniex_ticker(Crypto,Fiat))
        urls.append(api.itbit_ticker(Crypto, Fiat))
        urls.append(api.gemini_ticker(Crypto,Fiat))
        urls.append(api.bitstamp_ticker(Crypto, Fiat))
        urls.append(api.bitflyer_ticker(Crypto, Fiat))
        urls.append(api.bittrex_ticker(Crypto, Fiat))

start1 = time.time()

rs = (grequests.get(u) for u in urls)

print(grequests.map(rs))

end = time.time()

print(end-start1)