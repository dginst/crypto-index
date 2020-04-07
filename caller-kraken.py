import API_request as api 
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1)] )
#creating the empty collection rawdata within the database index
collection_geminitraw = db.geminitraw
collection_bittrextraw = db.bittrextraw
collection_bitflyertraw = db.bitflyertraw
collection_coinbasetraw = db.coinbasetraw
collection_bitstamptraw = db.bitstamptraw
collection_itbittraw = db.itbittraw
collection_poloniextraw = db.poloniextraw
collection_krakentraw = db.krakentraw


######################### KRAKEN

# assets
assets = ['BTC', 'ETH']
assets1 = ['LTC','BCH','XLM', 'ADA', 'XMR', 'EOS', 'ETC', 'ZEC']
assets2 = ['XRP']

# fiat
fiat = ['EUR', 'USD', 'CAD', 'GBP','JPY', 'USDC','USDT', 'CHF']
fiat1 = ['USD', 'EUR']
fiat2 = ['EUR', 'USD', 'CAD','JPY']

call =[api.kraken_ticker(Crypto, Fiat, collection_krakentraw)  for Crypto in assets for Fiat in fiat]
call =[api.kraken_ticker(Crypto, Fiat, collection_krakentraw)  for Crypto in assets1 for Fiat in fiat1]
call =[api.kraken_ticker(Crypto, Fiat, collection_krakentraw)  for Crypto in assets2 for Fiat in fiat2]

kraken = time.time()       


print("This script took: {} minutes".format(float(kraken-start)))
