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

########################## COINBASE
assets = ['BTC', 'ETH','XRP','LTC','BCH','XLM', 'ZEC', 'EOS', 'ETC']
fiat = ['EUR', 'USD', 'CAD', 'USDT', 'GBP', 'USDC','JPY', 'USDC']

ciao = [api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw) for Crypto in assets  for Fiat in fiat]

coinbase = time.time()
        

######################### KRAKEN
assets = ['BTC', 'ETH', 'XRP','ZEC']
assets1 = ['LTC','BCH','XLM', 'ADA', 'XMR', 'EOS', 'ETC']
fiat = ['EUR', 'USD', 'CAD', 'GBP', 'USDC','JPY', 'USDC','USDT']
fiat1 = ['USD', 'EUR']


ciao =[api.kraken_ticker(Crypto, Fiat, collection_krakentraw)  for Crypto in assets for Fiat in fiat]
ciao =[api.kraken_ticker(Crypto, Fiat, collection_krakentraw)  for Crypto in assets1 for Fiat in fiat1]
kraken = time.time()       

######################## itbit

assets1 = ['BTC', 'ETH']
fiat1 = ['EUR', 'USD']

for Crypto in assets1:
    for Fiat in  fiat1:

        api.itbit_ticker(Crypto, Fiat, collection_itbittraw)

itbit = time.time()

######################## bitstamp

assets2 = ['BTC', 'ETH','XRP','LTC','BCH']
fiat1 = ['EUR', 'USD']

for Crypto in assets2:
    for Fiat in fiat1:

        api.bitstamp_ticker(Crypto, Fiat, collection_bitstamptraw)

bitstamp = time.time()
######################## gemini

assets3 = ['BTC', 'ETH','XRP','LTC','BCH','ZEC']
fiat2 = ['USD', 'USDC']

for Crypto in assets3:
    for Fiat in fiat2:

        api.gemini_ticker(Crypto,Fiat, collection_geminitraw)

gemini = time.time()
######################## bittrex

assets = ['BTC', 'ETH','XRP','LTC','BCH','XLM', 'ZEC', 'EOS', 'ETC', 'BSV', 'XMR']
stbc = ['USD', 'EUR', 'USDT']

for Crypto in assets:
    for Fiat in stbc:

        api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)
        
bittrex = time.time()
######################## poloniex


stbc = ['USDC', 'USDT']

for Crypto in assets:
    for Fiat in stbc:

        api.poloniex_ticker(Crypto,Fiat, collection_poloniextraw)

poloniex = time.time()
###################### bitflyer

assets1 = ['BTC', 'ETH']
fiat1 = ['EUR', 'USD', 'JPY']

for Crypto in assets1:
    for Fiat in fiat1:
        
        api.bitflyer_ticker(Crypto, Fiat, collection_bitflyertraw)
bitflyer = time.time()
end = time.time()

print("This script took: {} minutes".format(float(coinbase-start)))
print("This script took: {} minutes".format(float(kraken-coinbase)))
print("This script took: {} minutes".format(float(itbit-kraken)))
print("This script took: {} minutes".format(float(bitstamp-itbit)))
print("This script took: {} minutes".format(float(gemini-bitstamp)))
print("This script took: {} minutes".format(float(bittrex-gemini)))
print("This script took: {} minutes".format(float(poloniex-bittrex)))
print("This script took: {} minutes".format(float(bitflyer-poloniex)))
print("This script took: {} minutes".format(float(end-start)))