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

# assets
assets = ['BTC', 'ETH']
assets1 = ['LTC', 'BCH', 'ETC']
assets2 = ['XLM', 'EOS', 'XRP']
assets3 = ['ZEC']

# fiat
fiat = ['EUR', 'USD', 'GBP', 'USDC']
fiat1 = ['EUR', 'USD', 'GBP']
fiat2 = ['EUR', 'USD']
fiat3 = ['USDC']

call = [api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw) for Crypto in assets  for Fiat in fiat]
call = [api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw) for Crypto in assets1  for Fiat in fiat1]
call = [api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw) for Crypto in assets2  for Fiat in fiat2]
call = [api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw) for Crypto in assets3  for Fiat in fiat3]
coinbase = time.time()
        

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

assets3 = ['BTC', 'ETH','LTC','BCH','ZEC']
fiat2 = ['USD']

for Crypto in assets3:
    for Fiat in fiat2:

        api.gemini_ticker(Crypto,Fiat, collection_geminitraw)

gemini = time.time()
######################## bittrex

assets = ['BTC', 'ETH','BCH', 'BSV']
assets1 = ['LTC','XRP','ZEC', 'EOS', 'ETC']
assets2 = ['XLM', 'XMR']
stbc = ['USD', 'EUR', 'USDT']
stbc1 = ['USD', 'USDT']
stbc2 = ['USDT']

call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets for Fiat in stbc]
call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets1 for Fiat in stbc1]
call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets2 for Fiat in stbc2]
        
bittrex = time.time()
######################## poloniex

assets = ['BTC', 'ETH','BCHABC', 'BCHSV', 'LTC','XRP','ZEC', 'EOS', 'ETC', 'STR', 'XMR']
stbc = ['USDC', 'USDT']

call =[api.poloniex_ticker(Crypto,Fiat, collection_poloniextraw)  for Crypto in assets for Fiat in stbc]

poloniex = time.time()
###################### bitflyer

assets1 = ['BTC']
assets2 = ['ETH']
fiat1 = ['EUR', 'USD', 'JPY']
fiat2 = ['JPY']

call =[api.bitflyer_ticker(Crypto, Fiat, collection_bitflyertraw)  for Crypto in assets1 for Fiat in fiat1]
call =[api.bitflyer_ticker(Crypto, Fiat, collection_bitflyertraw)  for Crypto in assets2 for Fiat in fiat2]        
        
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