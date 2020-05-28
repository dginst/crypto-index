import time

import cryptoindex.API_request as api
from pymongo import MongoClient

start = time.time()

connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index
db.rawdata.create_index([("id", -1)])
# creating the empty collection rawdata within the database index

exc_raw_collection = db.EXC_rawdata


# COINBASE

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

call = [api.coinbase_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets for Fiat in fiat]
time.sleep(1)
call = [api.coinbase_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets1 for Fiat in fiat1]
time.sleep(1)
call = [api.coinbase_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets2 for Fiat in fiat2]
time.sleep(1)
call = [api.coinbase_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets3 for Fiat in fiat3]
coinbase = time.time()


print("This script took: {} seconds".format(float(coinbase-start)))
