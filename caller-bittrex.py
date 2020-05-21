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


# bittrex

assets = ['BTC', 'ETH', 'BCH', 'BSV']
assets1 = ['LTC', 'XRP', 'ZEC', 'EOS', 'ETC']
assets2 = ['XLM', 'XMR']
stbc = ['USD', 'EUR', 'USDT']
stbc1 = ['USD', 'USDT']
stbc2 = ['USDT']

call = [api.bittrex_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets for Fiat in stbc]
call = [api.bittrex_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets1 for Fiat in stbc1]
call = [api.bittrex_ticker(Crypto, Fiat, exc_raw_collection)
        for Crypto in assets2 for Fiat in stbc2]

bittrex = time.time()

print("This script took: {} seconds".format(float(bittrex-start)))
