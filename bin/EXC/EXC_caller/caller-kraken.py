import time

import cryptoindex.API_request as api
from pymongo import MongoClient

start = time.time()

connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index
db.rawdata.create_index([("id", -1)])
# creating the empty collection rawdata within the database index
exc_raw_collection = db.EXC_rawdata


# KRAKEN

# assets
assets = ["BTC", "ETH"]
assets1 = ["XLM", "ADA", "XMR", "EOS", "ETC", "ZEC"]
assets2 = ["XRP"]
assets3 = ["LTC", "BCH"]

# fiat
fiat = ["EUR", "USD", "CAD", "GBP", "JPY", "USDC", "USDT", "CHF"]
fiat1 = ["USD", "EUR"]
fiat2 = ["EUR", "USD", "CAD", "JPY", "GBP", "USDT"]
fiat3 = ["USD", "EUR", "GBP", "USDT"]

call = [
    api.kraken_ticker(Crypto, Fiat, exc_raw_collection)
    for Crypto in assets
    for Fiat in fiat
]
call = [
    api.kraken_ticker(Crypto, Fiat, exc_raw_collection)
    for Crypto in assets1
    for Fiat in fiat1
]
call = [
    api.kraken_ticker(Crypto, Fiat, exc_raw_collection)
    for Crypto in assets2
    for Fiat in fiat2
]
call = [
    api.kraken_ticker(Crypto, Fiat, exc_raw_collection)
    for Crypto in assets3
    for Fiat in fiat3
]
kraken = time.time()


print("This script took: {} seconds".format(float(kraken - start)))
