import time

import cryptoindex.API_request as api
from pymongo import MongoClient

start = time.time()

connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# creating the empty collection rawdata within the database index

exc_raw_collection = db.EXC_rawdata

# bitstamp

assets2 = ["BTC", "ETH", "XRP", "LTC", "BCH"]
fiat1 = ["EUR", "USD"]

for Crypto in assets2:
    for Fiat in fiat1:

        api.bitstamp_ticker(Crypto, Fiat, exc_raw_collection)

bitstamp = time.time()
kraken = time.time()


print("This script took: {} seconds".format(float(kraken - start)))
