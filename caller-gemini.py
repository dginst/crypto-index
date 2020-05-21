import API_request as api
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index
db.rawdata.create_index([("id", -1)])
# creating the empty collection rawdata within the database index

exc_raw_collection = db.EXC_rawdata


# gemini

assets3 = ['BTC', 'ETH', 'LTC', 'BCH', 'ZEC']
fiat2 = ['USD']

for Crypto in assets3:
    for Fiat in fiat2:

        api.gemini_ticker(Crypto, Fiat, exc_raw_collection)

gemini = time.time()

print("This script took: {} seconds".format(float(gemini-start)))
