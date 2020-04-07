import API_request as api 
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

#creating the empty collection rawdata within the database index

collection_bitstamptraw = db.bitstamptraw


######################## bitstamp

assets2 = ['BTC', 'ETH','XRP','LTC','BCH']
fiat1 = ['EUR', 'USD']

for Crypto in assets2:
    for Fiat in fiat1:

        api.bitstamp_ticker(Crypto, Fiat, collection_bitstamptraw)

bitstamp = time.time()
kraken = time.time()       


print("This script took: {} minutes".format(float(kraken-start)))
