import API_request as api 
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1)] )
#creating the empty collection rawdata within the database index

collection_itbittraw = db.itbittraw



######################## itbit

assets1 = ['BTC', 'ETH']
fiat1 = ['EUR', 'USD']

for Crypto in assets1:
    for Fiat in  fiat1:

        api.itbit_ticker(Crypto, Fiat, collection_itbittraw)

itbit = time.time()


print("This script took: {} minutes".format(float(itbit-start)))
