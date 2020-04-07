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

######################## gemini

assets3 = ['BTC', 'ETH','LTC','BCH','ZEC']
fiat2 = ['USD']

for Crypto in assets3:
    for Fiat in fiat2:

        api.gemini_ticker(Crypto,Fiat, collection_geminitraw)

gemini = time.time()

print("This script took: {} minutes".format(float(gemini-start)))
