import API_request as api 
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1)] )
#creating the empty collection rawdata within the database index

exc_raw_collection = db.EXC_rawdata

###################### bitflyer

assets1 = ['BTC']
assets2 = ['ETH']
fiat1 = ['EUR', 'USD', 'JPY']
fiat2 = ['JPY']

call =[api.bitflyer_ticker(Crypto, Fiat, exc_raw_collection)  for Crypto in assets1 for Fiat in fiat1]
call =[api.bitflyer_ticker(Crypto, Fiat, exc_raw_collection)  for Crypto in assets2 for Fiat in fiat2]        
        
bitflyer = time.time()

print("This script took: {} seconds".format(float(bitflyer-start)))