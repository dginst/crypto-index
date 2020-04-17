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



######################## poloniex

assets = ['BTC', 'ETH','BCHABC', 'BCHSV', 'LTC','XRP','ZEC', 'EOS', 'ETC', 'STR', 'XMR']
stbc = ['USDC', 'USDT']

call =[api.poloniex_ticker(Crypto,Fiat, exc_raw_collection)  for Crypto in assets for Fiat in stbc]
poloniex = time.time()

print("This script took: {} seconds".format(float(poloniex-start)))