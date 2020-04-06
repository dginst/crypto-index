import API_request as api 
from pymongo import MongoClient
import time

start = time.time()

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1)] )
#creating the empty collection rawdata within the database index

collection_bittrextraw = db.bittrextraw



######################## bittrex

assets = ['BTC', 'ETH','BCH', 'BSV']
assets1 = ['LTC','XRP','ZEC', 'EOS', 'ETC']
assets2 = ['XLM', 'XMR']
stbc = ['USD', 'EUR', 'USDT']
stbc1 = ['USD', 'USDT']
stbc2 = ['USDT']

call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets for Fiat in stbc]
call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets1 for Fiat in stbc1]
call =[api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)  for Crypto in assets2 for Fiat in stbc2]
        
bittrex = time.time()

print("This script took: {} minutes".format(float(bittrex-start)))