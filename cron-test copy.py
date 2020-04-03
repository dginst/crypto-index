import API_request_copy as api 
import time
import requests
import random
import multiprocessing

start = time.time()

# connection = MongoClient('localhost', 27017)
# #creating the database called index
# db = connection.index
# db.rawdata.create_index([ ("id", -1)] )
# #creating the empty collection rawdata within the database index
# collection_geminitraw = db.geminitraw
# collection_bittrextraw = db.bittrextraw
# collection_bitflyertraw = db.bitflyertraw
# collection_coinbasetraw = db.coinbasetraw
# collection_bitstamptraw = db.bitstamptraw
# collection_itbittraw = db.itbittraw
# collection_poloniextraw = db.poloniextraw
# collection_krakentraw = db.krakentraw

urls = []
urls1 = []
########################## COINBASE

# assets
assets = ['BTC', 'ETH']
assets1 = ['LTC', 'BCH', 'ETC']
assets2 = ['XLM', 'EOS', 'XRP']
assets3 = ['ZEC']

# fiat
fiat = ['EUR', 'USD', 'GBP', 'USDC']
fiat1 = ['EUR', 'USD', 'GBP']
fiat2 = ['EUR', 'USD']
fiat3 = ['USDC']

urls.append([api.coinbase_ticker(Crypto, Fiat) for Crypto in assets  for Fiat in fiat])
urls.append([api.coinbase_ticker(Crypto, Fiat) for Crypto in assets1  for Fiat in fiat1])
urls.append([api.coinbase_ticker(Crypto, Fiat) for Crypto in assets2  for Fiat in fiat2])
urls.append([api.coinbase_ticker(Crypto, Fiat) for Crypto in assets3  for Fiat in fiat3])
coinbase = time.time()
        

######################### KRAKEN

# assets
assets = ['BTC', 'ETH']
assets1 = ['LTC','BCH','XLM', 'ADA', 'XMR', 'EOS', 'ETC', 'ZEC']
assets2 = ['XRP']

# fiat
fiat = ['EUR', 'USD', 'CAD', 'GBP','JPY', 'USDC','USDT', 'CHF']
fiat1 = ['USD', 'EUR']
fiat2 = ['EUR', 'USD', 'CAD','JPY']

urls1.append([api.kraken_ticker(Crypto, Fiat)  for Crypto in assets for Fiat in fiat])
urls1.append([api.kraken_ticker(Crypto, Fiat)  for Crypto in assets1 for Fiat in fiat1])
urls1.append([api.kraken_ticker(Crypto, Fiat)  for Crypto in assets2 for Fiat in fiat2])
kraken = time.time()       

######################## itbit

assets1 = ['BTC', 'ETH']
fiat1 = ['EUR', 'USD']


urls.append([api.itbit_ticker(Crypto,Fiat)  for Crypto in assets1 for Fiat in fiat1])  
itbit = time.time()

######################## bitstamp

assets2 = ['BTC', 'ETH','XRP','LTC','BCH']
fiat1 = ['EUR', 'USD']

urls.append([api.bitstamp_ticker(Crypto,Fiat)  for Crypto in assets2 for Fiat in fiat1])

bitstamp = time.time()
######################## gemini

assets3 = ['BTC', 'ETH','LTC','BCH','ZEC']
fiat2 = ['USD']


urls.append([api.gemini_ticker(Crypto,Fiat)  for Crypto in assets3 for Fiat in fiat2])

       

gemini = time.time()
######################## bittrex

assets = ['BTC', 'ETH','BCH', 'BSV']
assets1 = ['LTC','XRP','ZEC', 'EOS', 'ETC']
assets2 = ['XLM', 'XMR']
stbc = ['USD', 'EUR', 'USDT']
stbc1 = ['USD', 'USDT']
stbc2 = ['USDT']

urls1.append([api.bittrex_ticker(Crypto, Fiat)  for Crypto in assets for Fiat in stbc])
urls1.append([api.bittrex_ticker(Crypto, Fiat)  for Crypto in assets1 for Fiat in stbc1])
urls.append([api.bittrex_ticker(Crypto, Fiat)  for Crypto in assets2 for Fiat in stbc2])
        
bittrex = time.time()
######################## poloniex

assets = ['BTC', 'ETH','BCHABC', 'BCHSV', 'LTC','XRP','ZEC', 'EOS', 'ETC', 'STR', 'XMR']
stbc = ['USDC', 'USDT']

#urls.append([api.poloniex_ticker(Crypto,Fiat)  for Crypto in assets for Fiat in stbc])

poloniex = time.time()
###################### bitflyer

assets1 = ['BTC']
assets2 = ['ETH']
fiat1 = ['EUR', 'USD', 'JPY']
fiat2 = ['JPY']

urls.append([api.bitflyer_ticker(Crypto, Fiat)  for Crypto in assets1 for Fiat in fiat1])
urls.append([api.bitflyer_ticker(Crypto, Fiat)  for Crypto in assets2 for Fiat in fiat2])        

flat_list = []
for sublist in urls:
    for item in sublist:
        flat_list.append(item)

flat_list1 = []
for sublist in urls1:
    for item in sublist:
        flat_list1.append(item)

print(flat_list)
print('PIRU PIRU')

random.seed(1021202341)
random.shuffle(flat_list)
bitflyer = time.time()

def call1(flat_list):

    call = [requests.get(u) for u in flat_list]
    
    return call

def call2(flat_list1):

    call = [requests.get(u) for u in flat_list1]

    return call

print(call1(flat_list))



end1 = time.time()
print("This script took: {} minutes".format(float(coinbase-start)))
print("This script took: {} minutes".format(float(kraken-coinbase)))
print("This script took: {} minutes".format(float(itbit-kraken)))
print("This script took: {} minutes".format(float(bitstamp-itbit)))
print("This script took: {} minutes".format(float(gemini-bitstamp)))
print("This script took: {} minutes".format(float(bittrex-gemini)))
print("This script took: {} minutes".format(float(poloniex-bittrex)))
print("This script took: {} minutes".format(float(bitflyer-poloniex)))
print("This script took: {} minutes".format(float(end1-bitflyer)))
print("This script took: {} minutes".format(float(end1-start)))