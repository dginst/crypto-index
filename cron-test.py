import API_request as api 
from pymongo import MongoClient

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_geminitraw = db.geminitraw
collection_bittrextraw = db.bittrextraw
collection_bitflyertraw = db.bitflyertraw
collection_coinbasetraw = db.coinbasetraw
collection_bitstamptraw = db.bitstamptraw
collection_itbittraw = db.itbittraw
collection_poloniextraw = db.poloniextraw
collection_krakentraw = db.krakentraw

assets = [['BTC'], ['ETH'],['XRP'],['LTC'],['BCH'],['XLM'], ['ADA'], ['ZEC'], ['XMR'], ['EOS'], ['BSV'], ['ETC']]
fiat = [['EUR'], ['USD'], ['CAD'], ['USDT'], ['GBP'], ['USDC'],['JPY']]

for Crypto in assets:
    for Fiat in fiat:

        api.coinbase_ticker(Crypto, Fiat, collection_coinbasetraw)
        api.kraken_ticker(Crypto, Fiat, collection_krakentraw)
        api.poloniex_ticker(Crypto,Fiat, collection_poloniextraw)
        api.itbit_ticker(Crypto, Fiat, collection_itbittraw)
        api.gemini_ticker(Crypto,Fiat, collection_geminitraw)
        api.bitstamp_ticker(Crypto, Fiat, collection_bitstamptraw)
        api.bitflyer_ticker(Crypto, Fiat, collection_bitflyertraw)
        api.bittrex_ticker(Crypto, Fiat, collection_bittrextraw)