######################################################################################################
# The file download from the CryotoWatch websites the market data of this set of Cryptocurrencies:
# 'ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV'and 'ETC'
# from this set of exchanges:
# 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken' and 'bitflyer'
# and for each fiat currenncies in this set:
# 'gbp', 'usd', 'eur', 'cad' and 'jpy'
# Once downloaded the file saves the raw data on MongoDB in the database "index" and collection "rawdata"
# It is possible to change the period of downlaod modifying the "start_date"
#######################################################################################################

# standard library import
import time
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import requests
from requests import get

# third party import
import pandas as pd
import numpy as np
from pymongo import MongoClient

# local import
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.mongo_setup as mongo

####################################### initial settings ############################################

start_date = '01-01-2016'

# set end_date as today, otherwise comment and choose an end_date
end_date = datetime.now().strftime('%m-%d-%Y')
# end_date = 


# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600


# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)

pair_array = ['gbp', 'usd', 'eur', 'cad', 'jpy']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC'] 
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup MongoDB connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.rawdata.drop()

#creating the empty collection rawdata within the database index
db.rawdata.create_index([ ("id", -1) ])
collection_raw = db.rawdata

####################################### downloading and storing part #################################

for Crypto in Crypto_Asset:

    currencypair_array = []
    for i in pair_array:
        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
    
    
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting to CryptoWatch website
            data_download.CW_raw_to_mongo(exchange, cp, collection_raw, start_date, end_date)




