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
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.mongo_setup as mongo

####################################### initial settings ############################################

start_date = '01-01-2016'
CW_stop_date = '04-17-2020'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)

# define the array containing the date where the index uses CW feed data
CW_date_vector = data_setup.timestamp_gen(start_date, CW_stop_date)
CW_date_vector = [str(date) for date in CW_date_vector]

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC'] 
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup MongoDB connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.index_data_feed.drop()

#creating the empty collection cleandata within the database index
db.index_data_feed.create_index([ ("id", -1) ])
collection_feed = db.index_data_feed

# defining the database name and the collection name
database = "index"
collection_CW = "CW_final_data"
collection_EXC = "EXC_final_data"

############################## data conversion main part ##################################

EXC_series = mongo.query_mongo(database, collection_EXC)

print('EXC done')

for date in CW_date_vector:

    print(date)
    query = {'Time': str(date)}
    single_date_matrix = mongo.query_mongo(database, collection_CW, query)

    if date == CW_date_vector[0]:

        matrix = single_date_matrix
    
    else:

        matrix = matrix.append(single_date_matrix)







data_feed = matrix.append(EXC_series, sort = True)


# put the converted data on MongoDB
data = data_feed.to_dict(orient = 'records')  
collection_feed.insert_many(data)

