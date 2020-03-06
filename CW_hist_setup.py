import utils.data_setup as data_setup
import utils.data_download as data_download
from pymongo import MongoClient
import time
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import pandas as pd
import requests
from requests import get
import mongoconn as mongo

start_date = '01-01-2019'

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

####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.cleandata.drop()

#creating the empty collection cleandata within the database index
db.cleandata.create_index([ ("id", -1) ])
collection_clean = db.cleandata

############################## fixing historical series main part ##################################

# defining the database name and the collection name
db = "index"
collection = "rawdata"

for Crypto in Crypto_Asset:

    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
        
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]

            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange" : exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(db, collection, query_dict)
            matrix = matrix.drop(columns = ['Exchange', 'Pair', 'Low', 'High','Open'])

            # checking if the matrix is not empty
            if matrix.shape[0] > 1:

                # check if the historical series start at the same date as the stert date
                # if not fill the dataframe with zero values
                matrix = data_setup.homogenize_series(matrix, reference_date_vector)

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:
                    
                    matrix = data_setup.CW_series_fix_missing(matrix, exchange, Crypto, pair, start_date)

            # add exchange and currency_pair column
            matrix['Exchange'] = exchange
            matrix['Pair'] = cp
            # put the manipulated data on MongoDB
            data = matrix.to_dict(orient='records')  
            collection_clean.insert_many(data)




