# standard library import
import os.path
from pathlib import Path
import json
from datetime import datetime
import cryptoindex.calc as calc
from datetime import *
import time

# third party import
from pymongo import MongoClient
import numpy as np
import pandas as pd

# local import
import cryptoindex.mongo_setup as mongo
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download

############################################# INITIAL SETTINGS #############################################

pair_array = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
#############################################################################################################

####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index


# drop the pre-existing collection (if there is one)
db.CW_final_data.drop()

# collection for index weights
db.CW_final_data.create_index([ ("id", -1) ])
collection_final_data = db.CW_final_data


# define database name and collection name
db_name = "index"
collection_converted_data = "converted_data"

for Crypto in Crypto_Asset:

    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
        
        for cp in currencypair_array:

            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange" : exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(db_name, collection_converted_data, query_dict)
            # checking if the matrix is not empty
            try:

                if matrix.shape[0] > 1:

                    matrix = data_setup.fix_zero_value(matrix)


                # put the manipulated data on MongoDB
                data = matrix.to_dict(orient='records')  
                collection_final_data.insert_many(data)
            
            except AttributeError:
                pass