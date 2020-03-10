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

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC'] 
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.converted_data.drop()

#creating the empty collection cleandata within the database index
db.converted_data.create_index([ ("id", -1) ])
collection_converted = db.converted_data

############################## data conversion main part ##################################

# defining the database name and the collection name
db = "index"
collection_data = "cleandata"
collection_rates = "ecb_clean"

# field of conversion 
conv_fields = ['Close Price', 'Pair Volume']

for date in reference_date_vector:

    for fiat in pair_array:
        
        if fiat != 'usd':

            fiat = fiat.upper()
            ex_rate = fiat + '/USD'
            # defining the dictionary for the MongoDB query   
            query_dict_rate = {"Currency": ex_rate, "Date": str(date)}

            # retriving the needed information on MongoDB
            matrix_rate = mongo.query_mongo(db, collection_rates, query_dict_rate)
            # finding the conversion rate
            conv_rate = np.array(matrix_rate['Rate'])

        currencypair_array = []
        
        for Crypto in Crypto_Asset:

            currencypair_array.append(Crypto.lower() + fiat.lower())
        
        for cp in currencypair_array:

            # defining the dictionary for the MongoDB query
            query_dict_data = {"Pair": cp, "Time": str(date)}
            
            # retriving the needed information on MongoDB
            matrix_data = mongo.query_mongo(db, collection_data, query_dict_data)
            
            try:

                if fiat != 'usd':

                    # converting the values
                    matrix_data['Close Price'] = matrix_data['Close Price'] / conv_rate
                    matrix_data['Pair Volume'] = matrix_data['Pair Volume'] / conv_rate

                else:

                    matrix_data = matrix_data
            
                # put the manipulated data on MongoDB
                conv_data = matrix_data.to_dict(orient='records')  
                collection_converted.insert_many(conv_data)
            
            except TypeError:

                pass



