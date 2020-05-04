######################################################################################################
# The file retrieves data from MongoDB collection "cleandata" and, for each Crypto-Fiat historical 
# series, converts the data into USD values using the ECB manipulated rates stored on MongoDB in 
# the collection "ecb_clean"
# Once everything is converted into USD the historical series is saved into MongoDB in the collection
# "converted_data"
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
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.mongo_setup as mongo

####################################### initial settings ############################################

start_date = '01-01-2016'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)

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
db.converted_data.drop()

#creating the empty collection cleandata within the database index
db.converted_data.create_index([ ("id", -1) ])
collection_converted = db.converted_data

############################## data conversion main part ##################################

# defining the database name and the collection name
db = "index"
collection_data = "CW_cleandata"
collection_rates = "ecb_clean"

# queryng the two dataframe
matrix_rate = mongo.query_mongo2(db, collection_rates)
matrix_data = mongo.query_mongo2(db, collection_data)


# field of conversion 
conv_fields = ['Close Price', 'Pair Volume']

for date in reference_date_vector[0:len(reference_date_vector) - 1]:

    print(date)
    date_rate = matrix_rate.loc[matrix_rate.Date == str(date)]
    #date_data = matrix_data.loc[matrix_data.Time == str(date)]
    date_matrix = matrix_data.loc[matrix_data.Time == str(date)]

    for fiat in pair_array:
        
        if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

            fiat = fiat.upper()
            ex_rate = fiat + '/USD'  

            # finding the conversion rate
            conv_rate = date_rate.loc[date_rate.Currency == ex_rate, ['Rate']]
            conv_rate = np.array(conv_rate)

        currencypair_array = []
        
        for Crypto in Crypto_Asset:

            currencypair_array.append(Crypto.lower() + fiat.lower())
        
        for cp in currencypair_array:
            
            
            cp_matrix = date_matrix.loc[date_matrix.Pair == cp]

            try:

                if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

                    # converting the values
                    cp_matrix['Close Price'] = cp_matrix['Close Price'].div(float(conv_rate))
                    cp_matrix['Pair Volume'] = cp_matrix['Pair Volume'].div(float(conv_rate))

                else:

                    cp_matrix = cp_matrix

            
            except TypeError:

                pass
        

            try:

                converted_matrix = converted_matrix.append(cp_matrix)

            except:

                converted_matrix = cp_matrix


# adding a human-readable date format
standard_date = [datetime.fromtimestamp(int(element)) for element in converted_matrix['Time']]
standard_date = [element.strftime('%d-%m-%Y') for element in standard_date]

converted_matrix['Standard Date'] = standard_date

# put the converted data on MongoDB
conv_data = converted_matrix.to_dict(orient='records')  
collection_converted.insert_many(conv_data)