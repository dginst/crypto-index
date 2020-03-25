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

# set start_period (ever)
Start_Period = '01-01-2016'
# set today
today = datetime.now().strftime('%Y-%m-%d')

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC'] 
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index

# naming the existing converted_data collection as a variable
collection_converted = db.converted_data

############################################# converted_data check ###########################################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]

# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5) : len(date_complete)]


# date_complete = data_setup.timestamp_to_str(date_complete)
# defining the MongoDB path where to look for the rates
database = 'index'
collection = 'converted_data'
query = {'Exchange': "coinbase-pro", 'Pair': "ethusd"} 

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection, query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5) : len(date_list)]

# finding the date to download as difference between complete array of date and 
# date now stored on MongoDB
date_to_convert = data_setup.Diff(last_five_days, last_five_days_mongo)

######################################## conversion ###################################

# defining the database name and the collection name
db = "index"
collection_data = "cleandata"
collection_rates = "ecb_clean"

# field of conversion 
conv_fields = ['Close Price', 'Pair Volume']

for date in date_to_convert:

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

                # adding a human-readable date format
                standard_date = np.array([])

                for element in matrix_data['Time']:

                    standard = datetime.fromtimestamp(int(element))
                    standard = standard.strftime('%d-%m-%Y')
                    standard_date = np.append(standard_date, standard)

                matrix_data['Standard Date'] = standard_date

                # put the converted data on MongoDB
                conv_data = matrix_data.to_dict(orient='records')  
                collection_converted.insert_many(conv_data)
            
            except TypeError:

                pass
