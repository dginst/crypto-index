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

pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
# pair complete = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC'] 
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index

# naming the existing converted_data collection as a variable
collection_converted = db.converted_data
collection_final_data = db.CW_final_data

#################################### defining the time vector for the check #############################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]
# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5) : len(date_complete)]

############################################# converted_data check ###########################################

# defining the MongoDB path where to look for the rates
database = 'index'
collection_convert = 'converted_data'
query_dict = {'Exchange': "coinbase-pro", 'Pair': "ethusd"} 

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection_convert, query_dict)

# checking the time column of the retrieved matrix
date_list = np.array(matrix["Time"])
# selecting the last days to check
last_five_days_mongo = date_list[(len(date_list) - 5) : len(date_list)]

# finding the date to download as difference between complete array of date and 
# date now stored on MongoDB
date_to_convert = data_setup.Diff(last_five_days, last_five_days_mongo)

########################################### conversion ##############################################

# defining the collections name to query
collection_data = "cleandata"
collection_rates = "ecb_clean"

# field of conversion 
conv_fields = ['Close Price', 'Pair Volume']

if date_to_convert != []:

    for date in date_to_convert:

        for fiat in pair_array:
            
            if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

                fiat = fiat.upper()
                ex_rate = fiat + '/USD'
                # defining the dictionary for the MongoDB query   
                query_dict_rate = {"Currency": ex_rate, "Date": str(date)}

                # retriving the needed information on MongoDB
                matrix_rate = mongo.query_mongo(database, collection_rates, query_dict_rate)
                # finding the conversion rate
                conv_rate = np.array(matrix_rate['Rate'])

            currencypair_array = []
            
            for Crypto in Crypto_Asset:

                currencypair_array.append(Crypto.lower() + fiat.lower())
            
            for cp in currencypair_array:

                # defining the dictionary for the MongoDB query
                query_dict_data = {"Pair": cp, "Time": str(date)}
                
                # retriving the needed information on MongoDB
                matrix_data = mongo.query_mongo(database, collection_data, query_dict_data)
                
                # correcting the Pair Volume as Close Proce * Crypto Volume
                try:

                    matrix_data['Pair Volume'] = matrix_data['Close Price'] * matrix_data['Crypto Volume']

                except TypeError:

                    pass

                # conversion part in order to have only usd based values
                try:

                    if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

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

else:

    print('Message: No new date to upload on on Mongo DB, the converted_data collection on MongoDB is updated.')


######################################### CW_final_data check #########################################

# define collections where to look up
collection_final = "CW_final_data"

# defining the MongoDB path where to look for the rates
query_dict = {'Exchange': "coinbase-pro", 'Pair': "ethusd"}  

# retrieving data from MongoDB 'index' and "CW_final_data" collection
matrix = mongo.query_mongo(database, collection_final, query_dict)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5) : len(date_list)]

# finding the date to download as difference between complete array of date and 
# date now stored on MongoDB
date_to_convert = data_setup.Diff(last_five_days, last_five_days_mongo)

######################################## CW_final_data upload #####################################

if date_to_convert != []:

    for date in date_to_convert:

        query_dict = {"Time": date}
        # retriving the needed information on MongoDB
        matrix = mongo.query_mongo(database, collection_convert, query_dict)

        # put the manipulated data on MongoDB
        data = matrix.to_dict(orient = 'records')  
        collection_final_data.insert_many(data)


else:

    print('Message: No new date to upload on on Mongo DB, the CW_final_data collection on MongoDB is updated.')