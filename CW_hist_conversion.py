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

start = time.time()

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
db.CW_final_data.drop()

#creating the empty collection cleandata within the database index
db.CW_final_data.create_index([ ("id", -1) ])
db.converted_data.create_index([ ("id", -1) ])
collection_final_data = db.CW_final_data
collection_converted = db.converted_data

############################## data conversion main part ##################################

# defining the database name and the collection name
db_name = "index"
collection_data = "CW_cleandata"
collection_rates = "ecb_clean"

# field of conversion 
conv_fields = ['Close Price', 'Pair Volume']

for date in reference_date_vector:

    for fiat in pair_array:
        print(fiat)
        if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

            fiat = fiat.upper()
            ex_rate = fiat + '/USD'
            # defining the dictionary for the MongoDB query   
            query_dict_rate = {"Currency": ex_rate, "Date": str(date)}
            # retriving the needed information on MongoDB
            matrix_rate = mongo.query_mongo(db_name, collection_rates, query_dict_rate)
            # finding the conversion rate
            conv_rate = np.array(matrix_rate['Rate'])

        currencypair_array = []
        
        for Crypto in Crypto_Asset:

            currencypair_array.append(Crypto.lower() + fiat.lower())
        
        for cp in currencypair_array:

            # defining the dictionary for the MongoDB query
            query_dict_data = {"Pair": cp, "Time": str(date)}
            
            # retriving the needed information on MongoDB
            matrix_data = mongo.query_mongo(db_name, collection_data, query_dict_data)
            
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

                # put the manipulated data on MongoDB
                data = matrix_data.to_dict(orient = 'records')  
                collection_converted.insert_many(data)
            
            except TypeError:

                pass

################################### zero volume values fixing part #################################

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

# deleting unuseful collection

db.converted_data.drop()
end = time.time()

print("This script took: {} seconds".format(float(end-start)))
