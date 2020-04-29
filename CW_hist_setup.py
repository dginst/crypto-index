######################################################################################################
# The file completes the historical series of Cryptocurrencies market data stored on MongoDB
# The main rules for the manipulation of raw data are the followings:
# - if a certain Crypto-Fiat pair does not start at the beginning of the period but later, the file 
#   will put a series of zeros from the start period until the actual beginning of the series
# - if a certain data is missing in a certain date the file will compute a value to insert using
#   all the values displayed, for the same Crypto-Fiat pair, in the other exchanges.
# - if, trying to fix a series as described above, the code find out that just one exchange has the
#   values for the wanted Crypto-Fiat pair, the file will put a 0-values array for all the missing date
# Once the data is manipulated and the series has been homogeineized, the file will save the the
# historical series on MongoDB in the collection "cleandata"
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

start = time.time()
####################################### initial settings ############################################

start_date = '01-01-2016'
#end_date = '03-01-2020'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)
#reference_date_vector = data_setup.timestamp_to_str(reference_date_vector)

pair_array = ['gbp', 'usd', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
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
db.cleandata.drop()
db.volume_checked_data.drop()

#creating the empty collection "cleandata" within the database index
db.cleandata.create_index([ ("id", -1) ])
collection_clean = db.cleandata

#creating the empty collection "volume_checked_data" within the database index
db.volume_checked_data.create_index([ ("id", -1) ])
collection_volume = db.volume_checked_data

# defining the database name and the collection name where to look for data
db = "index"
collection_raw = "rawdata"
collection_volume_check = "volume_checked_data"

############################### fixing the "Pair Volume" information ##############################

for Crypto in Crypto_Asset:

    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
        
        for cp in currencypair_array:

            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange" : exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(db, collection_raw, query_dict)
            matrix = matrix.drop(columns = ['Low', 'High', 'Open'])
            # checking if the matrix is not empty
            if matrix.shape[0] > 1: 
                
                if (exchange == 'bittrex' and cp == 'btcusdt'):

                    sub_vol = np.array(matrix.loc[matrix.Time == 1544486400, 'Crypto Volume'])
                    matrix.loc[matrix.Time == 1544572800, 'Crypto Volume'] = sub_vol
                    matrix.loc[matrix.Time == 1544659200, 'Crypto Volume'] = sub_vol

                matrix['Pair Volume'] = matrix['Close Price'] * matrix ['Crypto Volume']

            # put the manipulated data on MongoDB
            data = matrix.to_dict(orient='records')  
            collection_volume.insert_many(data)


############################## fixing historical series main part ##################################

print('SECCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOMMMMMMMMMMSDASASDASASASDASD')

for Crypto in Crypto_Asset:

    print(Crypto)
    currencypair_array = []

    for i in pair_array:

        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
        
        print(exchange)
        for cp in currencypair_array:

            print(cp)
            crypto = cp[:3]
            pair = cp[3:]

            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange" : exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(db, collection_volume_check, query_dict)
            matrix = matrix.drop(columns = ['Exchange', 'Pair'])
            # checking if the matrix is not empty
            if matrix.shape[0] > 1:

                # check if the historical series start at the same date as the stert date
                # if not fill the dataframe with zero values
                matrix = data_setup.homogenize_series(matrix, reference_date_vector)

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:
                    
                    matrix = data_setup.CW_series_fix_missing(matrix, exchange, Crypto, pair, start_date)

            ######### part that transform the timestamped date into string ###########
            new_date = np.array([])
            for element in matrix['Time']:

                element = str(element)
                new_date = np.append(new_date, element)

            matrix['Time'] = new_date
            ########################################################################

            # add exchange and currency_pair column
            matrix['Exchange'] = exchange
            matrix['Pair'] = cp
            # put the manipulated data on MongoDB
            data = matrix.to_dict(orient='records')  
            collection_clean.insert_many(data)


end = time.time()

print("This script took: {} seconds".format(float(end-start)))