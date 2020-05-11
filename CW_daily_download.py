# standard library import
import time
from datetime import datetime

# third party import
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo


####################################### initial settings ############################################

# set start_period (ever)
Start_Period = '01-01-2016'
# set today
today = datetime.now().strftime('%Y-%m-%d')

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
# pair complete = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP',
                'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp',
             'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index

# naming the existing rawdata collection as a variable
collection_raw = db.CW_rawdata


############################################# converted_data check ###########################################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]

# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5):len(date_complete)]

# defining the MongoDB path where to look for the rates
database = 'index'
collection = 'CW_rawdata'
query = {'Exchange': "coinbase-pro", 'Pair': "ethusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo2(database, collection, query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5):len(date_list)]
last_five_days_mongo = [str(single_date)
                        for single_date in last_five_days_mongo]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_add = data_setup.Diff(last_five_days, last_five_days_mongo)

if date_to_add != []:

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = data_setup.timestamp_to_human(
            [date_to_add[0]], date_format='%m-%d-%Y')
        start_date = start_date[0]
        end_date = data_setup.timestamp_to_human(
            [date_to_add[len(date_to_add) - 1]], date_format='%m-%d-%Y')
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime('%m-%d-%Y')
        end_date = start_date

    ########################### download part #############

    for Crypto in Crypto_Asset:

        currencypair_array = []
        for i in pair_array:
            currencypair_array.append(Crypto.lower() + i)

        for exchange in Exchanges:

            for cp in currencypair_array:

                crypto = cp[:3]
                pair = cp[3:]
                # create the matrix for the single currency_pair connecting to CryptoWatch website
                data_download.CW_raw_to_mongo(
                    exchange, cp, collection_raw, str(start_date))
else:

    print('Message: No new date to download from CryptoWatch, the rawdata collection on MongoDB is updated.')
