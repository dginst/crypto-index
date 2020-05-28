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
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo


####################################### initial settings ############################################

# set start_period # aggiungere lo start, deve coincidere con la data di inzio dei ticker
Start_Period = '01-01-2016'
# set today
today = datetime.now().strftime('%Y-%m-%d')

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur']
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP',
                'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp',
             'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# naming the existing collections as a variable
collection_clean = db.EXC_cleandata

# defining the database name and the collection name where to look for data
database = "index"
collection_raw = "EXC_rawdata"
collection_clean_check = "EXC_cleandata"
collection_CW_raw = "CW_rawdata"

############################################# missing days check ###########################################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]

# searching only the last five days from the "date_complete" array
last_five_days = date_complete[(len(date_complete) - 5): len(date_complete)]

# defining the MongoDB path where to look for the rates
query = {'Exchange': "coinbase-pro", 'Pair': "btcusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo2(database, collection_clean_check, query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_add = data_setup.Diff(last_five_days, last_five_days_mongo)
print(date_to_add)

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

    relative_reference_vector = data_setup.timestamp_gen(
        start_date, end_date, EoD='N')

    ############################## fixing historical series main part ##################################


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
                query_dict = {"Exchange": exchange, "Pair": cp}
                # retrieving the needed information from rawdata collection on MongoDB
                matrix = mongo.query_mongo(
                    database, collection_raw, query_dict)
                matrix = matrix.drop(
                    columns=['Ticker_time', 'Date', 'Bid', 'Ask', 'Traded_id'])

                # selecting the date of interest
                matrix = matrix.loc[matrix['Time'].isin(
                    relative_reference_vector)]

                # checking if the matrix is not empty
                if matrix.shape[0] > 1:

                    matrix['Pair Volume'] = matrix['Crypto Volume'] * \
                        matrix['Close Price']

                # if the matrix is empty, the code searches the value in CW rawdata
                else:

                    # querying the rawdata from CW_rawdata collection looking for data
                    CW_matrix = mongo.query_mongo(
                        database, collection_CW_raw, query_dict)

                    # checking if the exchange-pair exists in CW_rawdata
                    if CW_matrix.shape[0] > 1:

                        CW_matrix.drop(columns=['Low', 'High', 'Open'])
                        # selecting the date of interest
                        int_ref_vector = [int(date)
                                          for date in relative_reference_vector]
                        CW_matrix = CW_matrix.loc[CW_matrix['Time'].isin(
                            int_ref_vector)]
                        CW_matrix['Pair Volume'] = CW_matrix['Crypto Volume'] * \
                            CW_matrix['Close Price']
                        # renaming CW_matrix
                        matrix = CW_matrix

                    # if it not exists code takes the day before values
                    else:

                        yesterday_matrix = mongo.query_mongo(
                            database, collection_clean_check, query_dict)
                        yesterday_date = [str(int(date) - 86400)
                                          for date in relative_reference_vector]
                        yesterday_matrix = yesterday_matrix.loc[yesterday_matrix['Time'].isin(
                            yesterday_date)]
                        # renaming yesterday_matrix and changing "Time" column values
                        matrix = yesterday_matrix
                        matrix['Time'] = [str(int(date) + 86400)
                                          for date in matrix['Time']]

                try:

                    # put the manipulated data on MongoDB
                    data = matrix.to_dict(orient='records')
                    collection_clean.insert_many(data)

                except:

                    pass

else:

    print('Message: No new date to fix, the EXC_cleandata collection on MongoDB is updated.')
