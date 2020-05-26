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

# set start_period (ever)
Start_Period = '01-01-2016'
# set today
today = datetime.now().strftime('%Y-%m-%d')

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

# naming the existing converted_data collection as a variable
collection_final_data = db.EXC_final_data

#################################### defining the time vector for the check #############################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]
# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5): len(date_complete)]

############################################# EXC_final_datacheck ###########################################

# defining the MongoDB path where to look for the rates
database = 'index'
collection_final_check = 'EXC_final_data'
query_dict = {'Exchange': "coinbase-pro", 'Pair': "btcusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection_final_check, query_dict)

# checking the time column of the retrieved matrix
date_list = np.array(matrix["Time"])
# selecting the last days to check
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_convert = data_setup.Diff(last_five_days, last_five_days_mongo)

########################################### conversion ##############################################

# defining the collections name to query
collection_data = "EXC_cleandata"
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
                matrix_rate = mongo.query_mongo(
                    database, collection_rates, query_dict_rate)
                # finding the conversion rate
                conv_rate = np.array(matrix_rate['Rate'])

            currencypair_array = []

            for Crypto in Crypto_Asset:

                currencypair_array.append(Crypto.lower() + fiat.lower())

            for cp in currencypair_array:

                # defining the dictionary for the MongoDB query
                query_dict_data = {"Pair": cp, "Time": str(date)}

                # retriving the needed information on MongoDB
                matrix_data = mongo.query_mongo(
                    database, collection_data, query_dict_data)

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
                        standard = standard.strftime('%Y-%m-%d')
                        standard_date = np.append(standard_date, standard)

                    matrix_data['Standard Date'] = standard_date

                    # put the converted data on MongoDB
                    data = matrix_data.to_dict(orient='records')
                    collection_final_data.insert_many(data)

                except TypeError:

                    pass

else:

    print('Message: No new date to upload on on Mongo DB, the EXC_final_data collection on MongoDB is updated.')

# ################# DATA CONVERSION MAIN PART ##################

start = time.time()
# defining the database name and the collection name
db = "index"
collection_data = "EXC_cleandata"
collection_rates = "ecb_clean"
collection_stable = 'stable_coin_rates'

# querying the data from mongo
matrix_rate = mongo.query_mongo2(db, collection_rates)
matrix_rate = matrix_rate.rename({'Date': 'Time'}, axis='columns')
matrix_rate = matrix_rate.loc[matrix_rate.Time.isin(date_array)]
matrix_data = mongo.query_mongo2(db, collection_data)
matrix_rate_stable = mongo.query_mongo2(db, collection_stable)
matrix_rate_stable = matrix_rate_stable.loc[matrix_rate_stable.Time.isin(
    date_array)]

# creating a column containing the fiat currency
matrix_rate['fiat'] = [x[:3].lower() for x in matrix_rate['Currency']]
matrix_data['fiat'] = [x[3:].lower() for x in matrix_data['Pair']]
matrix_rate_stable['fiat'] = [x[:4].lower()
                              for x in matrix_rate_stable['Currency']]

# ############ creating a USD subset which will not be converted #########

usd_matrix = matrix_data.loc[matrix_data['fiat'] == 'usd']
usd_matrix = usd_matrix[['Time', 'Close Price',
                         'Crypto Volume', 'Pair Volume', 'Exchange', 'Pair']]

# ########### converting non-USD fiat currencies #########################

# creating a matrix for conversion
conv_fiat = ['gbp', 'eur', 'cad', 'jpy']
conv_matrix = matrix_data.loc[matrix_data['fiat'].isin(conv_fiat)]

# merging the dataset on 'Time' and 'fiat' column
conv_merged = pd.merge(conv_matrix, matrix_rate, on=['Time', 'fiat'])

# converting the prices in usd
conv_merged['Close Price'] = conv_merged['Close Price'] / conv_merged['Rate']
conv_merged['Close Price'] = conv_merged['Close Price'].replace(
    [np.inf, -np.inf], np.nan)
conv_merged['Close Price'].fillna(0, inplace=True)
conv_merged['Pair Volume'] = conv_merged['Pair Volume'] / conv_merged['Rate']
conv_merged['Pair Volume'] = conv_merged['Pair Volume'].replace(
    [np.inf, -np.inf], np.nan)
conv_merged['Pair Volume'].fillna(0, inplace=True)


# subsetting the dataset with only the relevant columns
conv_merged = conv_merged[['Time', 'Close Price',
                           'Crypto Volume', 'Pair Volume', 'Exchange', 'Pair']]

# ############## converting STABLECOINS currencies #########################

# creating a matrix for stablecoins
stablecoin = ['usdc', 'usdt']
stablecoin_matrix = matrix_data.loc[matrix_data['fiat'].isin(stablecoin)]

# merging the dataset on 'Time' and 'fiat' column
stable_merged = pd.merge(
    stablecoin_matrix, matrix_rate_stable, on=['Time', 'fiat'])

# converting the prices in usd
stable_merged['Close Price'] = stable_merged['Close Price'] / \
    stable_merged['Rate']
stable_merged['Close Price'] = stable_merged['Close Price'].replace(
    [np.inf, -np.inf], np.nan)
stable_merged['Close Price'].fillna(0, inplace=True)
stable_merged['Pair Volume'] = stable_merged['Pair Volume'] / \
    stable_merged['Rate']
stable_merged['Pair Volume'] = stable_merged['Pair Volume'].replace(
    [np.inf, -np.inf], np.nan)
stable_merged['Pair Volume'].fillna(0, inplace=True)

# subsetting the dataset with only the relevant columns
stable_merged = stable_merged[['Time', 'Close Price',
                               'Crypto Volume', 'Pair Volume',
                               'Exchange', 'Pair']]

# reunite the dataframes and put data on MongoDB
converted_data = conv_merged
converted_data = converted_data.append(stable_merged)
converted_data = converted_data.append(usd_matrix)

data = converted_data.to_dict(orient='records')
collection_converted.insert_many(data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
