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
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

####################################### setup MongoDB connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.converted_data.drop()
db.CW_final_data.drop()
db.stable_coin_rates.drop()

#creating the empty collection cleandata within the database index
db.CW_final_data.create_index([("id", -1)])
db.converted_data.create_index([("id", -1)])
db.stable_coin_rates.create_index([("id", -1)])
collection_stable = db.stable_coin_rates
collection_final_data = db.CW_final_data
collection_converted = db.converted_data

########################## USDC/USD and USDT/USD computation ###########################
database = "index"
collection = "CW_cleandata"

# USDT exchange rates computation 

# kraken usdt/usd exchange rate

query_usd = {'Exchange': 'kraken', 'Pair': 'btcusd'}
query_usdt = {'Exchange': 'kraken', 'Pair': 'btcusdt'}
usdt_kraken = mongo.query_mongo2(database, collection, query_usdt)
usd_kraken = mongo.query_mongo2(database, collection, query_usd)

# computing the rate
usdt_kraken['rate'] = usdt_kraken['Close Price'] / usd_kraken['Close Price']

# composing a vector that allows to exclude days where the stablecoin-usd pair does not exist
existence_check = np.array([])
for x in usdt_kraken['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)

usdt_kraken['tot_vol'] = (usdt_kraken['Crypto Volume'] + usd_kraken['Crypto Volume']) * existence_check

# bittrex usdt/usd exchange rate 

query_usd = {'Exchange' : 'bittrex', 'Pair' : 'btcusd'}
query_usdt = {'Exchange' : 'bittrex','Pair' : 'btcusdt'}

usdt_bittrex = mongo.query_mongo2(database, collection, query_usdt)
usd_bittrex = mongo.query_mongo2(database, collection, query_usd)

# computing the rate
usdt_bittrex['rate'] = usdt_bittrex['Close Price'] / usd_bittrex['Close Price']

# composing a vector that allows to exclude days where the stablecoin-usd pair does not exist
existence_check = np.array([])
for x in usdt_bittrex['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)

usdt_bittrex.fillna(0, inplace=True)
usdt_bittrex['tot_vol'] = (usdt_bittrex['Crypto Volume'] + usd_bittrex['Crypto Volume']) * existence_check

# usdt rates weighted average computation

kraken_weighted = usdt_kraken['rate'] * usdt_kraken['tot_vol']
bittrex_weighted = usdt_bittrex['rate'] * usdt_bittrex['tot_vol']
total_weights = usdt_kraken['tot_vol'] + usdt_bittrex['tot_vol']
usdt_rates = (kraken_weighted + bittrex_weighted) / total_weights
usdt_rates = 1 / usdt_rates
usdt_rates = pd.DataFrame(usdt_rates, columns=['Rate'])
usdt_rates = usdt_rates.replace([np.inf, -np.inf], np.nan)
usdt_rates.fillna(0, inplace=True)

usdt_rates['Currency'] = np.zeros(len(usdt_rates['Rate']))
usdt_rates['Currency'] = [str(x).replace('0', 'USDT/USD') for x in usdt_rates['Currency']]
usdt_rates['Time'] = usd_bittrex['Time']
usdt_rates['Standard Date'] = data_setup.timestamp_to_human(usd_bittrex['Time'])

# df = df.rename({'Time': 'Date'}, axis='columns')

# USDT mongoDB upload
usdt_data = usdt_rates.to_dict(orient='records')  
collection_stable.insert_many(usdt_data)

######################### USDC exchange rates computation ################################

# kraken usdc/usd exchange rate 

query_usdc = {'Exchange': 'kraken', 'Pair': 'btcusdc'}
usdc_kraken = mongo.query_mongo2(database, collection, query_usdc)

usdc_kraken['rate'] = usdc_kraken['Close Price'] / usd_kraken['Close Price']

existence_check = np.array([])

for x in usdc_kraken['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)


usdc_kraken['tot_vol'] = (usdc_kraken['Crypto Volume'] + usd_kraken['Crypto Volume']) * existence_check

# coinbase usdc exchange rate 

query_usd_coinbase = {'Exchange': 'coinbase-pro', 'Pair': 'btcusd'}
query_usdc_coinbase = {'Exchange': 'coinbase-pro', 'Pair': 'btcusdc'}

usdc_coinbase = mongo.query_mongo2(database, collection, query_usdc_coinbase)
usd_coinbase = mongo.query_mongo2(database, collection, query_usd_coinbase)


usdc_coinbase['rate'] = usdc_coinbase['Close Price'] / usd_coinbase['Close Price']

existence_check = np.array([])

for x in usdc_coinbase['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)

usdc_coinbase.fillna(0, inplace=True)
usdc_coinbase['tot_vol'] = (usdc_coinbase['Crypto Volume'] + usd_coinbase['Crypto Volume']) * existence_check

# usdc rates weighted average computation

kraken_weighted = usdc_kraken['rate'] * usdc_kraken['tot_vol']
coinbase_weighted = usdc_coinbase['rate'] * usdc_coinbase['tot_vol']
total_weights = usdc_kraken['tot_vol'] + usdc_coinbase['tot_vol']
usdc_rates = (kraken_weighted + coinbase_weighted) / total_weights
usdc_rates = 1 / usdc_rates
usdc_rates = pd.DataFrame(usdc_rates, columns=['Rate'])
usdc_rates = usdc_rates.replace([np.inf, -np.inf], np.nan)
usdc_rates.fillna(0, inplace=True)

usdc_rates['Currency'] = np.zeros(len(usdc_rates['Rate']))
usdc_rates['Currency'] = [str(x).replace('0', 'USDC/USD') for x in usdc_rates['Currency']]
usdc_rates['Time'] = usd_coinbase['Time']
usdc_rates['Standard Date'] = data_setup.timestamp_to_human(usd_coinbase['Time'])

# USDC mongoDB upload

usdc_data = usdc_rates.to_dict(orient='records')
collection_stable.insert_many(usdc_data)

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
                data = matrix_data.to_dict(orient='records')  
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
