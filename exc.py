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
import difflib

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
ciao = 'ethusdt'
print(ciao[3:])
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
start = time.time()
# defining the database name and the collection name
db = "index"
collection_data = "CW_cleandata"
collection_rates = "ecb_clean"

matrix_rate = mongo.query_mongo2(db, collection_rates)
matrix_data = mongo.query_mongo2(db, collection_data)
######################################################
############# alternativa #########################
# leave out USD pair
matrix_data['fiat'] = matrix_data['Pair'].str[3:]
fiat_to_conv = ['gbp', 'eur', 'cad', 'jpy']
fiat_no_conv = ['usd', 'usdt', 'usdc']
matrix_data = matrix_data.loc[matrix_data['fiat'].isin(fiat_to_conv)]
matrix_no_conv = matrix_data.loc[matrix_data['fiat'].isin(fiat_no_conv)]

matrix_rate['key'] = matrix_rate['Currency'].str[:3].lower() + matrix_rate['Time']
matrix_data['key'] = matrix_data['Pair'].str[3:] + matrix_rate['Time']

new_df = matrix_data.merge(matrix_rate, on = 'key',  how = 'left')
new_df['Close Price'] = new_df['Close Price'] / new_df['Rate']
print(new_df)
print(new_df.info())
# rearrange matrix
new_df = new_df.drop(columns = ['key'])
final_matrix = new_df.append(matrix_no_conv)
final_matrix = final_matrix.drop(columns = ['fiat'])
###################################################
# queryng the two dataframe

matrix_rate['a'] = [x[:3].lower() for x in matrix_rate['Currency'] ]
print(matrix_rate['a'])
matrix_data['a'] = [x[3:].lower() for x in matrix_data['Pair'] ]
print(matrix_data['a'])
df = pd.merge(matrix_data,matrix_rate, on = ['Time','a'])

df = df['Close Price'] * df['Rate']

print(df.info())
##########################################
# first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usd']
# print(first_conv_data.info())
# first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usdc']
# print(first_conv_data.info())
# first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usdt']
# print(first_conv_data.info())




# df['Currency'] = df['Currency'].apply(lambda x: x.lower())

# df = df.loc[df['Pair'].str[3:] == df['Currency'].str[:3]]

# df['Close Price'] = df['Close Price'] * df['Rate']


        
   
# df['Close Price'] = df['Close Price'] * df['Rate']




end = time.time()

print("This script took: {} seconds".format(float(end-start)))