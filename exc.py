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
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo

start = time.time()

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
collection_stable = 'stable_coin_rates'

# querying the data from mongo
matrix_rate = mongo.query_mongo2(db, collection_rates)
matrix_data = mongo.query_mongo2(db, collection_data)
matrix_rate_stable = mongo.query_mongo2(db, collection_stable)

# creating a column containing the fiat currency 
matrix_rate['fiat'] = [x[:3].lower() for x in matrix_rate['Currency']]
matrix_data['fiat'] = [x[3:].lower() for x in matrix_data['Pair']]
matrix_rate_stable['fiat'] = [x[3:].lower() for x in matrix_rate_stable['Currency']]

# creating a matrix for usd
usd_matrix = matrix_data.loc[matrix_data['fiat'] == 'usd']
usd_matrix = usd_matrix[['Time', 'Close Price', 'Crypto Volume', 'Standard Date']]

## converting non-usd fiat currencies ##

# creating a matrix for conversion
conv_fiat = ['gbp', 'eur', 'cad', 'jpy']
conv_matrix = matrix_data.loc[matrix_data['fiat'].isin(conv_fiat)]

# merging the dataset on 'Time' and 'fiat' column
conv_merged = pd.merge(conv_matrix, matrix_rate, on=['Time', 'fiat'])

# converting the prices in usd
conv_merged['Close Price'] = conv_merged['Close Price'] * conv_merged['Rate']

# subsetting the dataset with only the relevant columns
conv_merged = conv_merged[['Time', 'Close Price', 'Crypto Volume', 'Standard Date']]

## converting stablecoins currencies ##

# creating a matrix for stablecoins
stablecoin = ['usdc', 'usdt']
stablecoin_matrix = matrix_data.loc[matrix_data['fiat'].isin(stablecoin)]

# merging the dataset on 'Time' and 'fiat' column
stable_merged = pd.merge(stablecoin_matrix, matrix_rate_stable, on=['Time', 'fiat'])

# converting the prices in usd
stable_merged['Close Price'] = stable_merged['Close Price'] * conv_merged['Rate']

# subsetting the dataset with only the relevant columns
stable_merged = stable_merged[['Time', 'Close Price', 'Crypto Volume', 'Standard Date']]


# reunite the dataframes and put data on MongoDB

converted_data = conv_merged
converted_data = converted_data.append(stable_merged)
converted_data = converted_data.append(usd_matrix)

#storing the prices in mongo 

print(df.info())
print(df.head())
##########################################



end = time.time()

print("This script took: {} seconds".format(float(end-start)))