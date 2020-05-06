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

# querying the data from mongo
matrix_rate = mongo.query_mongo2(db, collection_rates)
matrix_data = mongo.query_mongo2(db, collection_data)

# creating subsets only for the crypto/usd, crypto/usdc and crypto/usdt  pairs that don't need conversiont and storing them in mongo
first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usd']

first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usdc']

first_conv_data = matrix_data.loc[matrix_data['Pair'].str[3:] == 'usdt']



# creating an equal column in both datasets for merging

matrix_rate['fiat'] = [x[:3].lower() for x in matrix_rate['Currency'] ]
matrix_data['fiat'] = [x[3:].lower() for x in matrix_data['Pair'] ]


# merging the dataset

df = pd.merge(matrix_data,matrix_rate, on = ['Time','fiat'])

# converting the prices in usd

df['Close Price'] = df['Close Price'] * df['Rate']

# subsetting the dataset with only the relevant columns

df = df[['Time', 'Close Price', 'Crypto Volume', 'Standard Date']]

#storing the prices in mongo 

print(df.info())
print(df.head())
##########################################



end = time.time()

print("This script took: {} seconds".format(float(end-start)))