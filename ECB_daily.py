# standard library import
import sys
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
import numpy as np
from pymongo import MongoClient
import pandas as pd

# local import
import utils.data_setup as data_setup
import utils.data_download as data_download
import utils.mongo_setup as mongo

####################################### initial settings ############################################

# set start_period (ever)
Start_Period = '2015-12-31'
# set today
today = datetime.now().strftime('%Y-%m-%d')


key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

####################################### setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index

# naming the exixting ecb_raw collection as a variable
collection_ECB_raw = db.ecb_raw

############################################# ecb_raw check ###########################################

# defining the array containing all the date from start_period until today
date_complete = data_setup.timestamp_gen(start_date)
date_complete = data_setup.timestamp_to_str(date_complete)
# defining the MongoDB path where to look for the rates
database = 'index'
collection = 'ecb_raw'
query = {'CURRENCY': "USD"} 

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection, query)

# checking the time column
date_list = np.array(matrix["TIME PERIOD"])

# finding the date to download as difference between complete array of date and 
# date now stored on MongoDB
date_to_download = data_setup.Diff(date_complete, date_list)



######################################## ECB rates raw data download ###################################

Exchange_Rate_List = pd.DataFrame()
   
for i, single_date in enumerate(date_to_download):

    # retrieving data from ECB website
    single_date_ex_matrix = data_download.ECB_rates_extractor(key_curr_vector, date_to_download[i])
    # put a sleep time in order to do not overuse API connection
    time.sleep(0.05)

    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix

    else:
        Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort = True)

########################## upload the raw data to MongoDB ############################

data = Exchange_Rate_List.to_dict(orient = 'records')  
print(data)
collection_ECB_raw.insert_many(data)