import utils.data_setup as data_setup
import utils.data_download as data_download
from pymongo import MongoClient
import time
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import pandas as pd
import requests
from requests import get
import mongoconn as mongo

start = time.time()


#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.ecb_raw.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_ECB_raw = db.ecb_raw

# this download all the ecb raw data
 ###########################################################################
Start_Period = '2015-05-01'
End_Period = '2016-02-17'
key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

   
date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
print(date[0])

Exchange_Rate_List = pd.DataFrame()
   
for i, single_date in enumerate(date):
    time.sleep(0.05)
    # retrieving data from ECB website
    single_date_ex_matrix = mongo.ECB_rates_extractor(key_curr_vector, date[i])

    if Exchange_Rate_List.size == 0:
        Exchange_Rate_List = single_date_ex_matrix
    else:
        Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort=True)

 ########################## import the raw ecb data to mongo

end = time.time()

print("This first part took: {} minutes".format(int((end-start)/60)))

data = Exchange_Rate_List.to_dict(orient='records')  
collection_ECB_raw.insert_many(data)

 ######################## query 

end = time.time()

print("This script took: {} minutes".format(int((end-start)/60)))

######################################### part to put on main to query all the raw data to clean them
database = 'index'
collection = 'ecb_raw'
query = {}

rates = mongo.query_mongo(database,collection, query)

print(rates.head())