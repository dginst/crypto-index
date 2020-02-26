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

Start_Period = '2016-02-15'
End_Period = '2016-04-17'

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']



####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.ecb_raw.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_ECB_raw = db.ecb_raw

######################################## ECB rates raw data download ###################################

# create an array of date containing the list of date to download
date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')


Exchange_Rate_List = pd.DataFrame()
   
for i, single_date in enumerate(date):

    # retrieving data from ECB website
    single_date_ex_matrix = mongo.ECB_rates_extractor(key_curr_vector, date[i])
    # put a sllep time in order to do not overuse API connection
    time.sleep(0.05)

    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix
    else:
        Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort=True)

########################## upload the raw data to MongoDB ############################

data = Exchange_Rate_List.to_dict(orient='records')  

collection_ECB_raw.insert_many(data)
