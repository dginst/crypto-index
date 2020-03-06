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

Start_Period = '2018-12-31'
today = datetime.now().strftime('%Y-%m-%d')
End_Period = today

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']



####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.ecb_raw.drop()
#creating the empty collection rawdata within the database index
db.ecb_raw.create_index([ ("id", -1) ])
collection_ECB_raw = db.ecb_raw

######################################## ECB rates raw data download ###################################

# create an array of date containing the list of date to download
date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N', EoD = 'N')


Exchange_Rate_List = pd.DataFrame()
   
for i, single_date in enumerate(date):

    # retrieving data from ECB website
    single_date_ex_matrix = data_download.ECB_rates_extractor(key_curr_vector, date[i])
    # put a sllep time in order to do not overuse API connection
    time.sleep(0.05)

    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix

    else:
        Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort=True)

########################## upload the raw data to MongoDB ############################

data = Exchange_Rate_List.to_dict(orient = 'records')  

collection_ECB_raw.insert_many(data)
