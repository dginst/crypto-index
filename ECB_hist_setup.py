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

Start_Period = '12-31-2018'
End_Period = '02-28-2020'

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

#connecting to mongo in local
connection = MongoClient('localhost', 27017)

db = connection.index
db.ecb_clean.create_index([ ("id", -1) ])

#creating the empty collection rawdata within the database index
collection_ECB_clean = db.ecb_clean

# makes the raw data clean through the ECB_setup function
mongo_clean = data_setup.ECB_setup(key_curr_vector, Start_Period, End_Period)

# correct the date in order to obtain a timestamp date UTC 12:00
mongo_clean['Date'] = pd.to_datetime(mongo_clean['Date'])
mongo_clean['Date'] = (mongo_clean['Date'] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
new_date = np.array([])
for element in mongo_clean['Date']:

    element = str(element)
    new_date = np.append(new_date, element)

mongo_clean['Date'] = new_date
# upload the cleaned data in MongoDB
data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)
