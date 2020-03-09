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
today = datetime.now().strftime('%m-%d-%Y')
End_Period = today

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# drop the pre-existing collection (if there is one)
db.ecb_clean.drop()

#creating the empty collection rawdata within the database index
db.ecb_clean.create_index([ ("id", -1) ])
collection_ECB_clean = db.ecb_clean

# makes the raw data clean through the ECB_setup function
mongo_clean = data_setup.ECB_setup(key_curr_vector, Start_Period, End_Period)

######### part that transform the timestamped date into string ###########
new_date = np.array([])

for element in mongo_clean['Date']:

    element = str(element)
    print(element)
    new_date = np.append(new_date, element)

mongo_clean['Date'] = new_date
########################################################################

# upload the cleaned data in MongoDB
data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)
