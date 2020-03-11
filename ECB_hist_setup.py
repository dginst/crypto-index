######################################################################################################
# The file aims to complete the historical series of European Central Bank Websites exchange rates.
# It retrieves the rates from MongoDB in the database "index" and collection "ecb_raw" then add values
# for all the holidays and weekends simply copiyng the value of the last day with value. 
# Morover the file takes the rates as EUR based exchange rates and returns USD based exchange rates.
# The completed USD based historical series is saved back in MongoDb in the collection "ecb_clean"
# is possible to change the period of downlaod modifying the "Start_Period"
#######################################################################################################

# standard import 
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
# local import
import mongoconn as mongo
import utils.data_setup as data_setup
import utils.data_download as data_download

####################################### initial settings ############################################

Start_Period = '12-31-2018'

# set today as End_period
End_Period = datetime.now().strftime('%m-%d-%Y')

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# drop the pre-existing collection (if there is one)
db.ecb_clean.drop()

#creating the empty collection rawdata within the database index
db.ecb_clean.create_index([ ("id", -1) ])
collection_ECB_clean = db.ecb_clean

###################################### ECB rates manipulation ###################################

# makes the raw data clean through the ECB_setup function
mongo_clean = data_setup.ECB_setup(key_curr_vector, Start_Period, End_Period)

######### part that transform the timestamped date into string ###########
new_date = np.array([])

for element in mongo_clean['Date']:

    element = str(element)
    new_date = np.append(new_date, element)

mongo_clean['Date'] = new_date
########################################################################

########################## upload the manipulated data in MongoDB ############################

# upload the data in ecb_clean collection

data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)
