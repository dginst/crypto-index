# standard library import
import time
from datetime import datetime

# third party import
import numpy as np
from pymongo import MongoClient
import pandas as pd

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo

####################################### initial settings ############################################

# set start_period (ever)
Start_Period = '12-31-2015'
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
date_complete = data_setup.timestamp_gen(Start_Period)
# defining the array considering legal/solar hours
date_complete = data_setup.timestamp_gen_legal_solar(date_complete)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete]

# searching only the last five days
last_five_days = date_complete[(len(date_complete) - 5):len(date_complete)]


# date_complete = data_setup.timestamp_to_str(date_complete)
# defining the MongoDB path where to look for the rates
database = 'index'
collection = 'ecb_raw'
query = {'CURRENCY': "USD"} 

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection, query)

# checking the time column
date_list = np.array(matrix["TIME_PERIOD"])
last_five_days_mongo = date_list[(len(date_list) - 5):len(date_list)]

# finding the date to download as difference between complete array of date and 
# date now stored on MongoDB
date_to_download = data_setup.Diff(last_five_days, last_five_days_mongo)

# converting the timestamp into YYYY-MM-DD in order to perform the download from the ECB website
date_to_download = [datetime.fromtimestamp(int(date)) for date in date_to_download]
date_to_download = [date.strftime('%Y-%m-%d') for date in date_to_download]

######################################## ECB rates raw data download ###################################

Exchange_Rate_List = pd.DataFrame()
   
for single_date in date_to_download:

    # retrieving data from ECB website
    single_date_ex_matrix = data_download.ECB_rates_extractor(key_curr_vector, single_date)
    # put a sleep time in order to do not overuse API connection
    time.sleep(0.05)

    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix

    else:
        Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort=True)

########################## upload the raw data to MongoDB ############################

if Exchange_Rate_List.empty == True:

    print('Message: No new date to download, the ecb_raw collection on MongoDB is updated.')

else:

    data = Exchange_Rate_List.to_dict(orient='records')  
    collection_ECB_raw.insert_many(data)