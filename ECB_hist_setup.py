######################################################################################################
# The file aims to complete the historical series of European Central Bank Websites exchange rates.
# It retrieves the rates from MongoDB in the database "index" and collection "ecb_raw" then add values
# for all the holidays and weekends simply copiyng the value of the last day with value.
# Morover the file takes the rates as EUR based exchange rates and returns USD based exchange rates.
# The completed USD based historical series is saved back in MongoDb in the collection "ecb_clean"
# is possible to change the period of downlaod modifying the "Start_Period"
#######################################################################################################

# standard library import
from datetime import datetime

# third party import
from pymongo import MongoClient
import numpy as np

# local import
import cryptoindex.data_setup as data_setup

# initial settings ############################################

Start_Period = '12-31-2015'

# set today as End_period
End_Period = datetime.now().strftime('%m-%d-%Y')
# or
# End_Period = '03-01-2020'

key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

# setup mongo connection ###################################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
db = connection.index

# drop the pre-existing collection (if there is one)
db.ecb_clean.drop()

# creating the empty collection rawdata within the database index
db.ecb_clean.create_index([("id", -1)])
collection_ECB_clean = db.ecb_clean

# ECB rates manipulation ###################################

# makes the raw data clean through the ECB_setup function
try:

    mongo_clean = data_setup.ECB_setup(
        key_curr_vector, Start_Period, End_Period)

except UnboundLocalError:

    print("The first date of the chosen period does not exist in ECB websites.\
        Be sure to avoid holiday as first date")

# transform the timestamp format date into string
new_date = np.array([])
standard_date = np.array([])

for element in mongo_clean['Date']:

    standard = datetime.fromtimestamp(int(element))
    standard = standard.strftime('%Y-%m-%d')
    element = str(element)
    new_date = np.append(new_date, element)
    standard_date = np.append(standard_date, standard)

mongo_clean['Date'] = new_date
mongo_clean['Standard Date'] = standard_date

# upload the manipulated data in MongoDB ############################

data = mongo_clean.to_dict(orient='records')
collection_ECB_clean.insert_many(data)
