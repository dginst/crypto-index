# ############################################################################
# The file aims to complete the historical series of European Central Bank
# Websites exchange rates. It retrieves the rates from MongoDB in the database
# "index" and collection "ecb_raw" then add values for all the holidays and
# weekends simply copiyng the value of the last day with value.
# Morover the file takes the rates as EUR based exchange rates and returns
# USD based exchange rates. The completed USD based historical series is put
# back on MongoDb in the collection "ecb_clean"
# #############################################################################

# standard library import
from datetime import datetime

# third party import
import numpy as np

# local import
from cryptoindex.data_setup import ECB_setup
from cryptoindex.config import ECB_FIAT
from cryptoindex.mongo_setup import (
    mongo_coll_drop, mongo_coll,
    mongo_upload, mongo_indexing
)

# #################### initial settings ################################

start_period = "12-31-2015"

# set today as End_period
End_Period = datetime.now().strftime("%m-%d-%Y")

# ################ setup MongoDB connection ################

# drop the pre-existing collection
mongo_coll_drop("ecb_hist_s")

# creating the empty collection cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()

# ################ ECB rates cleaning ###########################

# makes the raw data clean through the ECB_setup function
try:

    mongo_clean = ECB_setup(
        ECB_FIAT, start_period, End_Period)

except UnboundLocalError:

    print(
        "The first date of the chosen period does not exist in ECB websites.\
        Be sure to avoid holiday as first date"
    )

# transform the timestamp format date into string
new_date = np.array([])
standard_date = np.array([])

for element in mongo_clean["Date"]:

    temp = datetime.fromtimestamp(int(element))
    standard = temp.strftime("%Y-%m-%d")
    element = str(element)
    new_date = np.append(new_date, element)
    standard_date = np.append(standard_date, standard)

mongo_clean["Date"] = new_date
mongo_clean["Standard Date"] = standard_date

# ############# upload data on MongoDB ##################

mongo_upload(mongo_clean, "collection_ecb_clean")
