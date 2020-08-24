# standard library import
import time
from datetime import datetime

# third party import
import numpy as np
import pandas as pd

# local import
from cryptoindex.data_setup import (
    date_gen, Diff
)
from cryptoindex.data_download import ECB_rates_extractor
from cryptoindex.mongo_setup import (
    mongo_coll,
    mongo_upload, mongo_indexing,
    query_mongo
)
from cryptoindex.config import (
    ECB_START_DATE, ECB_FIAT,
    DB_NAME, MONGO_DICT
)


# ################ setup MongoDB connection ################

# creating the empty collection cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()

# ############### ecb_raw collection check ##################

# set today
today = datetime.now().strftime("%Y-%m-%d")

# defining the array containing all the date from start_period until today
date_tot = date_gen(ECB_START_DATE)

# converting the timestamp format date into string
date_tot_str = [str(single_date) for single_date in date_tot]

# searching only the last five days
last_five_days = date_tot_str[(len(date_tot_str) - 5): len(date_tot_str)]

# defining the MongoDB path where to look for the rates

query = {"CURRENCY": "USD"}

matrix = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_raw"), query)

# checking the time column
date_list = np.array(matrix["TIME_PERIOD"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_download = Diff(last_five_days, last_five_days_mongo)

# converting the timestamp into YYYY-MM-DD in order to perform
# the download from the ECB website
date_to_download = [datetime.fromtimestamp(
    int(date)) for date in date_to_download]
date_to_download = [date.strftime("%Y-%m-%d") for date in date_to_download]

# ############## new ECB rates raw data download ######################

Exchange_Rate_List = pd.DataFrame()

for single_date in date_to_download:

    # retrieving data from ECB website
    single_date_ex_matrix = ECB_rates_extractor(
        ECB_FIAT, single_date
    )
    # put a sleep time in order to do not overuse API connection
    time.sleep(0.05)

    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix

    else:

        Exchange_Rate_List = Exchange_Rate_List.append(
            single_date_ex_matrix, sort=True)

# ################ upload data on MongoDB ############################

if Exchange_Rate_List.empty is True:

    print(
        "Message: No new date to download, the ecb_raw\
        collection on MongoDB is updated."
    )

else:

    mongo_upload(Exchange_Rate_List, "collection_ecb_raw")
