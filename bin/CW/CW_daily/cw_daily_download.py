# standard library import
from datetime import datetime

# third party import
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing, query_mongo)
from cryptoindex.config import (
    START_DATE, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME)

# initial settings ############################################


# set today
today = datetime.now().strftime("%Y-%m-%d")


# setup mongo connection ###################################

mongo_indexing()

collection_dict_upload = mongo_coll()


# converted_data check ###########################################

# defining the array containing all the date from start_period until today
date_tot = data_setup.date_gen(START_DATE)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_tot]

# searching only the last five days
last_five_days = date_tot[(len(date_tot) - 5): len(date_tot)]
print(last_five_days)
# defining the MongoDB path where to look for the rates

query = {"Exchange": "coinbase-pro", "Pair": "ethusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"), query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]
last_five_days_mongo = [str(single_date)
                        for single_date in last_five_days_mongo]
print(last_five_days_mongo)
# finding the date to download as difference between
# complete array of date and date now stored on MongoDB
date_to_add = data_setup.Diff(last_five_days, last_five_days_mongo)

if date_to_add != []:

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = data_setup.timestamp_to_human(
            [date_to_add[0]], date_format="%m-%d-%Y"
        )
        start_date = start_date[0]
        end_date = data_setup.timestamp_to_human(
            [date_to_add[len(date_to_add) - 1]], date_format="%m-%d-%Y"
        )
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime("%m-%d-%Y")
        end_date = start_date

    # download part #############

    for Crypto in CRYPTO_ASSET:

        ccy_pair_array = []
        for i in PAIR_ARRAY:
            ccy_pair_array.append(Crypto.lower() + i)

        for exchange in EXCHANGES:

            for cp in ccy_pair_array:

                crypto = cp[:3]
                pair = cp[3:]
                # create the matrix for the single currency_pair
                # connecting to CryptoWatch website
                data_download.CW_raw_to_mongo(
                    exchange, cp, collection_dict_upload.get(
                        "collection_cw_raw"), str(start_date)
                )
else:

    print(
        "Message: No new date to download from CryptoWatch,\
        the rawdata collection on MongoDB is updated."
    )
