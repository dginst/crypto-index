# ############################################################################
# The file download from the European Central Bank Websites the exchange rates
# for the currencies 'USD', 'GBP', 'CAD' and 'JPY'. Then store the retrieved
# data on MongoDB in the database "index" and collection "ecb_raw"
# ############################################################################

# standard import
import time
from datetime import datetime

# third party import
import pandas as pd
from cryptoindex.data_setup import date_gen

from cryptoindex.data_download import ECB_rates_extractor
from cryptoindex.mongo_setup import (
    mongo_coll_drop, mongo_coll,
    mongo_upload, mongo_indexing
)
from cryptoindex.config import (
    ECB_START_DATE_D, ECB_FIAT
)

# ################ setup MongoDB connection ################

# drop the pre-existing collection
mongo_coll_drop("ecb_hist_d")

# creating the empty collection cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()

# ####################### ECB rates raw data download #########################

# set today as end_date
end_date = datetime.now().strftime("%Y-%m-%d")
# create an array of date containing the list of date to download

date_list = date_gen(ECB_START_DATE_D, end_date,
                     timeST="N", clss="list", EoD="N")

date_list_str = [datetime.strptime(day, "%m-%d-%Y").strftime("%Y-%m-%d")
                 for day in date_list]

Exchange_Rate_List = pd.DataFrame()

for i, single_date in enumerate(date_list_str):

    # retrieving data from ECB website
    single_date_ex_matrix = ECB_rates_extractor(
        ECB_FIAT, date_list_str[i])
    # put a sllep time in order to do not overuse API connection
    time.sleep(0.05)
    print(single_date_ex_matrix)
    # put all the downloaded data into a DafaFrame
    if Exchange_Rate_List.size == 0:

        Exchange_Rate_List = single_date_ex_matrix

    else:

        Exchange_Rate_List = Exchange_Rate_List.append(
            single_date_ex_matrix, sort=True)

# ################ upload the raw data to MongoDB #######################

mongo_upload(Exchange_Rate_List, "collection_ecb_raw")
