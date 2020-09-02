# standard library import
import time
from datetime import datetime

# third party import
import numpy as np
import pandas as pd

# local import
from cryptoindex.data_setup import (
    date_gen, Diff, ECB_daily_setup,
    timestamp_to_human
)
from cryptoindex.data_download import ECB_rates_extractor
from cryptoindex.mongo_setup import (
    mongo_upload, mongo_indexing,
    query_mongo
)
from cryptoindex.config import (
    ECB_START_DATE, ECB_FIAT,
    DB_NAME, MONGO_DICT
)


def check_missing(tot_date_arr, coll_to_check, query, days_to_check=5):

    # selecting the last five days and put them into an array
    last_days = tot_date_arr[(
        len(tot_date_arr) - days_to_check): len(tot_date_arr)]

    # retrieving the wanted data on MongoDB collection
    matrix = query_mongo(DB_NAME, MONGO_DICT.get(coll_to_check), query)

    # checking the time column and selecting only the last five days retrived
    # from MongoDB collection
    try:

        date_list = np.array(matrix["Time"])

    except KeyError:

        try:

            date_list = np.array(matrix["TIME_PERIOD"])

        except KeyError:

            date_list = np.array(matrix["Date"])

    last_days_db = date_list[(len(date_list) - 5): len(date_list)]
    last_days_db_str = [str(single_date)
                        for single_date in last_days_db]

    # finding the date to download as difference between
    # complete array of date and date now stored on MongoDB
    date_to_add = Diff(last_days, last_days_db_str)

    return date_to_add


def missing_start_stop(date_to_add):

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = timestamp_to_human(
            [date_to_add[0]], date_format="%m-%d-%Y"
        )
        start_date = start_date[0]
        end_date = timestamp_to_human(
            [date_to_add[len(date_to_add) - 1]], date_format="%m-%d-%Y"
        )
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime("%m-%d-%Y")
        end_date = start_date

    return start_date, end_date


def ecb_daily_download(day_to_download=None):

    # creating the empty collection cleandata within the database index
    mongo_indexing()

    if day_to_download is None:

        # defining the array containing all the date from start_period until today
        date_tot = date_gen(ECB_START_DATE)

        # converting the timestamp format date into string
        date_tot_str = [str(single_date) for single_date in date_tot]

        date_to_download = check_missing(
            date_tot_str, "coll_ecb_raw", {"CURRENCY": "USD"})

        # converting the timestamp into YYYY-MM-DD in order to perform
        # the download from the ECB website
        date_to_download = [datetime.fromtimestamp(
            int(date)) for date in date_to_download]
        date_to_download = [date.strftime("%Y-%m-%d")
                            for date in date_to_download]

        Exchange_Rate_List = pd.DataFrame()

        if date_to_download != []:

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

        else:

            print(
                "Message: No new date to download, the ecb_raw collection on MongoDB is updated."
            )

    else:

        single_date_ex_matrix = ECB_rates_extractor(
            ECB_FIAT, day_to_download
        )

        Exchange_Rate_List = single_date_ex_matrix

        print("New ECB data have been correctly downloaded")
    return Exchange_Rate_List


def ecb_daily_op(day=None):

    if day is None:

        date_tot = date_gen(ECB_START_DATE)
        date_tot_str = [str(single_date) for single_date in date_tot]

        ecb_day_raw = ecb_daily_download()

        try:

            mongo_upload(ecb_day_raw, "collection_ecb_raw")

        except TypeError:
            pass

        day_missing = check_missing(
            date_tot_str, "coll_ecb_clean", {"Currency": "EUR/USD"}
        )

        if day_missing != []:

            ecb_day_clean = ECB_daily_setup(ECB_FIAT)

            mongo_upload(ecb_day_clean, "collection_ecb_clean")

        else:

            print(
                "Message: No need to upload rates, the ecb_clean collection on MongoDB is updated."
            )

    else:

        ecb_day_raw = ecb_daily_download(day)

        mongo_upload(ecb_day_raw, "collection_ecb_raw")

        ecb_day_clean = ECB_daily_setup(ECB_FIAT, day_to_clean=day)

        print('New ECB rawdata have been correctly manipulated and are now ready')

        mongo_upload(ecb_day_clean, "collection_ecb_clean")

    return None
