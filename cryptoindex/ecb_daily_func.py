# standard library import
import time
import logging
from datetime import datetime, timezone

# third party import
import numpy as np
import pandas as pd

# local import
from cryptoindex.data_setup import (
    date_gen, Diff, ECB_daily_setup,
    ECB_setup
)
from cryptoindex.data_download import ECB_rates_extractor
from cryptoindex.mongo_setup import (
    mongo_upload, mongo_indexing,
    query_mongo, mongo_coll_drop,
    mongo_daily_delete
)
from cryptoindex.config import (
    ECB_START_DATE, ECB_START_DATE_D,
    ECB_FIAT, DAY_IN_SEC,
    DB_NAME, MONGO_DICT
)

# DAILY


def daily_check_mongo(coll_to_check, query, day_to_check=None, coll_kind=None):
    '''
    @param day_to_check: has to be either None or string yy-mm-dd format
    '''

    day_before_TS, _ = days_variable(day_to_check)

    if coll_kind is None:

        query["Time"] = int(day_before_TS)

    elif coll_kind == "ecb_raw":

        query["TIME_PERIOD"] = str(day_before_TS)

    elif coll_kind == "ecb_clean":

        query["Date"] = str(day_before_TS)

    # retrieving the wanted data on MongoDB collection
    matrix = query_mongo(DB_NAME, MONGO_DICT.get(coll_to_check), query)
    # print(matrix)

    if isinstance(matrix, list):

        res = False

    else:
        res = True

    return bool(res)


def days_variable(day):

    if day is None:

        today_str = datetime.now().strftime("%Y-%m-%d")
        today = datetime.strptime(today_str, "%Y-%m-%d")
        today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = today_TS - DAY_IN_SEC
        two_before_TS = day_before_TS - DAY_IN_SEC

    else:

        day_date = datetime.strptime(day, "%Y-%m-%d")
        day_before_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
        two_before_TS = day_before_TS - DAY_IN_SEC

    return day_before_TS, two_before_TS


def ecb_daily_download(day_to_download_TS):

    # creating the empty collection cleandata within the database index
    mongo_indexing()

    day_date = datetime.fromtimestamp(int(day_to_download_TS))
    day_date_str = day_date.strftime("%Y-%m-%d")

    # retrieving data from ECB website
    rate_of_day = ECB_rates_extractor(ECB_FIAT, day_date_str)

    return rate_of_day


def ecb_daily_up(day_to_download_TS):

    try:
        ecb_day_raw = ecb_daily_download(day_to_download_TS)
    except Exception:
        logging.error("Exception occurred", exc_info=True)
        logging.info('Daily download form ECB failed')
    try:

        mongo_upload(ecb_day_raw, "collection_ecb_raw")

    except TypeError:

        print("No rate on ECB website, the passed day is a holiday")


def ecb_daily_op(day=None):

    day_to_download_TS, _ = days_variable(day)

    if day is None:

        if daily_check_mongo("coll_ecb_raw", {"CURRENCY": "USD"}, coll_kind="ecb_raw") is False:

            ecb_daily_up(day_to_download_TS)

        else:

            print("The ecb_raw collection on MongoDB is already updated.")

        if daily_check_mongo("coll_ecb_clean", {"Currency": "EUR/USD"}, coll_kind="ecb_clean") is False:

            ecb_day_clean = ECB_daily_setup(ECB_FIAT)
            mongo_upload(ecb_day_clean, "collection_ecb_clean")

        else:

            print(
                "The ecb_clean collection on MongoDB is already updated."
            )

    else:

        ecb_daily_up(day_to_download_TS)

        ecb_day_clean = ECB_daily_setup(ECB_FIAT, day)

        mongo_upload(ecb_day_clean, "collection_ecb_clean")


# HIST

def ecb_hist_download(start_date):

    # drop the pre-existing collection related to ecb_rawdata
    mongo_coll_drop("ecb_hist_d")

    # set today as end_date
    end_date = datetime.now().strftime("%Y-%m-%d")

    # create an array of date containing the list of date to download
    date_list = date_gen(start_date, end_date,
                         timeST="N", clss="list", EoD="N")

    date_list_str = [datetime.strptime(day, "%m-%d-%Y").strftime("%Y-%m-%d")
                     for day in date_list]

    ecb_hist_series = pd.DataFrame()

    for date in date_list_str:

        # retrieving data from ECB website
        single_date_ex_matrix = ECB_rates_extractor(
            ECB_FIAT, date)
        # put a sllep time in order to do not overuse API connection
        time.sleep(0.02)

        # put all the downloaded data into a DafaFrame
        if ecb_hist_series.size == 0:

            ecb_hist_series = single_date_ex_matrix

        else:

            ecb_hist_series = ecb_hist_series.append(
                single_date_ex_matrix, sort=True)

    return ecb_hist_series


def ecb_hist_setup(start_date, fiat_curr):

    # set today as End_period
    end_date = datetime.now().strftime("%m-%d-%Y")

    # drop the pre-existing collection
    mongo_coll_drop("ecb_hist_s")

    # make the raw data clean through the ECB_setup function
    try:

        cleaned_ecb = ECB_setup(
            fiat_curr, start_date, end_date)

    except UnboundLocalError:

        print(
            "The chosen start date does not exist in ECB websites. Be sure to avoid holiday as first date"
        )

    # transform the timestamp format date into string

    timestamp_str, standard_date = date_converter(cleaned_ecb["Date"])

    cleaned_ecb["Date"] = timestamp_str
    cleaned_ecb["Standard Date"] = standard_date

    return cleaned_ecb


def date_converter(date_arr):

    ts_series = np.array([])
    standard_series = np.array([])

    for ts in date_arr:

        date_format = datetime.fromtimestamp(int(ts))
        date_str = date_format.strftime("%Y-%m-%d")
        ts_str = str(ts)
        ts_series = np.append(ts_series, ts_str)
        standard_series = np.append(standard_series, date_str)

    return ts_series, standard_series


def ecb_hist_op(start_date_d=ECB_START_DATE_D, start_date_s=ECB_START_DATE, fiat_curr=ECB_FIAT):

    ecb_hist_raw = ecb_hist_download(start_date_d)
    mongo_upload(ecb_hist_raw, "collection_ecb_raw")
    ecb_hist_clean = ecb_hist_setup(start_date_s, fiat_curr)
    mongo_upload(ecb_hist_clean, "collection_ecb_clean")

    return None


def ecb_hist_no_download(start_date_s=ECB_START_DATE, fiat_curr=ECB_FIAT):

    ecb_hist_clean = ecb_hist_setup(start_date_s, fiat_curr)
    mongo_upload(ecb_hist_clean, "collection_ecb_clean")

    return None


def check_missing(tot_date_arr, coll_to_check, query, days_to_check=10):

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

    to_del = np.array([0])
    date_list = np.setdiff1d(date_list, to_del)

    last_days_db = date_list[(len(date_list) - days_to_check):len(date_list)]

    # last_days_db_str = [str(single_date)
    #                     for single_date in last_days_db]

    # finding the date to download as difference between
    # complete array of date and date now stored on MongoDB
    date_to_add = Diff(last_days, last_days_db)
    print(date_to_add)

    if len(date_to_add) > 9:

        date_to_add = None

    return date_to_add


# #############################
# function for table with exchange rate and coinbase-pro BTCUSD value

def daily_table():

    # assign date of interest to variables
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    y_TS = today_TS - DAY_IN_SEC

    ecb_query = {"Date": str(y_TS), "Currency": "EUR/USD"}
    btcusd_query = {"Time": y_TS, "Exchange": "coinbase-pro", "Pair": "btcusd"}

    eurusd = query_mongo(DB_NAME, "ecb_clean", ecb_query)
    btcusd = query_mongo(DB_NAME, "index_data_feed", btcusd_query)

    btcusd_value = np.array(btcusd["Close Price"])
    eurusd_value = np.array(eurusd["Rate"])
    human_date = np.array(eurusd["Standard Date"])

    df = pd.DataFrame(columns=["Date", "EUR/USD", "BTCUSD"])
    df["Date"] = human_date
    df["EUR/USD"] = eurusd_value
    df["BTCUSD"] = btcusd_value

    return df
