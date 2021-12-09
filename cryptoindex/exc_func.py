from datetime import datetime, timezone
from typing import Dict

import numpy as np
import pandas as pd

from cryptoindex.calc import conv_into_usd
from cryptoindex.config import (CLEAN_DATA_HEAD, CONVERSION_FIAT, CRYPTO_ASSET,
                                DAY_IN_SEC, DB_NAME, EXC_START_DATE, EXCHANGES,
                                MONGO_DICT, PAIR_ARRAY, STABLE_COIN,
                                START_DATE)
from cryptoindex.cw_hist_func import crypto_fiat_pair_gen
from cryptoindex.data_setup import (daily_fix_miss, date_gen,
                                    exc_pair_cleaning, homogenize_series,
                                    pair_vol_fix)
from cryptoindex.mongo_setup import (df_reorder, mongo_coll, mongo_coll_drop,
                                     mongo_indexing, mongo_upload, query_mongo)


def daily_check_mongo(coll_to_check, query, day_to_check=None, coll_kind=None):

    day_before_TS, _ = days_variable(day_to_check)

    if coll_kind is None:

        query["Time"] = int(day_before_TS)

    elif coll_kind == "ecb_raw":

        query["TIME_PERIOD"] = str(day_before_TS)

    elif coll_kind == "ecb_clean":

        query["Date"] = str(day_before_TS)

    # retrieving the wanted data on MongoDB collection
    matrix = query_mongo(DB_NAME, MONGO_DICT.get(coll_to_check), query)

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


def daily_exc_pair_vol_fix(daily_mat, exc_list):

    daily_mat = daily_mat[
        ["Pair", "Exchange", "Close Price", "Time", "Crypto Volume", "date"]
    ]

    # selecting the exchange used in the index computation
    daily_mat = daily_mat.loc[daily_mat["Exchange"].isin(exc_list)]

    # creating a column containing the hour of extraction
    daily_mat["date"] = [str(d) for d in daily_mat["date"]]
    daily_mat["hour"] = daily_mat["date"].str[11:16]

    daily_mat = daily_mat.loc[daily_mat.Time != 0]

    daily_mat_clean = exc_pair_cleaning(daily_mat)
    daily_mat_fixed = pair_vol_fix(daily_mat_clean)

    return daily_mat_fixed


def exc_time_split(daily_mat):

    # creating 4 different df for each exctraction hour
    daily_mat_00 = daily_mat.loc[daily_mat.hour == "00:00"]
    daily_mat_12 = daily_mat.loc[daily_mat.hour == "12:00"]
    daily_mat_16 = daily_mat.loc[daily_mat.hour == "16:00"]
    daily_mat_20 = daily_mat.loc[daily_mat.hour == "20:00"]

    # creating the exchange-pair couples key for the daily matrix
    # for each above defined df
    daily_mat_00["key"] = daily_mat_00["Exchange"] + \
        "&" + daily_mat_00["Pair"]
    daily_mat_12["key"] = daily_mat_12["Exchange"] + \
        "&" + daily_mat_12["Pair"]
    daily_mat_16["key"] = daily_mat_16["Exchange"] + \
        "&" + daily_mat_16["Pair"]
    daily_mat_20["key"] = daily_mat_20["Exchange"] + \
        "&" + daily_mat_20["Pair"]

    return daily_mat_00, daily_mat_12, daily_mat_16, daily_mat_20


def exc_dead_key_mng(logic_key_df, daily_mat_00, daily_mat_12, day_to_clean_TS):

    # selecting only the exchange-pair couples present in the historical series
    key_present = logic_key_df.loc[logic_key_df.logic_value == 1]
    key_present = key_present.drop(columns=["logic_value"])
    # applying a left join between the prresent keys matrix and the daily
    # matrix, this operation returns a matrix containing all the keys in
    # "key_present" and, if some keys are missing in "daily_mat" put NaN
    merged = pd.merge(key_present, daily_mat_00, on="key", how="left")
    # assigning some columns values and substituting NaN with 0
    # in the "merged" df
    merged["Time"] = day_to_clean_TS
    split_val = merged["key"].str.split("&", expand=True)
    merged["Exchange"] = split_val[0]
    merged["Pair"] = split_val[1]

    # define a df containing only the NaN value, so the keys
    # that are not present in daily_mat_00
    check_key = merged.loc[merged["Close Price"].isnull(), "key"]

    # join the dataframes that contains the keys not present in daily_mat_00
    # and the daily_mat_12
    check_12 = pd.merge(check_key, daily_mat_12, on="key", how="left")

    # isolate the potential non-NaN resulting from the join, the df would
    # contain the keys present in the extraction hour 12:00
    present_12 = check_12.loc[check_12["Close Price"].notnull()]

    # if the "present_12" df is not empty, then substitute the values for each
    # keys in the "merged" df, otherwise pass
    if present_12.empty is False:

        for k in present_12["key"]:

            price = present_12.loc[present_12["key"] == k, "Close Price"]
            p_vol = present_12.loc[present_12["key"] == k, "Pair Volume"]
            c_vol = present_12.loc[present_12["key"] == k, "Crypto Volume"]
            merged.loc[merged["key"] == k, "Close Price"] = price
            merged.loc[merged["key"] == k, "Pair Volume"] = p_vol
            merged.loc[merged["key"] == k, "Crypto Volume"] = c_vol
    else:
        pass

    # giving 0 to all the remainig values
    merged.fillna(0, inplace=True)
    print(merged)

    return merged


def log_key_update(key_to_update, collection):

    collection_dict_upload = mongo_coll()

    q_del = {"key": key_to_update}
    collection_dict_upload.get(collection).delete_many(q_del)

    # updating the logic matrix of exchange-pair couple
    new_key_log = pd.DataFrame(np.column_stack(
        (key_to_update, int(1))), columns=["key", "logic_value"])
    new_key_log["logic_value"] = 1

    return new_key_log


def new_series_composer(key, new_key_df, date_tot_str, day_to_check, kind_of_series="CW"):

    # create the df containing the historical series of the new couple(s)
    # composed by zeros
    splited_key = key.split("&")
    key_hist_df = pd.DataFrame(date_tot_str, columns=["Time"])
    key_hist_df["Close Price"] = 0
    key_hist_df["Pair Volume"] = 0
    key_hist_df["Crypto Volume"] = 0
    key_hist_df["Exchange"] = splited_key[0]
    key_hist_df["Pair"] = splited_key[1]

    if kind_of_series == "CW":

        collection_dict_upload = mongo_coll()

        # uploading on MongoDB collections "CW_converted_data" and "CW_final_data"
        # the new series of zero except for the last value (yesterday)
        mongo_upload(key_hist_df, "collection_cw_converted")
        mongo_upload(key_hist_df, "collection_cw_final_data")

        query_to_del = {"Time": int(day_to_check)}
        collection_dict_upload.get(
            "collection_cw_converted").delete_many(query_to_del)
        collection_dict_upload.get(
            "collection_cw_final_data").delete_many(query_to_del)

    else:
        pass

    # inserting the today value of the new couple(s)
    new_price = np.array(new_key_df.loc[new_key_df.key == key, "Close Price"])
    new_p_vol = np.array(new_key_df.loc[new_key_df.key == key, "Pair Volume"])
    new_c_vol = np.array(
        new_key_df.loc[new_key_df.key == key, "Crypto Volume"])
    key_hist_df.loc[key_hist_df.Time == int(
        day_to_check), "Close Price"] = new_price
    key_hist_df.loc[key_hist_df.Time == int(
        day_to_check), "Pair Volume"] = new_p_vol
    key_hist_df.loc[key_hist_df.Time == int(
        day_to_check), "Crypto Volume"] = new_c_vol

    return key_hist_df


def exc_new_key_mng(logic_key_df, daily_mat_00, day_to_clean_TS):

    # define a subset of keys that has not been present in the past
    key_absent = logic_key_df.loc[logic_key_df.logic_value == 0]
    key_absent.drop(columns=["logic_value"])

    # merging the two dataframe (left join) in order to find potential
    # new keys in the data of the day
    merg_absent = pd.merge(key_absent, daily_mat_00, on="key", how="left")
    merg_absent.fillna("NaN", inplace=True)
    new_key_df = merg_absent.loc[merg_absent["Close Price"] != "NaN"]

    if new_key_df.empty is False:

        print("Message: New exchange-pair couple(s) found.")
        new_key_list = new_key_df["key"]
        print(new_key_list)

        date_tot_int = date_gen(START_DATE)
        # converting the timestamp format date into string
        date_tot_str = [int(single_date) for single_date in date_tot_int]

        for key in new_key_list:

            # updating the logic matrix of exchange-pair keys
            logic_row_update = log_key_update(key, "collection_EXC_key")
            mongo_upload(logic_row_update, "collection_EXC_key")

            key_hist_df = new_series_composer(
                key, new_key_df, date_tot_str, day_to_clean_TS)

    else:

        key_hist_df = []

    return key_hist_df


def exc_daily_key_mngm(daily_mat_00, daily_mat_12, day_to_clean_TS):

    logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_keys"))

    # adding to the daily matrix the value referred to dead crypto-fiat pair
    daily_mat_with_dead = exc_dead_key_mng(
        logic_key, daily_mat_00, daily_mat_12, day_to_clean_TS)

    # searching for possible new crypto-fiat pair
    new_key_hist = exc_new_key_mng(logic_key, daily_mat_00, day_to_clean_TS)

    if new_key_hist != []:

        collection_dict_upload = mongo_coll()
        # upload the new artificial historical series on MongoDB
        # collection "EXC_cleandata"
        mongo_upload(new_key_hist, collection_dict_upload.get(
            "collection_exc_clean"))

    else:

        pass

    return daily_mat_with_dead


def exc_daily_fix_op(day_bfr_mat, daily_mat_complete):

    # add the "key" column
    day_bfr_mat["key"] = day_bfr_mat["Exchange"] + "&" + day_bfr_mat["Pair"]

    # looping through all the daily keys looking for potential missing value
    for key_val in day_bfr_mat["key"]:

        new_val = daily_mat_complete.loc[daily_mat_complete.key == key_val]
        # if the new 'Close Price' referred to a certain key is 0 the script
        # check the previous day value: if is == 0 then pass, if is != 0
        # the values related to the selected key needs to be corrected
        # ###### IF A EXC-PAIR DIES AT A CERTAIN POINT, THE SCRIPT
        # CHANGES THE VALUES. MIGHT BE WRONG #######################
        if np.array(new_val["Close Price"]) == 0.0:

            d_before_val = day_bfr_mat.loc[day_bfr_mat.key == key_val]

            if np.array(d_before_val["Close Price"]) != 0.0:

                price_var = daily_fix_miss(
                    new_val, daily_mat_complete, day_bfr_mat)

                # applying the weighted variation to the day before 'Close Price'
                new_price = (1 + price_var) * d_before_val["Close Price"]
                # changing the 'Close Price' value using the new computed price
                daily_mat_complete.loc[daily_mat_complete.key
                                       == key_val, "Close Price"] = new_price

            else:
                pass

        else:
            pass

    # put the manipulated data on MongoDB
    daily_mat_final = daily_mat_complete.drop(columns=["key", "date", "hour"])
    daily_mat_final['Time'] = [int(t) for t in daily_mat_final['Time']]

    return daily_mat_final


def exc_daily_cleaning(exc_list, day_to_clean):

    previous_day = int(day_to_clean) - DAY_IN_SEC

    # download from MongoDB the exc raw data of yesterday
    q_dict: Dict[str, str] = {}
    q_dict = {"Time": str(day_to_clean + DAY_IN_SEC)}
    daily_mat = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_exc_raw"), q_dict)

    # fixing the "pair volume" information in the daily df
    daily_mat_vol_fix = daily_exc_pair_vol_fix(daily_mat, exc_list)

    # creating different df based on the hour of download
    (daily_mat_00, daily_mat_12, _, _) = exc_time_split(daily_mat_vol_fix)

    # transpose the Time information by 86400 seconds (1 day) ###### str o int???
    daily_mat_00["Time"] = [str(int(element) - DAY_IN_SEC)
                            for element in daily_mat_00["Time"]]
    daily_mat_12["Time"] = [str(int(element) - DAY_IN_SEC)
                            for element in daily_mat_12["Time"]]

    # completing the daily matrix with dead crypto-fiat pair
    daily_mat_complete = exc_daily_key_mngm(
        daily_mat_00, daily_mat_12, day_to_clean)

    # downloading from MongoDB the matrix referring to the previuos day
    q_dict_bfr: Dict[str, int] = {}
    q_dict_bfr = {"Time": int(previous_day)}
    day_bfr_mat = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_exc_clean"), q_dict_bfr)

    fixed_daily_mat = exc_daily_fix_op(day_bfr_mat, daily_mat_complete)

    return fixed_daily_mat


def daily_conv_op(day_to_conv_TS, conversion_fiat=CONVERSION_FIAT,
                  stable=STABLE_COIN, series="CW"):

    # querying the data from mongo
    query_data = {"Time": int(day_to_conv_TS)}
    query_rate = {"Date": str(day_to_conv_TS)}
    query_stable = {"Time": int(day_to_conv_TS)}
    # querying the data from mongo
    matrix_rate = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_ecb_clean"), query_rate)

    if series == "CW":

        matrix_data = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_data)

    elif series == "EXC":

        matrix_data = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_exc_clean"), query_data)

    matrix_rate_stable = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_stable_rate"), query_stable)

    # converting the "matrix_rate" df
    converted_df = conv_into_usd(
        DB_NAME, matrix_data, matrix_rate,
        matrix_rate_stable, conversion_fiat, stable)

    return converted_df


def exc_daily_op(day=None):

    day_before_TS, _ = days_variable(day)

    if day is None:

        if daily_check_mongo("coll_exc_clean", {"Exchange": "coinbase-pro", "Pair": "ethusd"}) is False:

            cleaned_df = exc_daily_cleaning(EXCHANGES, day_before_TS)
            mongo_upload(cleaned_df, "collection_exc_clean")

        else:
            print("The collection EXC_cleandata is already updated")

        if daily_check_mongo("coll_exc_final", {"Exchange": "coinbase-pro", "Pair": "ethusd"}) is False:

            converted_df = daily_conv_op(day_before_TS, series="EXC")
            converted_df.fillna(0, inplace=True)
            mongo_upload(converted_df, "collection_exc_final_data")

        else:

            print("The collection EXC_final_data is already updated")

    else:
        pass

    return None


def cw_exc_merging(start_date=START_DATE, exc_start=EXC_START_DATE,
                   db=DB_NAME, coll_cw="coll_cw_final",
                   coll_exc="coll_exc_final"):

    cw_date_arr = date_gen(start_date, exc_start, EoD="N")

    exc_series = query_mongo(db, MONGO_DICT.get(coll_exc))
    exc_part = df_reorder(exc_series, column_set="conversion")

    # downloading the CW series from MongoDB and selecting only the date
    # from 2016-01-01 to 2020-04-17
    cw_series = query_mongo(db, MONGO_DICT.get(coll_cw))
    cw_part = cw_series.loc[cw_series.Time.isin(cw_date_arr)]
    cw_part = df_reorder(cw_part, column_set="conversion")

    # creting an unique dataframe containing the two different data source
    merged_series = cw_part.append(exc_part, sort=True)
    merged_series["Time"] = [int(d) for d in merged_series["Time"]]

    return merged_series


def data_feed_op():

    mongo_coll_drop("index_feed")

    merged_series = cw_exc_merging()
    mongo_upload(merged_series, "collection_data_feed")

    return None


def exc_daily_feed(day=None):

    day_before_TS, _ = days_variable(day)

    if day is None:

        if daily_check_mongo("coll_data_feed", {"Exchange": "coinbase-pro", "Pair": "ethusd"}) is False:

            query_data = {"Time": int(day_before_TS)}
            exc_daily_df = query_mongo(
                DB_NAME, MONGO_DICT.get("coll_exc_final"), query_data)
            mongo_upload(exc_daily_df, "collection_data_feed")

        else:
            print("The collection index_data_feed is already updated")

    else:
        pass

    return None

# ########## HISTORICAL EXC RAW DATA OPERATION ##############


# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC

# creating the timestamp array at 12:00 AM
date_array = date_gen(EXC_START_DATE)
date_array_str = [str(el) for el in date_array]


def all_crypto_fiat_gen():

    # defining the crypto-fiat pairs array
    all_crypto_fiat_array = []

    for crypto in CRYPTO_ASSET:

        for fiat in PAIR_ARRAY:

            all_crypto_fiat_array.append(crypto.lower() + fiat)

    return all_crypto_fiat_array


def exc_initial_clean(all_data, crypto_fiat_arr):

    # correcting the Crypto Volume information for Bitflyer and pair BTCJPY
    all_data.loc[(
        all_data.Exchange == "bitflyer") & (all_data.Pair == "BTCJPY"), "Crypto Volume"] = all_data.loc[(
            all_data.Exchange == "bitflyer") & (all_data.Pair == "BTCJPY"), "volume_by_product"]

    # changing the "Time" values format from integer to string
    all_data["Time"] = [str(element) for element in all_data["Time"]]
    all_data["date"] = [str(element) for element in all_data["date"]]

    # creating a column containing the hour of extraction
    all_data["hour"] = all_data["date"].str[11:16]

    # isolating the df referred to midnights
    all_00, _, _, _ = exc_time_split(all_data)

    # keeping only the columns of interest among all the
    # information in rawdata
    all_00 = all_00.loc[:, CLEAN_DATA_HEAD]

    # changing some features in "Pair" field
    all_00_clean = exc_pair_cleaning(all_00)

    # changing an error in "bitstamp" spelling
    all_00_clean["Exchange"] = [
        element.replace("bistamp", "bitstamp") for element in all_00_clean["Exchange"]]

    # selecting the crypto-fiat pairs used in the index computation
    all_00_clean = all_00_clean.loc[all_00_clean["Pair"].isin(crypto_fiat_arr)]

    # selecting the exchange used in the index computation
    all_00_clean = all_00_clean.loc[all_00_clean["Exchange"].isin(EXCHANGES)]

    # fixing 1598486400 day
    prev_day = 1598486400 - DAY_IN_SEC
    prev_day_mat = all_00_clean.loc[all_00_clean["Time"] == str(prev_day)]
    prev_day_new_time = prev_day_mat

    prev_day_new_time["Time"] = str(1598486400)

    all_00_clean = all_00_clean.loc[all_00_clean.Time != str(1598486400)]
    all_00_clean = all_00_clean.append(prev_day_new_time)

    # correcting the "Pair Volume" field
    all_00_clean["Close Price"] = [float(element)
                                   for element in all_00_clean["Close Price"]]
    all_00_clean["Crypto Volume"] = [float(element)
                                     for element in all_00_clean["Crypto Volume"]]

    all_00_clean["Pair Volume"] = all_00_clean["Close Price"] * \
        all_00_clean["Crypto Volume"]

    # transpose everything by 1 day (86400 secs)
    all_00_clean["Time"] = [str(int(element) - DAY_IN_SEC)
                            for element in all_00_clean["Time"]]

    all_00_clean.sort_values(by=['Time'], inplace=True, ascending=True)
    all_00_clean.reset_index(drop=True, inplace=True)

    return all_00_clean


def exc_key_mngmt(exc_clean_df):

    # downloading from MongoDB the matrix containing the exchange-pair logic values
    logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_keys"))

    # creating the exchange-pair couples key for the daily matrix
    exc_clean_df["key"] = exc_clean_df["Exchange"] + "&" + exc_clean_df["Pair"]

    # ## adding the dead series to the daily values

    # selecting only the exchange-pair couples present in the historical series
    key_present = logic_key.loc[logic_key.logic_value == 1]
    key_present = key_present.drop(columns=["logic_value"])

    exc_clean_df = exc_clean_df.loc[exc_clean_df.Time != str(today_TS)]

    # selecting the last day of the EXC "historical" series
    last_day_with_val = max(exc_clean_df.Time)
    last_day = exc_clean_df.loc[exc_clean_df.Time
                                == last_day_with_val]

    # applying a left join between the prresent keys matrix and the last_day
    # matrix, this operation returns a matrix containing all the keys in
    # "key_present" and, if some keys are missing in "all_data" put NaN
    merged = pd.merge(key_present, last_day, on="key", how="left")

    # selecting only the absent keys
    merg_absent = merged.loc[merged["Close Price"].isnull()]

    header = CLEAN_DATA_HEAD
    header.extend(["key"])

    # create the historical series for each selected element
    for k in merg_absent["key"]:

        mat_to_add = pd.DataFrame(columns=header)
        mat_to_add["Time"] = date_array_str
        split_val = k.split("&")
        mat_to_add["Exchange"] = split_val[0]
        mat_to_add["Pair"] = split_val[1]
        mat_to_add["Close Price"] = 0.0
        mat_to_add["Crypto Volume"] = 0.0
        mat_to_add["Pair Volume"] = 0.0
        exc_clean_df = exc_clean_df.append(mat_to_add)

    # uploading the cleaned data on MongoDB in the collection EXC_cleandata
    exc_clean_df = exc_clean_df.drop(columns=["key"])
    exc_clean_df["Time"] = [int(element) for element in exc_clean_df["Time"]]

    # deleting the 17/04/2020 from the df (if presents)
    exc_complete_df = exc_clean_df.loc[exc_clean_df.Time != 1587081600]

    return exc_complete_df


def exc_hist_fix(exc_complete_df):

    exc_fixed_df = pd.DataFrame(columns=CLEAN_DATA_HEAD)

    for crypto in CRYPTO_ASSET:

        pair_arr = crypto_fiat_pair_gen(crypto)

        for exchange in EXCHANGES:

            ex_matrix = exc_complete_df.loc[exc_complete_df.Exchange == exchange]

            for cp in pair_arr:

                crypto = cp[:3]

                cp_matrix = ex_matrix.loc[ex_matrix["Pair"] == cp]
                cp_matrix = cp_matrix.drop(columns=["Exchange", "Pair"])

                if exchange == "poloniex" and (cp == "bchusdc" or cp == "bchusdt"):

                    cp_matrix = cp_matrix.loc[cp_matrix["Close Price"] != 0.000000]

                # checking if the matrix is not empty
                if cp_matrix.shape[0] > 1:

                    # check if the historical series start at the same date as
                    # the start date if not fill the dataframe with zero values
                    cp_matrix = homogenize_series(
                        cp_matrix, date_array)

                    if cp_matrix.shape[0] != len(date_array_str):

                        date_df = pd.DataFrame(columns=["Time"])
                        date_df["Time"] = np.array(date_array)
                        merged_cp = pd.merge(
                            date_df, cp_matrix, on="Time", how="left")
                        merged_cp.fillna("NaN", inplace=True)
                        nan_list = list(
                            merged_cp.loc[merged_cp["Close Price"] == "NaN", "Time"])
                        for nan in nan_list:

                            prev_price = merged_cp.loc[merged_cp.Time
                                                       == nan - DAY_IN_SEC, "Close Price"]
                            prev_p_vol = merged_cp.loc[merged_cp.Time
                                                       == nan - DAY_IN_SEC, "Pair Volume"]
                            prev_c_vol = merged_cp.loc[merged_cp.Time
                                                       == nan - DAY_IN_SEC, "Crypto Volume"]
                            merged_cp.loc[merged_cp.Time
                                          == nan, "Close Price"] = prev_price
                            merged_cp.loc[merged_cp.Time
                                          == nan, "Pair Volume"] = prev_p_vol
                            merged_cp.loc[merged_cp.Time
                                          == nan, "Crypto Volume"] = prev_c_vol

                        cp_matrix = merged_cp

                cp_matrix.fillna(0, inplace=True)
                cp_matrix["Exchange"] = exchange
                cp_matrix["Pair"] = cp

                exc_fixed_df = exc_fixed_df.append(cp_matrix)

    exc_fixed_df = exc_fixed_df.drop(columns=["key"])

    return exc_fixed_df


def exc_hist_conv(exc_fix_df):

    # querying the rates collection from MongoDB
    matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"))
    matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
    matrix_rate = matrix_rate.loc[matrix_rate.Time.isin(date_array_str)]

    # querying the stable rates collection from MongoDB
    matrix_rate_stable = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_stable_rate"))
    matrix_rate_stable = matrix_rate_stable.loc[matrix_rate_stable.Time.isin(
        date_array_str)]

    converted_data = conv_into_usd(DB_NAME, exc_fix_df, matrix_rate,
                                   matrix_rate_stable, CONVERSION_FIAT, STABLE_COIN)

    converted_data["Time"] = [int(element)
                              for element in converted_data["Time"]]

    return converted_data


def exc_hist_op():

    mongo_coll_drop("exc")

    mongo_indexing()

    # defining the crytpo_fiat array
    crypto_fiat_arr = all_crypto_fiat_gen()
    # querying all raw data from EXC_rawdata
    exc_raw_df = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_raw"))

    midnight_clean = exc_initial_clean(exc_raw_df, crypto_fiat_arr)
    mongo_upload(midnight_clean, "collection_exc_uniform")

    # deleting the values for xrp in the coinbase-pro exchange
    midnight_clean["key"] = midnight_clean["Exchange"] + \
        "&" + midnight_clean["Pair"]
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "coinbase-pro&xrpusd"]
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "coinbase-pro&xrpeur"]
    # deleting the values for zec and xmr in the bittrex exchange
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "bittrex&zecusd"]
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "bittrex&zecusdt"]
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "bittrex&zecusdc"]
    midnight_clean = midnight_clean.loc[midnight_clean.key
                                        != "bittrex&xmrusdt"]

    midnight_clean = midnight_clean.drop(columns="key")

    exc_complete_df = exc_key_mngmt(midnight_clean)
    exc_fixed_df = exc_hist_fix(exc_complete_df)
    mongo_upload(exc_fixed_df, "collection_exc_clean")

    exc_converted = exc_hist_conv(exc_fixed_df)
    exc_converted.fillna(0, inplace=True)
    mongo_upload(exc_converted, "collection_exc_final_data")

    return None


def hist_data_feed_op():

    # define the array containing the date where the index uses CW feed data
    CW_date_arr = date_gen(START_DATE, EXC_START_DATE)
    CW_date_str = [str(date) for date in CW_date_arr]

    # drop the pre-existing collection (if there is one)
    mongo_coll_drop("index_feed")

    # downloading the EXC series from MongoDB
    EXC_series = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_final"))
    EXC_series = EXC_series[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]

    # downloading the CW series from MongoDB and selecting only the date
    # from 2016-01-01 to 2020-04-17
    CW_series = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_final"))
    CW_series["Time"] = [str(x) for x in CW_series["Time"]]
    print("CW")
    print(CW_series)
    CW_sub_series = CW_series.loc[CW_series.Time.isin(CW_date_str)]
    print(CW_sub_series)
    CW_sub_series = CW_sub_series[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]
    CW_sub_series.reset_index(drop=True, inplace=True)
    print(CW_sub_series)

    # creting an unique dataframe containing the two different data source
    data_feed = CW_sub_series.append(EXC_series, sort=True)
    data_feed.reset_index(drop=True, inplace=True)

    data_feed = data_feed[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]

    print(data_feed)
    data_feed = homogeneize_feed(data_feed)
    print("post hom")
    print(data_feed)

    # put the converted data on MongoDB
    mongo_upload(data_feed, "collection_data_feed")

    return None


def homogeneize_feed(initial_df):

    df = initial_df.copy()
    list_of_exchanges = list(np.array(df["Exchange"].unique()))
    list_of_pair = list(np.array(df["Pair"].unique()))

    # today_str = datetime.now().strftime("%Y-%m-%d")
    # today = datetime.strptime(today_str, "%Y-%m-%d")
    # today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    # day_before_TS = today_TS - DAY_IN_SEC
    list_of_missing = date_gen(EXC_START_DATE)

    ref_shape = df.loc[(df.Exchange == "coinbase-pro")
                       & (df.Pair) == "btcusd"].shape[0]

    for ex in list_of_exchanges:
        for p in list_of_pair:
            sub_df = df.loc[(df.Exchange == ex)]
            sub_df = sub_df.loc[sub_df.Pair == p]
            print(ex)
            print(p)
            print(sub_df.shape)
            if sub_df.shape[0] == ref_shape:
                pass
            elif sub_df.shape[0] == 1569:
                print(ex)
                print(p)
                zero_mat = np.zeros((len(list_of_missing), 6))
                zero_sub_df = pd.DataFrame(zero_mat, columns=df.columns)
                zero_sub_df["Time"] = list_of_missing
                zero_sub_df["Exchange"] = ex
                zero_sub_df["Pair"] = p
                print(zero_sub_df)

                df = df.append(zero_sub_df)

    new_df = df.copy()

    return new_df
