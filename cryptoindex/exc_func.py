# standard library import
from datetime import datetime, timezone
from typing import Dict

import numpy as np
# third party import
import pandas as pd

from cryptoindex.config import (DAY_IN_SEC, DB_NAME, EXCHANGES, MONGO_DICT,
                                START_DATE, STABLE_COIN, CONVERSION_FIAT)
# local import
from cryptoindex.data_setup import (daily_fix_miss, date_gen, Diff,
                                    exc_pair_cleaning, pair_vol_fix)
from cryptoindex.mongo_setup import (mongo_coll, mongo_upload,
                                     query_mongo)


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

        query_to_del = {"Time": str(day_to_check)}
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
    key_hist_df.loc[key_hist_df.Time == str(
        day_to_check), "Close Price"] = new_price
    key_hist_df.loc[key_hist_df.Time == str(
        day_to_check), "Pair Volume"] = new_p_vol
    key_hist_df.loc[key_hist_df.Time == str(
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
        date_tot_str = [str(single_date) for single_date in date_tot_int]

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
    daily_mat_final['Time'] = [str(t) for t in daily_mat_final['Time']]

    return daily_mat_final


def exc_daily_cleaning(exc_list, date_tot_arr, day_to_clean=None):

    if day_to_clean is None:

        today_str = datetime.now().strftime("%Y-%m-%d")
        today_date = datetime.strptime(today_str, "%Y-%m-%d")
        today_TS = int(today_date.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = today_TS - DAY_IN_SEC
        two_before_TS = day_before_TS - DAY_IN_SEC

        date_to_clean = check_missing(
            date_tot_arr, "coll_exc_clean", {
                "Exchange": "coinbase-pro", "Pair": "ethusd"})

        if date_to_clean != []:

            # download from MongoDB the exc raw data of yesterday
            q_dict: Dict[str, str] = {}
            q_dict = {"Time": str(day_before_TS)}
            daily_mat = query_mongo(
                DB_NAME, MONGO_DICT.get("coll_exc_raw"), q_dict)

            # fixing the "pair volume" information in the daily df
            daily_mat_vol_fix = daily_exc_pair_vol_fix(daily_mat, exc_list)

            # creating different df based on the hour of download
            (daily_mat_00, daily_mat_12, _,
             _) = exc_time_split(daily_mat_vol_fix)

            # completing the daily matrix with dead crypto-fiat pair
            daily_mat_complete = exc_daily_key_mngm(
                daily_mat_00, daily_mat_12, day_before_TS)

            # downloading from MongoDB the matrix referring to the previuos day
            q_dict_bfr: Dict[str, str] = {}
            q_dict_bfr = {"Time": str(two_before_TS)}
            day_bfr_mat = query_mongo(
                DB_NAME, MONGO_DICT.get("coll_exc_clean"), q_dict_bfr)

            fixed_daily_mat = exc_daily_fix_op(day_bfr_mat, daily_mat_complete)

        else:

            print("The collection EXC_cleandata is already updated")

    else:

        day_date = datetime.strptime(day_to_clean, "%Y-%m-%d")
        day_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = day_TS - DAY_IN_SEC

        # download from MongoDB the exc raw data of the day before
        q_dict: Dict[str, str] = {}
        q_dict = {"Time": str(day_TS)}
        daily_mat = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_exc_raw"), q_dict)

        # fixing the "pair volume" information in the daily df
        daily_mat_vol_fix = daily_exc_pair_vol_fix(daily_mat, exc_list)

        # creating different df based on the hour of download
        (daily_mat_00, daily_mat_12, _, _) = exc_time_split(daily_mat_vol_fix)

        # completing the daily matrix with dead crypto-fiat pair
        daily_mat_complete = exc_daily_key_mngm(
            daily_mat_00, daily_mat_12, day_TS)

        # downloading from MongoDB the matrix referring to the previuos day
        q_dict_bfr: Dict[str, str] = {}
        q_dict_bfr = {"Time": str(day_before_TS)}
        day_bfr_mat = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_exc_clean"), q_dict_bfr)

        fixed_daily_mat = exc_daily_fix_op(day_bfr_mat, daily_mat_complete)

    return fixed_daily_mat


def exc_daily_conversion(conversion_fiat, stablecoin, date_tot_arr, day_to_conv=None):

    if day_to_conv is None:

        today_str = datetime.now().strftime("%Y-%m-%d")
        today_date = datetime.strptime(today_str, "%Y-%m-%d")
        today_TS = int(today_date.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = today_TS - DAY_IN_SEC

        date_to_conv = check_missing(
            date_tot_arr, "coll_exc_final", {
                "Exchange": "coinbase-pro", "Pair": "ethusd"})

        if date_to_conv != []:

            converted_df = exc_conv_op(
                day_before_TS, conversion_fiat, stablecoin)

        else:

            print("The collection EXC_final_data is already updated")

    else:

        day_date = datetime.strptime(day_to_conv, "%Y-%m-%d")
        day_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = day_TS - DAY_IN_SEC

        converted_df = exc_conv_op(day_before_TS, conversion_fiat, stablecoin)

    return converted_df


def exc_conv_op(day_to_conv_TS, conversion_fiat, stablecoin):

    query_data = {"Time": str(day_to_conv_TS)}
    query_rate = {"Date": str(day_to_conv_TS)}
    query_stable = {"Time": str(day_to_conv_TS)}
    matrix_rate = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_ecb_clean"), query_rate)
    matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
    matrix_data = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_exc_clean"), query_data)
    matrix_rate_stable = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_stable_rate"), query_stable)

    # creating a column containing the fiat currency
    matrix_rate["fiat"] = [x[:3].lower() for x in matrix_rate["Currency"]]
    matrix_data["fiat"] = [x[3:].lower() for x in matrix_data["Pair"]]
    matrix_rate_stable["fiat"] = [x[:4].lower()
                                  for x in matrix_rate_stable["Currency"]]

    # creating a USD subset which will not be converted
    usd_matrix = matrix_data.loc[matrix_data["fiat"] == "usd"]
    usd_matrix = usd_matrix[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]

    #  converting non-USD fiat currencies
    conv_matrix = matrix_data.loc[matrix_data["fiat"].isin(conversion_fiat)]
    # merging the dataset on 'Time' and 'fiat' column
    conv_merged = pd.merge(conv_matrix, matrix_rate, on=["Time", "fiat"])

    # converting the prices in usd
    conv_merged["Close Price"] = conv_merged["Close Price"] / \
        conv_merged["Rate"]
    conv_merged["Close Price"] = conv_merged["Close Price"].replace(
        [np.inf, -np.inf], np.nan
    )
    conv_merged["Close Price"].fillna(0, inplace=True)
    conv_merged["Pair Volume"] = conv_merged["Pair Volume"] / \
        conv_merged["Rate"]
    conv_merged["Pair Volume"] = conv_merged["Pair Volume"].replace(
        [np.inf, -np.inf], np.nan
    )
    conv_merged["Pair Volume"].fillna(0, inplace=True)

    # subsetting the dataset with only the relevant columns
    conv_merged = conv_merged[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]
    ]

    # converting STABLECOINS currencies

    # creating a matrix for stablecoins]
    stablecoin_matrix = matrix_data.loc[matrix_data["fiat"].isin(stablecoin)]

    # merging the dataset on 'Time' and 'fiat' column
    stable_merged = pd.merge(
        stablecoin_matrix, matrix_rate_stable, on=["Time", "fiat"])

    # converting the prices in usd
    stable_merged["Close Price"] = stable_merged["Close Price"] / \
        stable_merged["Rate"]
    stable_merged["Close Price"] = stable_merged["Close Price"].replace(
        [np.inf, -np.inf], np.nan
    )
    stable_merged["Close Price"].fillna(0, inplace=True)
    stable_merged["Pair Volume"] = stable_merged["Pair Volume"] / \
        stable_merged["Rate"]
    stable_merged["Pair Volume"] = stable_merged["Pair Volume"].replace(
        [np.inf, -np.inf], np.nan
    )
    stable_merged["Pair Volume"].fillna(0, inplace=True)

    # subsetting the dataset with only the relevant columns
    stable_merged = stable_merged[
        ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]
    ]

    # reunite the dataframes and put data on MongoDB
    converted_data = conv_merged
    converted_data = converted_data.append(stable_merged)
    converted_data = converted_data.append(usd_matrix)

    return converted_data


def exc_daily_operation(day=None):

    date_tot_int = date_gen(START_DATE)
    # converting the timestamp format date into string
    date_tot_str = [str(single_date) for single_date in date_tot_int]

    cleaned_df = exc_daily_cleaning(EXCHANGES, date_tot_str, day)
    mongo_upload(cleaned_df, "collection_exc_clean")
    converted_df = exc_daily_conversion(
        CONVERSION_FIAT, STABLE_COIN, date_tot_str, day)
    mongo_upload(converted_df, "collection_exc_final_data")

    return None
