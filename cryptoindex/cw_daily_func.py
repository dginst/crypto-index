# standard library import
import logging
from datetime import datetime, timezone
from typing import Dict

# third party import
import numpy as np
import pandas as pd

# local import
from cryptoindex.data_download import cw_raw_download
from cryptoindex.data_setup import (
    date_gen, daily_fix_miss, pair_vol_fix
)
from cryptoindex.calc import (
    btcusd_average, stable_rate_calc,
    conv_into_usd
)
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing,
    query_mongo, mongo_upload,
    mongo_daily_delete
)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, DAY_IN_SEC,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CONVERSION_FIAT, USDC_EXC_LIST,
    USDT_EXC_LIST, STABLE_COIN, CW_RAW_HEAD
)


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


def cw_daily_download(day_to_download):

    # day_before_TS, _ = days_variable(day_to_download)
    date_long = datetime.fromtimestamp(int(day_to_download))
    date_h = date_long.strftime("%m-%d-%Y")
    cw_raw = pd.DataFrame(columns=CW_RAW_HEAD)

    for Crypto in CRYPTO_ASSET:

        ccy_pair_array = []

        for i in PAIR_ARRAY:

            ccy_pair_array.append(Crypto.lower() + i)

        for exchange in EXCHANGES:

            for cp in ccy_pair_array:

                # create the matrix for the single currency_pair
                # connecting to CryptoWatch website
                cw_raw = cw_raw_download(
                    exchange, cp, cw_raw, str(date_h), str(date_h)
                )

    return cw_raw


def daily_pair_vol_fix2(time_to_fix):

    # defining the query details
    q_dict: Dict[str, int] = {}
    q_dict = {"Time": time_to_fix}

    # querying oin MongoDB collection
    daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"), q_dict)
    daily_mat = daily_mat.loc[daily_mat.Time != 0]
    daily_mat = daily_mat.drop(columns=["Low", "High", "Open"])

    for Crypto in CRYPTO_ASSET:

        ccy_pair_array = []

        for i in PAIR_ARRAY:

            ccy_pair_array.append(Crypto.lower() + i)

        for exchange in EXCHANGES:

            for cp in ccy_pair_array:

                mat = daily_mat.loc[daily_mat["Exchange"] == exchange]
                mat = mat.loc[mat["Pair"] == cp]
                # checking if the matrix is not empty
                if mat.shape[0] > 1:

                    mat["Pair Volume"] = mat["Close Price"] * \
                        mat["Crypto Volume"]

                # put the manipulated data on MongoDB
                try:

                    mongo_upload(mat, "collection_cw_vol_check")

                except TypeError:

                    pass

    return None


def daily_pair_vol_fix(day):

    # defining the query details
    q_dict: Dict[str, int] = {}
    q_dict = {"Time": day}

    # querying on MongoDB collection
    daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"), q_dict)

    try:

        daily_mat = daily_mat.loc[daily_mat.Time != 0]
        daily_mat = daily_mat.drop(columns=["Low", "High", "Open"])

        daily_vol_fix = pair_vol_fix(daily_mat)

    except AttributeError:

        daily_vol_fix = []

    return daily_vol_fix


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


def cw_new_key_mngm(logic_key_df, daily_mat, time_to_check, date_tot_str):

    # selecting only the keys with 0 value
    key_absent = logic_key_df.loc[logic_key_df.logic_value == 0]
    key_absent.drop(columns=["logic_value"])

    # merging the dataframe in order to find the potenatial new keys
    merg_absent = pd.merge(key_absent, daily_mat, on="key", how="left")
    merg_absent.fillna("NaN", inplace=True)
    new_key_df = merg_absent.loc[merg_absent["Close Price"] != "NaN"]

    if new_key_df.empty is False:

        print("Message: New exchange-pair couple(s) found.")
        new_key_list = new_key_df["key"]

        date_tot_int = date_gen(START_DATE)
        # converting the timestamp format date into string
        date_tot_str = [str(single_date) for single_date in date_tot_int]

        for key in new_key_list:

            # updating the logic matrix of exchange-pair keys
            logic_row_update = log_key_update(key, "collection_CW_key")
            mongo_upload(logic_row_update, "collection_CW_key")

            key_hist_df = new_series_composer(
                key, new_key_df, date_tot_str, time_to_check, kind_of_series="CW")

            # upload the dataframe on MongoDB collection "CW_cleandata"
            mongo_upload(key_hist_df, "collection_cw_clean")

    else:
        pass

    return None


def cw_daily_key_mngm(volume_checked_df, time_to_check, date_tot_str):

    if isinstance(volume_checked_df, list):

        volume_checked_tot = query_mongo(DB_NAME,
                                         MONGO_DICT.get("coll_vol_chk"))

        last_day_with_val = max(volume_checked_tot.Time)

        volume_checked_df = volume_checked_tot.loc[volume_checked_tot.Time
                                                   == last_day_with_val]

    # downloading from MongoDB the matrix with the daily values and the
    # matrix containing the exchange-pair logic values
    logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_keys"))

    # creating the exchange-pair couples key for the daily matrix
    volume_checked_df["key"] = volume_checked_df["Exchange"] + \
        "&" + volume_checked_df["Pair"]

    # selecting only the exchange-pair couples present in the historical series
    key_present = logic_key.loc[logic_key.logic_value == 1]
    key_present = key_present.drop(columns=["logic_value"])
    # applying a left join between the prresent keys matrix and the daily
    # matrix, this operation returns a matrix containing all the keys in
    # "key_present" and NaN where some keys are missing
    merged = pd.merge(key_present, volume_checked_df, on="key", how="left")
    # assigning some columns values and substituting NaN with 0
    # in the "merged" df
    merged["Time"] = int(time_to_check)
    split_val = merged["key"].str.split("&", expand=True)
    merged["Exchange"] = split_val[0]
    merged["Pair"] = split_val[1]
    merged.fillna(0, inplace=True)

    #  checking potential new exchange-pair couple

    cw_new_key_mngm(logic_key, volume_checked_df, time_to_check, date_tot_str)

    return merged


def days_variable(day):
    '''
    @param day: "%Y-%m-%d" string format

    '''
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


def daily_fix_miss_op(daily_complete_df, day, collection):

    _, two_before_TS = days_variable(day)
    # defining the query details
    q_dict_time: Dict[str, int] = {}
    q_dict_time = {"Time": int(two_before_TS)}

    # downloading from MongoDB the matrix referring to the previuos day
    day_bfr_mat = query_mongo(
        DB_NAME, MONGO_DICT.get(collection), q_dict_time)

    # add the "key" column
    day_bfr_mat["key"] = day_bfr_mat["Exchange"] + "&" + day_bfr_mat["Pair"]

    # looping through all the daily keys looking for potential missing value
    for key_val in day_bfr_mat["key"]:

        new_val = daily_complete_df.loc[daily_complete_df.key == key_val]

        # if the new 'Close Price' referred to a certain key is 0 the script
        # check the previous day value: if is == 0 then pass, if is != 0
        # the values related to the selected key needs to be corrected
        if np.array(new_val["Close Price"]) == 0.0:

            d_before_val = day_bfr_mat.loc[day_bfr_mat.key == key_val]

            if np.array(d_before_val["Close Price"]) != 0.0:

                price_var = daily_fix_miss(
                    new_val, daily_complete_df, day_bfr_mat)
                # applying the weighted variation to the day before 'Close Price'
                new_price = (1 + price_var) * d_before_val["Close Price"]
                # changing the 'Close Price' value using the new computed price
                daily_complete_df.loc[daily_complete_df.key
                                      == key_val, "Close Price"] = new_price

            else:
                pass

        else:
            pass

    daily_complete_df.drop(columns=["key"])
    daily_complete_df["Time"] = [int(d) for d in daily_complete_df["Time"]]

    daily_fixed_df = daily_complete_df

    return daily_fixed_df


def stable_rates_op(coll_to_query, day_to_comp):

    if day_to_comp is None:

        average_btcusd = btcusd_average(
            DB_NAME, coll_to_query, EXCHANGES)

        usdt_rates = stable_rate_calc(
            DB_NAME, coll_to_query, "USDT", USDT_EXC_LIST, average_btcusd)

        usdc_rates = stable_rate_calc(
            DB_NAME, coll_to_query, "USDC", USDC_EXC_LIST, average_btcusd)

    else:

        average_btcusd = btcusd_average(
            DB_NAME, coll_to_query, EXCHANGES, day_to_comp=str(day_to_comp))

        usdt_rates = stable_rate_calc(
            DB_NAME, coll_to_query, "USDT", USDT_EXC_LIST, average_btcusd, day_to_comp=str(day_to_comp))

        usdc_rates = stable_rate_calc(
            DB_NAME, coll_to_query, "USDC", USDC_EXC_LIST, average_btcusd, day_to_comp=str(day_to_comp))

    return usdt_rates, usdc_rates


def cw_daily_conv_op(day_to_conv_TS):

    # querying the data from mongo
    query_data = {"Time": int(day_to_conv_TS)}
    query_rate = {"Date": str(day_to_conv_TS)}
    query_stable = {"Time": int(day_to_conv_TS)}
    # querying the data from mongo
    matrix_rate = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_ecb_clean"), query_rate)
    matrix_data = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_data)
    matrix_rate_stable = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_stable_rate"), query_stable)

    # converting the "matrix_rate" df
    converted_df = conv_into_usd(
        DB_NAME, matrix_data, matrix_rate,
        matrix_rate_stable, CONVERSION_FIAT, STABLE_COIN)

    return converted_df


def cw_daily_operation(day=None):
    '''
    @param day has to be either None or "%Y-%m-%d" string format

    '''

    # create the indexing for MongoDB and define the variable containing the
    # MongoDB collections where to upload data
    mongo_indexing()

    date_tot = date_gen(START_DATE)
    # converting the timestamp format date into string
    date_tot_str = [str(single_date) for single_date in date_tot]

    day_before_TS, _ = days_variable(day)

    if day is None:

        if daily_check_mongo("coll_cw_raw", {"Exchange": "coinbase-pro", "Pair": "btcusd"}) is False:

            try:
                cw_rawdata_daily = cw_daily_download(day_before_TS + DAY_IN_SEC)
            except Exception:
                logging.error("Exception occurred", exc_info=True)
                logging.info('Daily download from CryptoWatch failed')
            mongo_upload(cw_rawdata_daily, "collection_cw_raw")

        else:

            print("The CW_rawdata collection on MongoDB is updated.")

        if daily_check_mongo("coll_vol_chk", {
                "Exchange": "coinbase-pro", "Pair": "btcusd"}) is False:

            mat_vol_fix = daily_pair_vol_fix(day_before_TS)

            try:

                mongo_upload(mat_vol_fix, "collection_cw_vol_check")

            except AttributeError:
                pass

        else:

            mat_vol_fix = []
            print(
                "Message: No need to fix pair volume. The collection on MongoDB is updated."
            )

        # new and dead crypto-fiat key management

        daily_complete_df = cw_daily_key_mngm(
            mat_vol_fix, day_before_TS, date_tot_str)

        # missing data fixing

        if daily_check_mongo("coll_cw_clean", {"Exchange": "coinbase-pro", "Pair": "btcusd"}) is False:

            daily_fixed_df = daily_fix_miss_op(
                daily_complete_df, day, "coll_cw_clean")
            mongo_upload(daily_fixed_df, "collection_cw_clean")

        else:

            print(
                "Message: The collection cw_clean on MongoDB is updated."
            )

        if daily_check_mongo("coll_stable_rate", {"Currency": "USDT/USD"}) is False:

            usdt_rates, usdc_rates = stable_rates_op(
                "coll_cw_clean", str(day_before_TS))

            mongo_upload(usdt_rates, "collection_stable_rate")
            mongo_upload(usdc_rates, "collection_stable_rate")

        else:

            print("The stable_rates_collection on MongoDB is already updated.")

        if daily_check_mongo("coll_cw_conv", {"Exchange": "coinbase-pro", "Pair": "btcusd"}) is False:

            converted_data = cw_daily_conv_op(day_before_TS)
            mongo_upload(converted_data, "collection_cw_converted")

        else:

            print(
                "Message: The cw_converted_data collection on MongoDB is already updated."
            )

        if daily_check_mongo("coll_cw_final", {"Exchange": "coinbase-pro", "Pair": "btcusd"}) is False:

            mongo_upload(converted_data, "collection_cw_final_data")

        else:

            print(
                "The CW_final_data collection on MongoDB is already updated."
            )

    else:

        cw_rawdata_daily = cw_daily_download(day_before_TS)
        mongo_upload(cw_rawdata_daily, "collection_cw_raw")
        mat_vol_fix = daily_pair_vol_fix(day_before_TS)
        try:

            mongo_upload(mat_vol_fix, "collection_cw_vol_check")

        except AttributeError:
            pass

        daily_complete_df = cw_daily_key_mngm(
            mat_vol_fix, day_before_TS, date_tot_str)
        daily_fixed_df = daily_fix_miss_op(
            daily_complete_df, day, "coll_cw_clean")
        mongo_upload(daily_fixed_df, "collection_cw_clean")
        usdt_rates, usdc_rates = stable_rates_op(
            "coll_cw_clean", str(day_before_TS))
        mongo_upload(usdt_rates, "collection_stable_rate")
        mongo_upload(usdc_rates, "collection_stable_rate")
        converted_data = cw_daily_conv_op(day_before_TS)
        mongo_upload(converted_data, "collection_cw_converted")
        mongo_upload(converted_data, "collection_cw_final_data")

    return None
