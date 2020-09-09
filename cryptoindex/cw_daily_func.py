# standard library import
from datetime import datetime, timezone
from typing import Dict

# third party import
import numpy as np
import pandas as pd

# local import
from cryptoindex.data_download import CW_raw_to_mongo
from cryptoindex.data_setup import (
    date_gen, Diff, timestamp_to_human,
    daily_fix_miss, pair_vol_fix
)
from cryptoindex.calc import (
    btcusd_average, stable_rate_calc,
    conv_into_usd
)
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing, query_mongo, mongo_upload
)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, DAY_IN_SEC,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CONVERSION_FIAT, USDC_EXC_LIST,
    USDT_EXC_LIST, STABLE_COIN
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


def cw_daily_download():

    # defining the array containing all the date from start_period until today
    date_tot = date_gen(START_DATE)
    # converting the timestamp format date into string
    date_tot_str = [str(single_date) for single_date in date_tot]

    # create the indexing for MongoDB and define the variable containing the
    # MongoDB collections where to upload data
    mongo_indexing()
    collection_dict_upload = mongo_coll()

    date_to_add = check_missing(date_tot_str, "coll_cw_raw", {
                                "Exchange": "coinbase-pro", "Pair": "ethusd"})

    if date_to_add != []:

        for date in date_to_add:

            date_long = datetime.fromtimestamp(int(date))
            date_h = date_long.strftime("%m-%d-%Y")

            for Crypto in CRYPTO_ASSET:

                ccy_pair_array = []
                for i in PAIR_ARRAY:
                    ccy_pair_array.append(Crypto.lower() + i)

                for exchange in EXCHANGES:

                    for cp in ccy_pair_array:

                        # create the matrix for the single currency_pair
                        # connecting to CryptoWatch website
                        CW_raw_to_mongo(
                            exchange, cp, collection_dict_upload.get(
                                "collection_cw_raw"), str(date_h)
                        )

                        print('CW rawdata have been correctly downloaded ')
    else:

        print(
            "Message: No new date to download from CryptoWatch,\
            the rawdata collection on MongoDB is updated."
        )

    return None


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

    # querying oin MongoDB collection
    daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"), q_dict)
    daily_mat = daily_mat.loc[daily_mat.Time != 0]
    daily_mat = daily_mat.drop(columns=["Low", "High", "Open"])

    daily_vol_fix = pair_vol_fix(daily_mat)

    return daily_vol_fix


def cw_new_key_mngm(logic_key_df, daily_mat, time_to_check, date_tot_str):

    collection_dict_upload = mongo_coll()
    # selecting only the keys with 0 value
    key_absent = logic_key_df.loc[logic_key_df.logic_value == 0]
    key_absent.drop(columns=["logic_value"])

    # merging the dataframe in order to find the potenatial new keys
    merg_absent = pd.merge(key_absent, daily_mat, on="key", how="left")
    merg_absent.fillna("NaN", inplace=True)
    new_key = merg_absent.loc[merg_absent["Close Price"] != "NaN"]

    if new_key.empty is False:

        print("Message: New exchange-pair couple(s) found.")
        new_key_list = new_key["key"]

        for key in new_key_list:

            q_del = {"key": key}
            collection_dict_upload.get("collection_CW_key").delete_many(q_del)

            # updating the logic matrix of exchange-pair couple
            new_key_log = pd.DataFrame(np.column_stack(
                (key, int(1))), columns=["key", "logic_value"])
            new_key_log["logic_value"] = 1

            # create the historical series of the new couple(s)
            # composed by zeros
            splited_key = key.split("&")
            key_hist_df = pd.DataFrame(date_tot_str, columns=["Time"])
            key_hist_df["Close Price"] = 0
            key_hist_df["Pair Volume"] = 0
            key_hist_df["Crypto Volume"] = 0
            key_hist_df["Exchange"] = splited_key[0]
            key_hist_df["Pair"] = splited_key[1]

            # uploading on MongoDB collections "CW_converted_data" and "CW_final_data"
            # the new series of zero except for the last value (yesterday)
            mongo_upload(key_hist_df, "collection_cw_converted")
            mongo_upload(key_hist_df, "collection_cw_final_data")

            query_for_yst = {"Time": str(time_to_check)}
            collection_dict_upload.get(
                "collection_cw_converted").delete_many(query_for_yst)
            collection_dict_upload.get(
                "collection_cw_final_data").delete_many(query_for_yst)

            # inserting the today value of the new couple(s)
            new_price = np.array(new_key.loc[new_key.key == key, "Close Price"])
            new_p_vol = np.array(new_key.loc[new_key.key == key, "Pair Volume"])
            new_c_vol = np.array(
                new_key.loc[new_key.key == key, "Crypto Volume"])
            key_hist_df.loc[key_hist_df.Time == str(
                time_to_check), "Close Price"] = new_price
            key_hist_df.loc[key_hist_df.Time == str(
                time_to_check), "Pair Volume"] = new_p_vol
            key_hist_df.loc[key_hist_df.Time == str(
                time_to_check), "Crypto Volume"] = new_c_vol

            # upload the dataframe on MongoDB collection "CW_cleandata"
            mongo_upload(key_hist_df, "collection_cw_clean")

            # uploading the updated keys df on the CW_keys collection
            mongo_upload(new_key_log, "collection_CW_key")

    else:

        pass

    return None


def cw_daily_key_mngm(time_to_check, date_tot_str):

    # defining the query details
    q_dict = {"Time": int(time_to_check)}

    # downloading from MongoDB the matrix with the daily values and the
    # matrix containing the exchange-pair logic values
    daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_vol_chk"), q_dict)
    logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_keys"))

    # creating the exchange-pair couples key for the daily matrix
    daily_mat["key"] = daily_mat["Exchange"] + "&" + daily_mat["Pair"]

    # selecting only the exchange-pair couples present in the historical series
    key_present = logic_key.loc[logic_key.logic_value == 1]
    key_present = key_present.drop(columns=["logic_value"])
    # applying a left join between the prresent keys matrix and the daily
    # matrix, this operation returns a matrix containing all the keys in
    # "key_present" and NaN where some keys are missing
    merged = pd.merge(key_present, daily_mat, on="key", how="left")
    # assigning some columns values and substituting NaN with 0
    # in the "merged" df
    merged["Time"] = str(time_to_check)
    split_val = merged["key"].str.split("&", expand=True)
    merged["Exchange"] = split_val[0]
    merged["Pair"] = split_val[1]
    merged.fillna(0, inplace=True)

    #  checking potential new exchange-pair couple ##################

    cw_new_key_mngm(logic_key, daily_mat, time_to_check, date_tot_str)

    return merged


def cw_daily_cleaning():

    # create the indexing for MongoDB and define the variable containing the
    # MongoDB collections where to upload data
    mongo_indexing()

    # assign date of interest to variables
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    y_TS = today_TS - DAY_IN_SEC
    two_before_TS = y_TS - DAY_IN_SEC

    # defining the array containing all the date from start_period until today
    date_complete_int = date_gen(START_DATE)
    # converting the timestamp format date into string
    date_tot_str = [str(single_date) for single_date in date_complete_int]

    # fixing the "Pair Volume" information #################

    date_to_fix = check_missing(date_tot_str, "coll_vol_chk", {
                                "Exchange": "coinbase-pro", "Pair": "ethusd"})

    if date_to_fix != []:

        daily_pair_vol_fix(y_TS)

    else:

        print(
            "Message: No need to fix pair volume. The collection on MongoDB is updated."
        )

    # DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

    daily_fixed_df = cw_daily_key_mngm(y_TS, date_tot_str)

    # MISSING DATA FIXING ##############################

    date_to_add = check_missing(date_tot_str, "coll_cw_clean", {
                                "Exchange": "coinbase-pro", "Pair": "ethusd"})

    if date_to_add != []:

        # defining the query details
        q_dict_time: Dict[str, str] = {}
        q_dict_time = {"Time": str(two_before_TS)}

        # downloading from MongoDB the matrix referring to the previuos day
        day_bfr_mat = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_cw_clean"), q_dict_time)

        # add the "key" column
        day_bfr_mat["key"] = day_bfr_mat["Exchange"] + "&" + day_bfr_mat["Pair"]

        # looping through all the daily keys looking for potential missing value
        for key_val in day_bfr_mat["key"]:

            new_val = daily_fixed_df.loc[daily_fixed_df.key == key_val]

            # if the new 'Close Price' referred to a certain key is 0 the script
            # check the previous day value: if is == 0 then pass, if is != 0
            # the values related to the selected key needs to be corrected
            # ###### IF A EXC-PAIR DIES AT A CERTAIN POINT, THE SCRIPT
            # CHANGES THE VALUES. MIGHT BE WRONG #######################
            if np.array(new_val["Close Price"]) == 0.0:

                d_before_val = day_bfr_mat.loc[day_bfr_mat.key == key_val]

                if np.array(d_before_val["Close Price"]) != 0.0:

                    price_var = daily_fix_miss(
                        new_val, daily_fixed_df, day_bfr_mat)
                    # applying the weighted variation to the day before 'Close Price'
                    new_price = (1 + price_var) * d_before_val["Close Price"]
                    # changing the 'Close Price' value using the new computed price
                    daily_fixed_df.loc[daily_fixed_df.key
                                       == key_val, "Close Price"] = new_price

                else:
                    pass

            else:
                pass

        # put the manipulated data on MongoDB
        daily_fixed_df.drop(columns=["key"])
        daily_fixed_df["Time"] = [str(d) for d in daily_fixed_df["Time"]]
        mongo_upload(daily_fixed_df, "collection_cw_clean")

        print('The CW rawdata have been correctly manipulated and are now ready for conversion')

    else:

        print(
            "Message: The collection cw_clean on MongoDB is updated."
        )

    return None


def stable_rates_op(coll_to_query, coll_where_upload, day_to_comp=None):

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

    mongo_upload(usdt_rates, coll_where_upload)
    mongo_upload(usdc_rates, coll_where_upload)

    return None


def cw_daily_conv():

    # define today date as timestamp
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    y_TS = today_TS - DAY_IN_SEC

    # define the variable containing all the date from start_date to today.
    # the date are displayed as timestamp and each day refers to 12:00 am UTC
    date_tot_int = date_gen(START_DATE)
    date_tot_str = [str(single_date) for single_date in date_tot_int]

    # ########## MongoDB setup ################################

    # create the indexing for MongoDB and define the variable containing the
    # MongoDB collections where to upload data
    mongo_indexing()

    # USDC/USD and USDT/USD computation #####################

    rates_to_add = check_missing(
        date_tot_str, "coll_stable_rate", {"Currency": "USDT/USD"})

    if rates_to_add != []:

        stable_rates_op("coll_cw_clean",
                        "collection_stable_rate", day_to_comp=str(y_TS))

    else:

        print(
            "Message: No need to compute new rates. The stable rates collection on MongoDB is updated."
        )

    # ################# DAILY DATA CONVERSION MAIN PART ##################

    date_to_convert = check_missing(date_tot_str, "coll_cw_conv", {
        "Exchange": "coinbase-pro", "Pair": "ethusd"})

    if date_to_convert != []:

        # querying the data from mongo
        query_data = {"Time": str(y_TS)}
        query_rate = {"Date": str(y_TS)}
        query_stable = {"Time": str(y_TS)}
        # querying the data from mongo
        matrix_rate = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_ecb_clean"), query_rate)
        matrix_data = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_data)
        matrix_rate_stable = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_stable_rate"), query_stable)

        # converting the "matrix_rate" df
        converted_data = conv_into_usd(
            DB_NAME, matrix_data, matrix_rate,
            matrix_rate_stable, CONVERSION_FIAT, STABLE_COIN)

        # uploading converted data on MongoDB
        mongo_upload(converted_data, "collection_cw_converted")

    else:

        print(
            "Message: No need to convert. The cw converted data collection on MongoDB is updated."
        )

    # ################### CW_final_data upload ##################################

    date_to_upload = check_missing(date_tot_str, "coll_cw_final", {
        "Exchange": "coinbase-pro", "Pair": "ethusd"})

    if date_to_upload != []:

        for date in date_to_upload:

            query_dict = {"Time": str(date)}

            # retriving the needed information on MongoDB
            matrix = query_mongo(DB_NAME, MONGO_DICT.get(
                "coll_cw_conv"), query_dict)

            # put the manipulated data on MongoDB
            mongo_upload(matrix, "collection_cw_final_data")

            print(
                'CW data are now converted. Is it possible to compute the index-value right now.')
    else:

        print(
            "Message: No new date to upload on on Mongo DB, the CW_final_data \
            collection on MongoDB is updated."
        )

    return None


def cw_daily_operation(day=None):

    mat_vol_fix = daily_pair_vol_fix(day)
    mongo_upload(mat_vol_fix, "collection_cw_vol_check")

    return None
