from datetime import datetime

import numpy as np
import pandas as pd

from cryptoindex.calc import conv_into_usd, key_log_mat
from cryptoindex.config import (CLEAN_DATA_HEAD, CONVERSION_FIAT, CRYPTO_ASSET,
                                CW_RAW_HEAD, DB_NAME, EXCHANGES, MONGO_DICT,
                                PAIR_ARRAY, STABLE_COIN, START_DATE)
from cryptoindex.cw_daily_func import stable_rates_op
from cryptoindex.data_download import cw_raw_download
from cryptoindex.data_setup import (CW_series_fix_missing, Diff, date_gen,
                                    fix_zero_value, homogenize_dead_series,
                                    homogenize_series, make_unique,
                                    pair_vol_fix)
from cryptoindex.mongo_setup import (df_reorder, mongo_coll, mongo_coll_drop,
                                     mongo_indexing, mongo_upload, query_mongo)


def check_missing(tot_date_arr, coll_to_check, query, days_to_check=10):

    # selecting the last five days and put them into an array
    last_days = tot_date_arr[(
        len(tot_date_arr) - days_to_check): len(tot_date_arr)]
    print(last_days)
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


def start_stop_missing(date_to_add, series_to_check):

    [int(date) for date in date_to_add]
    date_to_add.sort()
    start_TS = date_to_add[0]
    end_TS = date_to_add[len(date_to_add) - 1]

    if series_to_check == "ECB":

        start_date = datetime.utcfromtimestamp(start_TS).strftime("%Y-%m-%d")
        stop_date = datetime.utcfromtimestamp(end_TS).strftime("%Y-%m-%d")

    elif series_to_check == "CW":

        start_date = datetime.utcfromtimestamp(start_TS).strftime("%m-%d-%Y")
        stop_date = datetime.utcfromtimestamp(end_TS).strftime("%m-%d-%Y")

    return start_date, stop_date


def cw_hist_download(start_date, end_date=None):

    if end_date is None:

        # set end_date as today
        end_date = datetime.now().strftime("%m-%d-%Y")

    else:
        pass

    df = pd.DataFrame(columns=CW_RAW_HEAD)

    for Crypto in CRYPTO_ASSET:
        print(Crypto)

        ccy_pair_array = []
        for i in PAIR_ARRAY:

            ccy_pair_array.append(Crypto.lower() + i)

        for exchange in EXCHANGES:

            for cp in ccy_pair_array:

                # create the matrix for the single currency_pair connecting
                # to CryptoWatch website
                df = cw_raw_download(
                    exchange, cp, df, start_date, end_date
                )

    return df


def cw_hist_raw_bugfix(bug_mat):

    # fixing the "Pair Volume" information
    bug_mat['key'] = bug_mat['Exchange'] + '&' + \
        bug_mat['Pair']
    # correct the "Crypto Volume" information for the date 2017-04-28 and
    # 2017-04-29 using the previous day value
    m_27_04 = bug_mat.loc[bug_mat.Time
                          == 1493251200, ['key', 'Crypto Volume']]

    m_28_04 = bug_mat.loc[bug_mat.Time == 1493337600]
    m_29_04 = bug_mat.loc[bug_mat.Time == 1493424000]

    for k in m_27_04['key']:

        prev_vol = np.array(m_27_04.loc[m_27_04.key == k, "Crypto Volume"])
        m_28_04.loc[m_28_04.key == k, "Crypto Volume"] = prev_vol
        m_29_04.loc[m_29_04.key == k, "Crypto Volume"] = prev_vol

    bug_mat = bug_mat.loc[bug_mat.Time != 1493337600]
    bug_mat = bug_mat.loc[bug_mat.Time != 1493424000]

    new_mat = bug_mat.append(m_28_04)
    new_mat = new_mat.append(m_29_04)
    new_mat = new_mat.sort_values(by=['Time'])
    new_mat = new_mat.drop(columns=['key'])

    # correcting some wrong values for bittrex - btcusdt

    sub_mat = new_mat.loc[new_mat["Exchange"] == "bittrex"]
    sub_mat = sub_mat.loc[sub_mat["Pair"] == "btcusdt"]

    value_to_sub = np.array(
        sub_mat.loc[sub_mat.Time == 1544400000, "Crypto Volume"])

    new_mat.loc[(new_mat.Time == 1544486400)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusdt"),
                "Crypto Volume"] = value_to_sub

    new_mat.loc[(new_mat.Time == 1544572800)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusdt"),
                "Crypto Volume"] = value_to_sub

    # correcting some wrong values for bittrex - btcusd

    sub_mat_2 = new_mat.loc[new_mat["Exchange"] == "bittrex"]
    sub_mat_2 = sub_mat_2.loc[sub_mat_2["Pair"] == "btcusd"]

    value_to_sub = np.array(
        sub_mat_2.loc[sub_mat_2.Time == 1544400000, "Crypto Volume"])

    new_mat.loc[(new_mat.Time == 1544486400)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusd"),
                "Crypto Volume"] = value_to_sub

    new_mat.loc[(new_mat.Time == 1544572800)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusd"),
                "Crypto Volume"] = value_to_sub

    return new_mat


def cw_hist_pair_vol_fix(hist_raw_mat):

    raw_matrix = hist_raw_mat.loc[hist_raw_mat.Time != 0]
    raw_matrix = raw_matrix.drop(columns=["Low", "High", "Open"])

    # remove potential duplicate values
    tot_matrix = make_unique(raw_matrix)

    # correcting some df issues related to original raw data
    bug_fixed_df = cw_hist_raw_bugfix(tot_matrix)

    # fixing the Pair_Vol info of the df
    vol_fixed_df = pair_vol_fix(bug_fixed_df)

    return vol_fixed_df


def crypto_fiat_pair_gen(crypto):

    pair_arr = []

    for fiat in PAIR_ARRAY:

        pair_arr.append(crypto.lower() + fiat)

    return pair_arr


def cw_hist_cleaning(vol_fixed_df, start_date, crypto_list=CRYPTO_ASSET, exc_list=EXCHANGES):

    tot_date_arr = date_gen(start_date)

    cleaned_df = pd.DataFrame(columns=CLEAN_DATA_HEAD)

    for crypto in crypto_list:

        pair_arr = crypto_fiat_pair_gen(crypto)

        for exchange in exc_list:

            ex_matrix = vol_fixed_df.loc[vol_fixed_df["Exchange"] == exchange]

            for cp in pair_arr:

                crypto = cp[:3]

                cp_matrix = ex_matrix.loc[ex_matrix["Pair"] == cp]
                cp_matrix = cp_matrix.drop(columns=["Exchange", "Pair"])
                # checking if the matrix is not empty
                if cp_matrix.shape[0] > 1:

                    # check if the historical series start at the same date as
                    # the start date if not fill the dataframe with zero values
                    cp_matrix = homogenize_series(
                        cp_matrix, tot_date_arr)

                    # check if the series stopped at certain point in
                    # the past, if yes fill with zero
                    cp_matrix = homogenize_dead_series(
                        cp_matrix, tot_date_arr)

                    # checking if the matrix has missing data and if ever fixing it
                    if cp_matrix.shape[0] != tot_date_arr.size:

                        print("fixing")
                        cp_matrix = CW_series_fix_missing(
                            cp_matrix,
                            exchange,
                            cp,
                            tot_date_arr,
                            DB_NAME,
                            MONGO_DICT.get("coll_vol_chk"),
                        )

                    # turn Time columns into string
                    [str(date) for date in cp_matrix["Time"]]

                    # add exchange and currency_pair column
                    cp_matrix["Exchange"] = exchange
                    cp_matrix["Pair"] = cp
                    reordered = df_reorder(
                        cp_matrix, column_set="conversion")
                    cleaned_df = cleaned_df.append(reordered)

    return cleaned_df


def cw_hist_zero_vol_fill_op(converted_df, head=CLEAN_DATA_HEAD):

    converted_df["Crypto"] = converted_df["Pair"] # .str[:3]

    for f in ["usdt", "usdc", "gbp", "usd", "cad", "jpy", "eur"]:
        converted_df["Crypto"] = [x.replace(f, "") for x in converted_df["Crypto"]]

    final_matrix = pd.DataFrame(columns=head)

    for crypto in CRYPTO_ASSET:

        cry_matrix = converted_df.loc[converted_df.Crypto == crypto.lower()]
        exc_list = list(converted_df["Exchange"].unique())

        for exc in exc_list:

            ex_matrix = cry_matrix.loc[cry_matrix.Exchange == exc]
            ex_matrix.drop(columns=["Crypto"])
            # finding the crypto-fiat pair in the selected exchange
            pair_list = list(ex_matrix["Pair"].unique())

            for pair in pair_list:

                cp_matrix = ex_matrix.loc[ex_matrix.Pair == pair]
                # checking if the matrix is not empty
                try:

                    if cp_matrix.shape[0] > 1:
                        
                        print(cp_matrix)
                        cp_matrix = fix_zero_value(cp_matrix)

                        final_matrix = final_matrix.append(cp_matrix)

                except AttributeError:
                    pass

    return final_matrix


def cw_hist_conv_op(cleaned_df, conv_fiat=CONVERSION_FIAT, stable=STABLE_COIN):

    matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"))
    matrix_rate_stable = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_stable_rate"))

    # converting the "matrix_rate" df
    converted_df = conv_into_usd(
        DB_NAME, cleaned_df, matrix_rate,
        matrix_rate_stable, conv_fiat, stable)

    return converted_df


def cw_hist_download_op(start_date=START_DATE):

    # deleting previous MongoDB collection for rawdata
    mongo_coll_drop("cw_hist_down")
    collection_dict_upload = mongo_coll()

    print("Downloading all CW history...")
    cw_raw_data = cw_hist_download(start_date)
    mongo_upload(cw_raw_data, "collection_cw_raw")
    print("CW series download completed")

    # deleting 31/12/2015 values if present
    last_2015_TS = 1451520000
    query_ = {'Time': last_2015_TS}
    collection_dict_upload.get("collection_cw_raw").delete_many(query_)

    return None


def cw_hist_operation(start_date=START_DATE):

    date_tot = date_gen(start_date)
    last_day_TS = date_tot[len(date_tot) - 1]

    mongo_indexing()

    # deleting previous MongoDB collections
    mongo_coll_drop("cw_hist_clean")
    mongo_coll_drop("cw_hist_conv")

    # fix and upload the series for the "pair volume" info
    tot_raw_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"))
    cw_vol_fix_data = cw_hist_pair_vol_fix(tot_raw_data)
    mongo_upload(cw_vol_fix_data, "collection_cw_vol_check")

    # clean and upload all the series
    cleaned_df = cw_hist_cleaning(cw_vol_fix_data, start_date)
    mongo_upload(cleaned_df, "collection_cw_clean")

    # compute and upload USDC and USDT rates series
    usdt_rates, usdc_rates = stable_rates_op(
        "coll_cw_clean", None)
    mongo_upload(usdt_rates, "collection_stable_rate")
    mongo_upload(usdc_rates, "collection_stable_rate")

    # convert and upload all the data into USD
    converted_df = cw_hist_conv_op(cleaned_df)
    mongo_upload(converted_df, "collection_cw_converted")

    # logic matrix of crypto-fiat keys
    key_df = key_log_mat(DB_NAME, "coll_cw_conv", last_day_TS,
                         EXCHANGES, CRYPTO_ASSET, PAIR_ARRAY)
    mongo_upload(key_df, "collection_CW_key")
    mongo_upload(key_df, "collection_EXC_key")

    # fill zero-volume data and upload on MongoDB
    final_df = cw_hist_zero_vol_fill_op(converted_df)
    mongo_upload(final_df, "collection_cw_final_data")

    return None
