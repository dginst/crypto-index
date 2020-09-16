
# standard library import
from datetime import datetime
import numpy as np
import pandas as pd

# local import

from cryptoindex.data_download import (
    cw_raw_download
)
from cryptoindex.data_setup import (CW_series_fix_missing, date_gen,
                                    homogenize_dead_series, homogenize_series,
                                    make_unique, pair_vol_fix, Diff)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll, mongo_coll_drop,
    mongo_indexing, mongo_upload, df_reorder)
from cryptoindex.config import (
    START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CW_RAW_HEAD, CLEAN_DATA_HEAD
)


def check_missing(tot_date_arr, coll_to_check, query, days_to_check=50):

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

    last_days_db = date_list[(len(date_list) - days_to_check): len(date_list)]
    # last_days_db_str = [str(single_date)
    #                     for single_date in last_days_db]

    # finding the date to download as difference between
    # complete array of date and date now stored on MongoDB
    date_to_add = Diff(last_days, last_days_db)
    print(date_to_add)

    if len(date_to_add) > 20:

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

    sub_mat = new_mat.loc[new_mat["Exchange"] == "bittrex"]
    sub_mat = sub_mat.loc[sub_mat["Pair"] == "btcusdt"]

    value_to_sub = np.array(
        sub_mat.loc[sub_mat.Time == 1544486400, "Crypto Volume"])

    new_mat.loc[(new_mat.Time == 1544572800)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusdt"),
                "Crypto Volume"] = value_to_sub

    new_mat.loc[(new_mat.Time == 1544659200)
                & (new_mat["Exchange"] == "bittrex")
                & (new_mat["Pair"] == "btcusdt"),
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


def cw_hist_operation(start_date=START_DATE):

    date_tot = date_gen(start_date)
    print(date_tot)
    mongo_indexing()
    # deleting previous MongoDB collection for rawdata
    raw_to_download = check_missing(date_tot, "coll_cw_raw", {
                                    "Exchange": "coinbase-pro", "Pair": "btcusd"})
    print(raw_to_download)
    if raw_to_download != [] and raw_to_download is not None:

        start, stop = start_stop_missing(raw_to_download, series_to_check="CW")
        print("Downloading from ", start, " to ", stop)
        cw_raw_data = cw_hist_download(start, stop)
        mongo_upload(cw_raw_data, "collection_cw_raw")

    elif raw_to_download is None:

        print("Downloading all CW history")
        mongo_coll_drop("cw_hist_d")
        cw_raw_data = cw_hist_download(start_date)
        mongo_upload(cw_raw_data, "collection_cw_raw")

    else:

        print("cw_rawdata is updated")

    # deleting previous MongoDB collection for cleandata
    mongo_coll_drop("cw_hist_clean")
    tot_raw_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"))
    cw_vol_fix_data = cw_hist_pair_vol_fix(tot_raw_data)
    print(cw_vol_fix_data)
    mongo_upload(cw_vol_fix_data, "collection_cw_vol_check")

    cleaned_df = cw_hist_cleaning(cw_vol_fix_data, start_date)
    mongo_upload(cleaned_df, "collection_cw_clean")

    mongo_coll_drop("cw_hist_conv")

    return None
