# standard library import
from datetime import datetime, timezone

# third party import
# from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
from . import mongo_setup as mongo


# TIME AND TIME ARRAYS FUNCTIONS

# function that generate an array of date in timstamp format starting from
# start_date to end_date given in mm-dd-yyyy format;
# if not specified end_date = today()
# function only returns timestamp value in second since epoch where every
# day is in the exact 12:00 am UTC
# function considers End of Day price series so, if not otherwise specified,
# the returned array of date will be from start to today - 1 (EoD = 'Y')


def date_gen(start_date, end_date=None, timeST="Y", clss="array", EoD="Y"):

    if end_date is None:

        end_date = datetime.now().strftime("%m-%d-%Y")

    date_index = pd.date_range(start_date, end_date)

    if timeST == "Y":

        date_ll = [
            int(date.replace(tzinfo=timezone.utc).timestamp()) for date in date_index
        ]

    else:

        date_ll = [datetime.strftime(date, "%m-%d-%Y") for date in date_index]

    if clss == "array":

        date_ll = np.array(date_ll)

    if EoD == "Y":

        date_ll = date_ll[: len(date_ll) - 1]

    return date_ll


def timestamp_to_human(ts_array, date_format="%Y-%m-%d"):

    human_date = [datetime.fromtimestamp(int(float(date))) for date in ts_array]
    human_date = [date.strftime(date_format) for date in human_date]

    return human_date


# function that creates a date array in timestamp format adding the choosen
# lag (1 day = 86400 sec on default) the input start and stop has to be
# timestamp date in INTEGER format


def timestamp_vector(start, stop, lag=86400):

    array = np.array([start])

    single_date = start + lag
    while single_date != stop:

        array = np.append(array, single_date)
        single_date = single_date + lag

    return array


# DATA FIXING FUNCTIONS


# function that returns a list containing the elements of list_1 (bigger one)
# not included in list_2 (smaller one)


def Diff(list_1, list_2):

    return list(set(list_1) - set(list_2))


# function that aims to homogenize the crypto-fiat series downloaded from
# CryptoWatch substituting the first n missing values of the series with 0
# values doing that the Dataframe related on all the crypto-fiat series would
# be of the same dimension and, at the same time, does not affects the
# computation because also the volume is set to 0.
# The function uses on default 5 days in order to asses if a series lacking of
# the first n days


def homogenize_series(series_to_check, reference_date_array_TS, days_to_check=1):

    reference_date_array_TS = np.array(reference_date_array_TS)
    header = list(series_to_check.columns)
    test_matrix = series_to_check.loc[
        series_to_check.Time.between(
            reference_date_array_TS[0],
            reference_date_array_TS[days_to_check],
            inclusive=True,
        ),
        header,
    ]

    if test_matrix.empty is True:

        first_date = np.array(series_to_check["Time"].iloc[0])
        # last_missing_date = (int(first_date) - 86400) ##
        first_missing_date = reference_date_array_TS[0]
        missing_date_array = timestamp_vector(first_missing_date, first_date)

        header.remove("Time")

        zero_mat = np.zeros((len(missing_date_array), 3))
        zero_df = pd.DataFrame(zero_mat, columns=header)
        zero_df["Time"] = missing_date_array

        complete_series = zero_df.append(series_to_check)

    else:

        complete_series = series_to_check

    complete_series = complete_series.reset_index(drop=True)

    return complete_series


# add description


def homogenize_dead_series(series_to_check, reference_date_array_TS, days_to_check=5):

    reference_date_array_TS = np.array(reference_date_array_TS)
    header = list(series_to_check.columns)
    last_day = reference_date_array_TS[len(reference_date_array_TS) - 1]
    first_check_day = reference_date_array_TS[
        len(reference_date_array_TS) - 1 - days_to_check
    ]
    test_matrix = series_to_check.loc[
        series_to_check.Time.between(
            first_check_day, last_day, inclusive=True), header
    ]

    if test_matrix.empty is True:

        print("inside")
        last_date = np.array(
            series_to_check["Time"].iloc[len(series_to_check["Time"]) - 1]
        )
        first_missing_date = int(last_date) + 86400
        last_missing_date = last_day

        missing_date_array = timestamp_vector(
            first_missing_date, last_missing_date)

        # new_series = pd.DataFrame(missing_date_array, columns = ['Time'])
        header.remove("Time")

        zero_mat = np.zeros((len(missing_date_array), 3))
        zero_df = pd.DataFrame(zero_mat, columns=header)
        zero_df["Time"] = missing_date_array
        # for element in header:

        #     new_series[element] = np.zeros(len(missing_date_array))

        complete_series = series_to_check.append(zero_df)

    else:

        complete_series = series_to_check

    complete_series = complete_series.reset_index(drop=True)

    return complete_series


# the function allows to fix potential zero values founded in "Crypto Volume"
# and "Pair Volume" it takes the Volume values of the previuos day and
# substitue it into the days with 0-values


def fix_zero_value(matrix):

    val_sum = 0

    for date in matrix["Time"]:

        value_to_check = np.array(
            matrix.loc[matrix.Time == date, "Crypto Volume"])
        price_check = np.array(matrix.loc[matrix.Time == date, "Close Price"])

        if val_sum != 0 and int(value_to_check) == 0:

            if int(price_check) == 0:

                previous_price = np.array(
                    matrix.loc[matrix.Time == str(
                        int(date) - 86400), "Close Price"]
                )

                matrix.loc[matrix.Time == date, "Close Price"] = previous_price

            previous_c_vol = np.array(
                matrix.loc[matrix.Time == str(
                    int(date) - 86400), "Crypto Volume"]
            )
            previous_p_vol = np.array(
                matrix.loc[matrix.Time == str(int(date) - 86400), "Pair Volume"]
            )
            matrix.loc[matrix.Time == date, "Crypto Volume"] = previous_c_vol
            matrix.loc[matrix.Time == date, "Pair Volume"] = previous_p_vol

        val_sum = val_sum + value_to_check

    return matrix


# DAILY AND HISTORICAL ECB RATES SETUP FUNCTIONS

# function returns a matrix of exchange rates USD based that contains
# Date, Exchange indicator (ex. USD/GBP) and rate of a defined period
# retrieving data from the website of European Central Bank
# the function, if data is missing (holiday and weekends), finds the
# first previous day with data and takes its values inputs are:
# key_curr_vector that passes the list of currencies of interest
# start_Period and End_Period


def ECB_setup(key_curr_vector, start_period, End_Period, timeST="N"):

    # defining the array of date to be used

    date_ECB = date_gen(start_period, End_Period, EoD="Y")
    # date = timestamp_convert(date_TS)
    # date = [datetime.strptime(x, '%Y-%m-%d') for x in date]

    # defining the headers of the returning data frame
    header = ["Date", "Currency", "Rate"]

    # for each date in "date" array the funcion retrieves data from
    # ECB website and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    for i, single_date in enumerate(date_ECB):

        database = "index"
        collection = "ecb_raw"
        query = {"TIME_PERIOD": str(date_ECB[i])}

        # retrieving data from MongoDB 'index' and 'ecb_raw' collection
        single_date_ex_matrix = mongo.query_mongo(database, collection, query)

        # check if rates exist in the specified date
        if len(single_date_ex_matrix) != 0:

            # find the USD/EUR rates useful for conversions
            cambio_USD_EUR = float(
                np.array(
                    single_date_ex_matrix.loc[
                        single_date_ex_matrix.CURRENCY == "USD", "OBS_VALUE"
                    ]
                )
            )

            # add a column to DF with the USD based rates
            single_date_ex_matrix["USD based rate"] = (
                single_date_ex_matrix["OBS_VALUE"]
            ) / cambio_USD_EUR

            # creat date array
            date_arr = np.full(len(key_curr_vector), single_date)

            # creating the array with 'XXX/USD' format
            curr_arr = single_date_ex_matrix["CURRENCY"] + "/USD"
            curr_arr = np.where(curr_arr == "USD/USD", "EUR/USD", curr_arr)

            # creating the array with rate values USD based
            rate_arr = single_date_ex_matrix["USD based rate"]
            rate_arr = np.where(
                rate_arr == 1.000000,
                1 / single_date_ex_matrix["OBS_VALUE"][0],
                rate_arr,
            )

            # stacking the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            # filling the return matrix
            if Exchange_Matrix.size == 0:

                Exchange_Matrix = array

            else:

                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

        # if the query returns an empty matrix, function will takes values of
        # the last useful day
        else:

            date_arr = np.full(len(key_curr_vector), single_date)

            # take the curr_arr values of the previous day
            curr_arr = curr_arr

            # take the rate_arr values of the pevious day
            rate_arr = rate_arr

            # stack the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            if Exchange_Matrix.size == 0:

                Exchange_Matrix = array

            else:

                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    if timeST != "N":

        for j, element in enumerate(Exchange_Matrix[:, 0]):

            to_date = datetime.strptime(element, "%Y-%m-%d")
            today_TS = int(to_date.replace(tzinfo=timezone.utc).timestamp())

            Exchange_Matrix[j, 0] = today_TS

    return pd.DataFrame(Exchange_Matrix, columns=header)


# function returns a matrix of exchange rates USD based that contains Date,
# Exchange indicator (ex. USD/GBP) and rate for TODAY retrieving data from
# the website of European Central Bank the function, if data is missing
# (holiday and weekends), finds the first previous day with data and takes
# its values input: key_curr_vector that passes the list of currencies of
# interest


def ECB_daily_setup(key_curr_vector):

    # defining the array of date to be used

    day_in_sec = 86400

    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")

    # timestamp date
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    y_TS = today_TS - day_in_sec
    two_before_TS = y_TS - day_in_sec

    # human format date
    yest_h = timestamp_to_human([y_TS])

    # defining the headers of the returning data frame
    header = ["Currency", "Rate"]

    # defining the MongoDB path where to look for the rates
    db = "index"
    raw_coll = "ecb_raw"
    clean_coll = "ecb_clean"

    # retrieving data from MongoDB 'index' and 'ecb_raw' collection
    ecb_raw_mat = mongo.query_mongo(db, raw_coll)

    # searching into the df only the values referred to yesterday
    y_ecb_raw = ecb_raw_mat.loc[ecb_raw_mat.TIME_PERIOD == str(y_TS)]

    if y_ecb_raw.empty is False:

        # find the USD/EUR rates useful for conversions
        exc_USD_EUR = float(
            np.array(
                y_ecb_raw.loc[
                    y_ecb_raw.CURRENCY == "USD", "OBS_VALUE"
                ]
            )
        )

        # add a column to DF with the USD based rates
        usd_based = y_ecb_raw["OBS_VALUE"] / exc_USD_EUR
        y_ecb_raw["USD based rate"] = usd_based

        # creating the array with 'XXX/USD' format
        curr_arr = y_ecb_raw["CURRENCY"] + "/USD"
        curr_arr = np.where(curr_arr == "USD/USD", "EUR/USD", curr_arr)

        # creating the array with rate values USD based
        rate_arr = y_ecb_raw["USD based rate"]
        exc_EUR_USD = float(y_ecb_raw.loc[y_ecb_raw.CURRENCY
                                          == "USD", "OBS_VALUE"])
        rate_arr = np.where(
            rate_arr == 1.000000,
            1 / exc_EUR_USD,
            rate_arr
        )

        # stacking the array together
        array = np.column_stack((curr_arr, rate_arr))

        # converting into dataframe
        df = pd.DataFrame(array, columns=header)
        df["Date"] = str(y_TS)
        df["Standard Date"] = yest_h[0]
        exc_ecb = df[["Date", "Standard Date", "Currency", "Rate"]]

    else:

        query = {"Date": str(two_before_TS)}
        prev_clean = mongo.query_mongo(db, clean_coll, query)

        # changing "Date" and "Standard Date" from two day before to yesterday
        prev_clean["Date"] = str(y_TS)
        prev_clean["Standard Date"] = yest_h[0]
        exc_ecb = prev_clean[["Date", "Standard Date", "Currency", "Rate"]]

    return exc_ecb


# #############################################################################
# function takes as input a Dataframe with missing values referred to specific
# exchange, cryptocurrency and fiat pair and fix it; the dataframe passed as
# broken_matrix is a CryptoWatch series and is Fixed for the columns 'Time',
# 'Close Price', 'Crypto Volume', 'Pair Volume' the function, in order to fix,
# looks for the same crypto-fiat pair on all the exchanges and returns a volume
# weighted average of the found values the values of the other exchanges are
# searched in MongoDB database "index" and in the "rawdata" collection


def CW_series_fix_missing(
    broken_matrix, exchange, crypto_fiat_pair, reference_array, db, collection
):

    # define the list af all exchanges and then pop out
    # the exchanges subject to the fixing
    exchange_list = [
        "bitflyer",
        "poloniex",
        "bitstamp",
        "bittrex",
        "coinbase-pro",
        "gemini",
        "kraken",
    ]
    exchange_list.remove(exchange)

    broken_array = broken_matrix["Time"]
    # iteratively find the missing value in all the exchanges
    fixing_price = np.array([])
    fixing_cry_vol = np.array([])
    fixing_pair_vol = np.array([])

    # query MongoDB and rerieve a DataFrame containing
    # all the data related to the specified crypto-fiat pair
    query_dict = {"Pair": crypto_fiat_pair}
    matrix = mongo.query_mongo(db, collection, query_dict)
    matrix = matrix.drop(columns=["Pair"])

    # defining the list of exchanges that actually trade the
    # specified crypto-fiat pair
    exc_with_pair = list(matrix["Exchange"].unique())
    #
    if exchange == "kraken" and crypto_fiat_pair == "btcusd":
        print(exc_with_pair)

    for element in exc_with_pair:

        # reducing the total matrix selecting only the element of the
        # selected exchange
        ex_matrix = matrix.loc[matrix.Exchange == element]

        # find variation of price and volume for the selected exchange
        variations_price, volumes = substitute_finder(
            broken_array, reference_array, ex_matrix, "Close Price"
        )
        variations_cry_vol, volumes = substitute_finder(
            broken_array, reference_array, ex_matrix, "Crypto Volume"
        )
        variations_pair_vol, volumes = substitute_finder(
            broken_array, reference_array, ex_matrix, "Pair Volume"
        )

        # assigning the retrived variation in each exchanges for the
        # selected crypto-fiat pair
        if fixing_price.size == 0:

            fixing_price = variations_price[:, 1]
            fixing_cry_vol = variations_cry_vol[:, 1]
            fixing_pair_vol = variations_pair_vol[:, 1]
            fixing_volume = volumes[:, 1]

        else:

            fixing_price = np.column_stack(
                (fixing_price, variations_price[:, 1]))
            fixing_cry_vol = np.column_stack(
                (fixing_cry_vol, variations_cry_vol[:, 1]))
            fixing_pair_vol = np.column_stack(
                (fixing_pair_vol, variations_pair_vol[:, 1])
            )
            fixing_volume = np.column_stack((fixing_volume, volumes[:, 1]))

    # defining the array containming the list of missing date
    missing_item_time = [int(x) for x in variations_price[:, 0]]

    # defining the dataframes containing the variations of price and volume
    fixing_price_df = pd.DataFrame(fixing_price)
    fixing_cry_vol_df = pd.DataFrame(fixing_cry_vol)
    fixing_pair_vol_df = pd.DataFrame(fixing_pair_vol)
    fixing_volume_df = pd.DataFrame(fixing_volume)

    # compute row sum of every missing items
    fixing_price_df["sum"] = fixing_price_df.sum(axis=1)
    fixing_cry_vol_df["sum"] = fixing_cry_vol_df.sum(axis=1)
    fixing_pair_vol_df["sum"] = fixing_pair_vol_df.sum(axis=1)
    fixing_volume_df["sum"] = fixing_volume_df.sum(axis=1)

    # adding time column
    fixing_price_df["missing date"] = missing_item_time
    fixing_cry_vol_df["missing date"] = missing_item_time
    fixing_pair_vol_df["missing date"] = missing_item_time

    # computing weighted variations for each element
    fixing_price_df["weighted"] = fixing_price_df["sum"] / \
        fixing_volume_df["sum"]
    fixing_price_df.fillna(0, inplace=True)
    fixing_cry_vol_df["weighted"] = fixing_cry_vol_df["sum"] / \
        fixing_volume_df["sum"]
    fixing_cry_vol_df.fillna(0, inplace=True)
    fixing_pair_vol_df["weighted"] = fixing_pair_vol_df["sum"] / \
        fixing_volume_df["sum"]
    fixing_pair_vol_df.fillna(0, inplace=True)

    # merging a column dataframe containing the reference array
    # with the broken matrix values, the missing date will display
    # NaN and will be fixed afterwards
    new_df = pd.DataFrame(reference_array, columns=["Time"])
    merged = pd.merge(new_df, broken_matrix, on="Time", how="left")
    # print(merged.head(100))
    # print(merged.loc[merged['Time'].isin(missing_item_time)])
    merged.fillna(0, inplace=True)
    # print(merged.head(10))
    print(merged.loc[merged['Time'].isin(missing_item_time)])

    for element in missing_item_time:

        prev_val = merged.loc[merged.Time == int(element) - 86400]

        price_var = float(
            fixing_price_df.loc[fixing_price_df["missing date"]
                                == element, "weighted"]
        )
        crypto_vol_var = float(
            fixing_cry_vol_df.loc[
                fixing_cry_vol_df["missing date"] == element, "weighted"
            ]
        )
        # crypto_pair_var = float(fixing_pair_vol_df.loc[fixing_pair_vol_df['missing date'] ==
        #                                                element, 'weighted'])

        if int(element) == 1451606400:

            merged.loc[merged.Time == element, "Close Price"] = 0.0
            merged.loc[merged.Time == element, "Crypto Volume"] = 0.0
            merged.loc[merged.Time == element, "Pair Volume"] = 0.0

        else:

            merged.loc[merged.Time == element, "Close Price"] = float(
                prev_val["Close Price"]
            ) * (1 + price_var)
            new_price = float(merged.loc[merged.Time == element, "Close Price"])
            merged.loc[merged.Time == element, "Crypto Volume"] = float(
                prev_val["Crypto Volume"]
            ) * (1 + crypto_vol_var)
            new_vol = float(merged.loc[merged.Time == element, "Crypto Volume"])
            merged.loc[merged.Time == element,
                       "Pair Volume"] = new_price * new_vol

    # print(merged.loc[merged['Time'].isin(missing_item_time)])

    return merged


# given a matrix (where_to_lookup), a date reference array and, broken date
# array with missing date function returns two matrices:
# the first one is about the "position" information and can be "Close Price",
# "Crypto Volume" or "Pair Volume" where the first column contains the list
# of date that broken array misses and the second column contains the variations
# of the "position" info between T and T-1 the second one contains the volume
# variations as seconda column and date as first


def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    # find the elements of ref array not included in
    # broken array (the one to check)
    missing_item = Diff(reference_array, broken_array)
    missing_item.sort()
    variations = np.array([])
    volumes = np.array([])

    for element in missing_item:
        # for each missing element try to find it in where_to_lookup,
        # if KeyError occurred meaning the searched item is not found,
        # then append zero
        try:

            today_value = float(
                where_to_lookup[where_to_lookup["Time"] == element][position]
            )

            yesterday_value = float(
                where_to_lookup[where_to_lookup["Time"]
                                == element - 86400][position]
            )

            numerator = today_value - yesterday_value
            variation = np.divide(
                numerator,
                yesterday_value,
                out=np.zeros_like(numerator),
                where=numerator != 0.0,
            )
            # consider crytpo vol
            volume = float(
                where_to_lookup[where_to_lookup["Time"]
                                == element]["Pair Volume"]
            )
            variation = variation * volume
            variations = np.append(variations, variation)
            volumes = np.append(volumes, volume)

        except KeyError:

            variations = np.append(variations, 0)
            volumes = np.append(volumes, 0)

        except TypeError:

            variations = np.append(variations, 0)
            volumes = np.append(volumes, 0)

    variation_matrix = np.column_stack((missing_item, variations))
    volume_matrix = np.column_stack((missing_item, volumes))

    return variation_matrix, volume_matrix


# add descr


def daily_fix_miss(curr_df, tot_curr_df, tot_prev_df):

    exchange = np.array(curr_df["Exchange"])
    pair = np.array(curr_df["Pair"])
    exchange = exchange[0]
    pair = pair[0]
    print(pair)
    # select a sub-df containing only the pair of interest of the previous
    # and current dataframes
    pair_prev_df = tot_prev_df.loc[tot_prev_df.Pair == pair]
    pair_curr_df = tot_curr_df.loc[tot_curr_df.Pair == pair]

    # find the list of exchange that actually trade the crypto-fiat pair
    exc_with_pair = list(pair_prev_df["Exchange"].unique())
    print(exc_with_pair)
    exc_with_pair.remove(exchange)
    print(exc_with_pair)
    if exc_with_pair == []:

        price_var = 0

    else:

        fixing_price = np.array([])
        fixing_p_vol = np.array([])

        for el in exc_with_pair:

            print(el)

            # find a subdataframe related with the single exchange of the loop
            ex_pair_prev_df = pair_prev_df.loc[pair_prev_df.Exchange == el]
            ex_pair_curr_df = pair_curr_df.loc[pair_curr_df.Exchange == el]

            weight_var, volume = daily_sub_finder(
                ex_pair_curr_df, ex_pair_prev_df)

            if fixing_price.size == 0:

                fixing_price = weight_var
                fixing_p_vol = volume

            else:

                fixing_price = np.column_stack((fixing_price, weight_var))
                fixing_p_vol = np.column_stack((fixing_p_vol, volume))

        # defining the dataframes containing the variations of price and volume

        print(float(fixing_price))
        print(type(fixing_price))
        fixing_price_df = pd.DataFrame(np.array([fixing_price]))
        fixing_p_vol_df = pd.DataFrame(np.array([fixing_p_vol]))
        print(fixing_price_df)
        # compute row sum
        fixing_price_df["sum"] = fixing_price_df.sum(axis=1)
        fixing_p_vol_df["sum"] = fixing_p_vol_df.sum(axis=1)

        # computing weighted variation
        fixing_price_df["weighted"] = fixing_price_df["sum"] / \
            fixing_p_vol_df["sum"]
        fixing_price_df.fillna(0, inplace=True)
        price_var = float(fixing_price_df["weighted"])

    return price_var


# add descr


def daily_sub_finder(curr_df, prev_df):

    # find the price of the two days
    curr_price = float(curr_df["Close Price"])
    prev_price = float(prev_df["Close Price"])

    # find the volume of the two days
    curr_vol = float(curr_df["Pair Volume"])

    # find the "Close Price" variation
    numerator = curr_price - prev_price
    variation = np.divide(
        numerator, prev_price, out=np.zeros_like(numerator), where=numerator != 0.0
    )

    # multiply the variation and the current volume
    weight_var = variation * curr_vol

    return weight_var, curr_vol


def exc_pair_cleaning(exc_df):

    exc_df["Pair"] = [
        element.replace("USDT_BCHSV", "bsvusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_BCHSV", "bsvusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_BCHABC", "bchusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_BCHABC", "bchusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_LTC", "ltcusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_LTC", "ltcusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_XRP", "xrpusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_XRP", "xrpusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_ZEC", "zecusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_ZEC", "zecusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_EOS", "eosusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_EOS", "eosusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_ETC", "etcusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_ETC", "etcusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_STR", "xlmusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_STR", "xlmusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_BTC", "btcusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_BTC", "btcusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDC_ETH", "ethusdc") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [
        element.replace("USDT_ETH", "ethusdt") for element in exc_df["Pair"]
    ]
    exc_df["Pair"] = [element.lower() for element in exc_df["Pair"]]
    exc_df["Pair"] = [element.replace("xbt", "btc")
                      for element in exc_df["Pair"]]

    clean_df = exc_df

    return clean_df


def exc_value_cleaning(exc_df):

    exc_df["Crypto Volume"] = [float(v) for v in exc_df["Crypto Volume"]]
    exc_df["Close Price"] = [float(p) for p in exc_df["Close Price"]]
    exc_df["Pair Volume"] = exc_df["Close Price"] * exc_df["Crypto Volume"]

    clean_df = exc_df

    return clean_df


def make_unique(df_to_check):

    # create a key that has to be unique
    time_to_str = [str(date) for date in df_to_check["Time"]]
    df_to_check["Time_str"] = time_to_str
    df_to_check["key"] = df_to_check["Time_str"] + \
        df_to_check["Exchange"] + df_to_check["Pair"]

    df_to_check.drop_duplicates(
        subset="key", keep="first", inplace=True)

    unique_df = df_to_check

    unique_df = unique_df.drop(columns=["Time_str", "key"])

    return unique_df
