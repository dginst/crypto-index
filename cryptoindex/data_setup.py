# standard library import
from datetime import datetime, timedelta, timezone
import time

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


def timestamp_gen(start_date, end_date=None, EoD="Y"):

    start = datetime.strptime(start_date, "%m-%d-%Y")
    start = int(start.replace(tzinfo=timezone.utc).timestamp())

    if end_date is None:

        end_date = datetime.now().strftime("%m-%d-%Y")

    end_date = datetime.strptime(end_date, "%m-%d-%Y")
    end = int(end_date.replace(tzinfo=timezone.utc).timestamp())

    array = np.array([start])
    date = start

    while date < end:
        date = date + 86400
        array = np.append(array, date)

    if EoD == "Y":

        array = array[: len(array) - 1]

    else:

        pass

    return array


def timestamp_gen_legal_solar(TS_array):

    legal_16 = (1459209600, 1477612800)
    legal_17 = (1490572800, 1509062400)
    legal_18 = (1522065600, 1540555200)
    legal_19 = (1554120000, 1572004800)
    legal_20 = (1585569600, 1603454400)

    new_array = np.array([legal_16[0]])
    for date in TS_array:

        if (
            legal_16[0] <= date <= legal_16[1]
            or legal_17[0] <= date <= legal_17[1]
            or legal_18[0] <= date <= legal_18[1]
            or legal_19[0] <= date <= legal_19[1]
            or legal_20[0] <= date <= legal_20[1]
        ):

            date = date - 3600
            new_array = np.append(new_array, date)

        else:

            new_array = np.append(new_array, date)

    new_array = new_array[1: len(new_array)]

    return new_array


# function that converts the date array found using the above function into a
# standard date array with the date format YYYY-MM-DD


def timestamp_convert(date_array):

    new_array = np.array = []

    for date in date_array:

        new_date = datetime.fromtimestamp(date)
        new_date = new_date.strftime("%Y-%m-%d")
        new_array = np.append(new_array, new_date)

    return new_array


# function takes as input an array and return the same array in string format


def timestamp_to_str(date_array):

    new_array = list()
    for date in date_array:

        new_date = str(date)

        if new_array == []:

            new_array = new_array.append(new_date)

        else:

            new_array = new_array.append(new_date)

    return new_array


# the function takes an array of timestamp as input and return an
# array of human readable date in dd-mm-yyyy format


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


# function that generate an array of date starting from start_date to end_date
# if not specified end_date = today()
# default format is in second since the epoch (timeST = 'Y'), type timeST='N'
# for date in format YY-mm-dd function considers End of Day price series so,
# if not otherwise specified, the returned array of date will be from start
# to today - 1 write all date in MM/DD/YYYY format


def date_array_gen(start_date, end_date=None, timeST="Y", EoD="Y"):

    # set end_date = today if empty
    if end_date is None:
        end_date = datetime.now().strftime("%m-%d-%Y")

    date_index = pd.date_range(start_date, end_date)

    DateList = date_list(date_index, timeST)

    if EoD == "Y":
        DateList = DateList[: len(DateList) - 1]

    return DateList


# given a start date and a period (number of days) the function returns an
# array containing the "period" date going back from the start date (default)
# or starting from the start date (direction='forward') the output can be both
# in timestamp since epoch (default) or in date MM/DD/YYYY (timeST='N')


def period_array(start_date, period, direction="backward", timeST="Y"):

    start_date = date_reformat(start_date, "-")

    if direction == "backward":
        end_date = datetime.strptime(
            start_date, "%m-%d-%Y") - timedelta(days=period)
        date_index = pd.date_range(end_date, start_date)

    else:
        end_date = datetime.strptime(
            start_date, "%m-%d-%Y") + timedelta(days=period)
        date_index = pd.date_range(start_date, end_date)

    DateList = date_list(date_index, timeST)

    return DateList


# function that returns a list containing date in timestamp format


def date_list(date_index, timeST="Y", lag_adj=3600):

    DateList = []

    for date in date_index:
        val = int(time.mktime(date.timetuple()))
        val = val + lag_adj
        DateList.append(val)

    NoStamp = []
    if timeST == "N":
        for string in DateList:
            value = int(string)
            NoStamp.append(datetime.utcfromtimestamp(
                value).strftime("%Y-%m-%d"))
        return NoStamp
    else:
        return DateList


# function that reforms the inserted date according to the choosen separator
# function takes as input date with "/" seprator and without separator for
# both the YY ans YYYY format; works on default with MM-DD_YYYY format and
# if different has to be specified ('YYYY-DD-MM' or 'YYYY-MM-DD')


def date_reformat(date_to_check, separator="-", order="MM-DD-YYYY"):
    if "/" in date_to_check and len(date_to_check) == 10:
        return_date = date_to_check.replace("/", separator)
    elif "/" in date_to_check and len(date_to_check) == 8:
        return_date = date_to_check.replace("/", separator)
    elif "/" not in date_to_check and (
        len(date_to_check) == 8 or len(date_to_check) == 6
    ):
        if order == "YYYY-DD-MM" or order == "YYYY-MM-DD":
            return_date = (
                date_to_check[:4]
                + separator
                + date_to_check[4:6]
                + separator
                + date_to_check[6:]
            )
        else:
            return_date = (
                date_to_check[:2]
                + separator
                + date_to_check[2:4]
                + separator
                + date_to_check[4:]
            )
    else:
        return_date = date_to_check

    return return_date


# DATA FIXING FUNCTIONS


# function that returns a list containing the elements of list_1 (bigger one)
# not included in list_2 (smaller one)


def Diff(list_1, list_2):

    return list(set(list_1) - set(list_2))


# function that checks if a item(array, matrix, string,...) is null


def Check_null(item):

    try:

        return len(item) == 0

    except TypeError:

        pass

    return False


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


# function takes as input a Dataframe with missing values referred to specific
# exchange, cryptocurrency and fiat pair and fix it; the dataframe passed as
# broken_matrix is a CryptoWatch series and is Fixed for the columns 'Time',
# 'Close Price', 'Crypto Volume', 'Pair Volume' the function, in order to fix,
# looks for the same crypto-fiat pair on all the exchanges and returns a volume
# weighted average of the found values the values of the other exchanges are
# searched in MongoDB database "index" and in the "rawdata" collection


def CW_series_fix_missing(
    broken_matrix,
    exchange,
    cryptocurrency,
    pair,
    start_date,
    end_date=None,
    db="index",
    collection="CW_rawdata",
):

    # define DataFrame header
    header = ["Time", "Close Price", "Crypto Volume", "Pair Volume"]

    # set end_date = today if empty
    if end_date is None:
        end_date = datetime.now().strftime("%m-%d-%Y")

    # creating the reference date array from start date to end date
    reference_array = timestamp_gen(start_date, end_date)
    # select just the date on broken_matrix
    broken_array = broken_matrix["Time"]
    ccy_pair = cryptocurrency.lower() + pair.lower()

    # set the list af all exchanges and then pop out the one in subject
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

    # iteratively find the missing value in all the exchanges
    fixing_price = np.array([])
    fixing_cry_vol = np.array([])
    fixing_pair_vol = np.array([])
    fixing_volume = np.array([])

    # variable that count how many exchanges actually have values for the
    # selected crypto+pair
    count_exchange = 0

    for element in exchange_list:

        # defining the dictionary to use in querying MongoDB
        query_dict = {"Exchange": element, "Pair": ccy_pair}
        # query MongoDB and rerieve a DataFrame called "matrix"
        matrix = mongo.query_mongo(db, collection, query_dict)
        matrix = matrix.drop(
            columns=["Exchange", "Pair", "Low", "High", "Open"])

        # checking if data frame is empty: if not then the crypto-fiat pair
        # exists in the exchange then add to the count variable
        if matrix.shape[0] > 1:

            count_exchange = count_exchange + 1

            # if the matrix is not null, find variation and volume of the
            # selected exchange and assign them to the related matrix
            variations_price, volumes = substitute_finder(
                broken_array, reference_array, matrix, "Close Price"
            )
            variations_cry_vol, volumes = substitute_finder(
                broken_array, reference_array, matrix, "Crypto Volume"
            )
            variations_pair_vol, volumes = substitute_finder(
                broken_array, reference_array, matrix, "Pair Volume"
            )
            variation_time = variations_price[:, 0]

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
                    (fixing_cry_vol, variations_cry_vol[:, 1])
                )
                fixing_pair_vol = np.column_stack(
                    (fixing_pair_vol, variations_pair_vol[:, 1])
                )
                fixing_volume = np.column_stack((fixing_volume, volumes[:, 1]))

    # find the volume weighted variation for all the variables
    weighted_var_price = []
    weighted_cry_vol = []
    weighted_pair_vol = []

    for i in range(len(fixing_price)):

        count_none = 0

        for j in range(int(count_exchange)):

            try:

                if fixing_price[i, j] == 0:

                    count_none = count_none + 1

            except IndexError:

                pass

        # checking if single date is missing in ALL the exchanges
        # if yes assign zero variation (the previous day value will be taken)
        if count_none == count_exchange:

            weighted_var_price.append(0)
            weighted_cry_vol.append(0)
            weighted_pair_vol.append(0)

        # condition that assure:
        # 1) not all values are 0,
        # 2) there is more than1 exchange (= more than 1 columns)
        # 3) if true, there is just an element to fix, so fixing variation is a
        #    1d array
        elif (
            count_none != count_exchange
            and count_exchange > 1
            and fixing_price.size == count_exchange
        ):

            price = fixing_price[i, :].sum() / fixing_volume[i, :].sum()
            cry_vol = fixing_cry_vol[i, :].sum() / fixing_volume[i, :].sum()
            pair_vol = fixing_pair_vol[i, :].sum() / fixing_volume[i, :].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)

        elif count_none != count_exchange and count_exchange == 1:

            price = fixing_price[i].sum() / fixing_volume[i].sum()
            cry_vol = fixing_cry_vol[i].sum() / fixing_volume[i].sum()
            pair_vol = fixing_pair_vol[i].sum() / fixing_volume[i].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)

        elif (
            count_none != count_exchange
            and count_exchange > 1
            and fixing_price.size > count_exchange
        ):

            price = fixing_price[i, :].sum() / fixing_volume[i, :].sum()
            cry_vol = fixing_cry_vol[i, :].sum() / fixing_volume[i, :].sum()
            pair_vol = fixing_pair_vol[i, :].sum() / fixing_volume[i, :].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)

    # create a matrix with columns: timestamp date, weighted variatons of
    # prices, weightes variations of volume both crypto and pair
    try:
        variation_matrix = np.column_stack(
            (variation_time, weighted_var_price,
             weighted_cry_vol, weighted_pair_vol)
        )
        variation_matrix = np.nan_to_num(variation_matrix)

        for i, row in enumerate(variation_matrix[:, 0]):

            # find previuos value and multiply the variation in order to obtain
            # the new values to insert
            prev_vals = broken_matrix[broken_matrix["Time"]
                                      == row - 86400].iloc[:, 1:4]
            if prev_vals.empty is True:
                prev_vals = np.zeros((1, 3))

            new_values = prev_vals * (1 + variation_matrix[i, 1:])
            new_values = pd.DataFrame(
                np.column_stack((row, new_values)), columns=header
            )

            # insert the new values into the broken_matrix
            broken_matrix = broken_matrix.append(new_values)
            broken_matrix = broken_matrix.sort_values(by=["Time"])
            broken_matrix = broken_matrix.reset_index(drop=True)

        fixed_matrix = broken_matrix
        int_date = np.array([])

        # convert the date into string (timestamp format)
        for date in fixed_matrix["Time"]:

            new_date = int(date)
            new_date = str(new_date)
            int_date = np.append(int_date, new_date)

        fixed_matrix["Time"] = int_date

    # this exception allows to manage the case in which the broken_matrix is
    # the only existing matrix containing the soecific crypto - fiat pair; in
    # other words just 1 exchange trades on that cp so, we put the missing
    # values to 0 and complete the dataframe with the standard dimension

    except UnboundLocalError:

        zeros_matrix = np.zeros((len(reference_array), len(header) - 1))
        fixed_matrix = np.column_stack((reference_array, zeros_matrix))
        fixed_matrix = pd.DataFrame(fixed_matrix, columns=header)
        header.remove("Time")

        for day in broken_matrix["Time"]:

            values_to_insert = np.array(
                broken_matrix.loc[broken_matrix.Time == day][header]
            )
            fixed_matrix.loc[fixed_matrix.Time
                             == day, header] = values_to_insert

        int_date = np.array([])

        # convert the date into string (timestamp format)
        for date in fixed_matrix["Time"]:

            new_date = int(date)
            new_date = str(new_date)
            int_date = np.append(int_date, new_date)

        fixed_matrix["Time"] = int_date

    return fixed_matrix


# given a matrix (where_to_lookup), a date reference array and, broken date
# array with missing date function returns two matrices:
# the first one is about the "position" information and can be "Close Price",
# "Crypto Volume" or "Pair Volume" where the first column contains the list of
# date that broken array misses and the second column contains the variations
# of the "position" info between T and T-1 the second one contains the volume
# variations as seconda column and date as first


def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    # find the elements of ref array not included in broken array (the one to
    # check)
    missing_item = Diff(reference_array, broken_array)
    missing_item.sort()
    variations = []
    volumes = []
    for element in missing_item:
        # for each missing element try to find it in where to look up, if
        # KeyError occurred meaning the searched item is not found, then append zero
        try:

            # today_alt = where_to_lookup[where_to_lookup["Time"] ==
            # element][position]
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
            variations.append(variation)
            volumes.append(volume)

        except KeyError:

            variations.append(0)
            volumes.append(0)

        except TypeError:

            variations.append(0)
            volumes.append(0)

    volumes = np.array(volumes)
    variations = np.array(variations)
    variation_matrix = np.column_stack((missing_item, variations))
    volume_matrix = np.column_stack((missing_item, volumes))

    return variation_matrix, volume_matrix


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
    date = timestamp_gen(start_period, End_Period, EoD="N")
    date_ECB = timestamp_gen_legal_solar(date)
    # date = timestamp_convert(date_TS)
    # date = [datetime.strptime(x, '%Y-%m-%d') for x in date]

    # defining the headers of the returning data frame
    header = ["Date", "Currency", "Rate"]

    # for each date in "date" array the funcion retrieves data from
    # ECB website and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    for i, single_date in enumerate(date):

        database = "index"
        collection = "ecb_raw"
        query = {"TIME_PERIOD": str(date_ECB[i])}

        # retrieving data from MongoDB 'index' and 'ecb_raw' collection
        single_date_ex_matrix = mongo.query_mongo(database, collection, query)

        # check if rates exist in the specified date
        if Check_null(single_date_ex_matrix) is False:

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
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j, 0] = int(time_stamp)

    return pd.DataFrame(Exchange_Matrix, columns=header)


# function returns a matrix of exchange rates USD based that contains Date,
# Exchange indicator (ex. USD/GBP) and rate for TODAY retrieving data from
# the website of European Central Bank the function, if data is missing
# (holiday and weekends), finds the first previous day with data and takes
# its values input: key_curr_vector that passes the list of currencies of
# interest


def ECB_daily_setup(key_curr_vector, timeST="N"):

    # defining the array of date to be used
    today = datetime.now().strftime("%m-%d-%Y")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%m-%d-%Y")
    date = date_array_gen(yesterday, today, timeST="N", EoD="N")
    date = [datetime.strptime(x, "%Y-%m-%d") for x in date]

    # defining the headers of the returning data frame
    header = ["Date", "Currency", "Rate"]

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    # defining the MongoDB path where to look for the rates
    database = "index"
    collection = "ecb_raw"
    query = {"TIME_PERIOD": date[1]}

    # retrieving data from MongoDB 'index' and 'ecb_raw' collection
    single_date_ex_matrix = mongo.query_mongo(database, collection, query)

    # check if rates exist in the specified date
    if Check_null(single_date_ex_matrix) is False:

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
        date_arr = np.full(len(key_curr_vector), date[1])

        # creating the array with 'XXX/USD' format
        curr_arr = single_date_ex_matrix["CURRENCY"] + "/USD"
        curr_arr = np.where(curr_arr == "USD/USD", "EUR/USD", curr_arr)

        # creating the array with rate values USD based
        rate_arr = single_date_ex_matrix["USD based rate"]
        rate_arr = np.where(
            rate_arr == 1.000000, 1
            / single_date_ex_matrix["OBS_VALUE"][0], rate_arr
        )

        # stacking the array together
        array = np.column_stack((date_arr, curr_arr, rate_arr))

        # filling the return matrix
        if Exchange_Matrix.size == 0:

            Exchange_Matrix = array

        else:

            Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    # if the query returns an empty matrix, function will takes values of the
    # last useful day
    else:

        # set the MongoDB query to yestarday
        query = {"TIME_PERIOD": date[0]}

        # retrieving data from MongoDB 'index' and 'ecb_raw' collection
        single_date_ex_matrix = mongo.query_mongo(database, collection, query)

        date_arr = np.full(len(key_curr_vector), date[1])

        # creating the array with 'XXX/USD' format
        curr_arr = single_date_ex_matrix["CURRENCY"] + "/USD"
        curr_arr = np.where(curr_arr == "USD/USD", "EUR/USD", curr_arr)

        # creating the array with rate values USD based
        rate_arr = single_date_ex_matrix["USD based rate"]
        rate_arr = np.where(
            rate_arr == 1.000000, 1
            / single_date_ex_matrix["OBS_VALUE"][0], rate_arr
        )

        # stack the array together
        array = np.column_stack((date_arr, curr_arr, rate_arr))

        if Exchange_Matrix.size == 0:

            Exchange_Matrix = array

        else:

            Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    if timeST != "N":

        for j, element in enumerate(Exchange_Matrix[:, 0]):

            to_date = datetime.strptime(element, "%Y-%m-%d")
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j, 0] = int(time_stamp)
    return pd.DataFrame(Exchange_Matrix, columns=header)


# function returns the Data Frame relative to a specified exchange/crypto/pair
# with the "pair" value converted in USD, more specifically converts the
# columns 'Close Price' and 'Pair Volume' into USD function takes as input:
# CW_matrix = CryptoWatch dataframe to be changed
# Ex_Rate_matrix = data frame of ECB exchange rates
# currency = string that specify the currency of CW_matrix (EUR, CAD, GBP,...)


def CW_data_setup(CW_matrix, currency):

    currency = currency.upper()

    if currency != "USD":

        ex_curr = currency + "/USD"

        # connecting to mongo in local
        # connection = MongoClient("localhost", 27017)
        # db = connection.index

        # defining the MongoDB path where to look for the rates

        for i in range(len(CW_matrix["Time"])):

            date = str(CW_matrix["Time"][i])

            # defining the MongoDB path where to look for the rates
            database = "index"
            collection = "ecb_clean"
            query = {"Date": date, "Currency": ex_curr}

            # retrieving data from MongoDB 'index' and 'ecb_raw' collection
            single_date_rate = mongo.query_mongo(database, collection, query)

            if single_date_rate.shape[0] == 1:

                rate = single_date_rate["Rate"]

            else:

                single_date_rate = single_date_rate.iloc[0]

            rate = single_date_rate["Rate"]

            CW_matrix["Close Price"][i] = float(
                CW_matrix["Close Price"][i] / rate)
            CW_matrix["Pair Volume"][i] = float(
                CW_matrix["Pair Volume"][i] / rate)

    else:

        CW_matrix = CW_matrix

    return CW_matrix


# OLD UNUSED FUNCTIONS


# function takes a .json file from Cryptowatch API and transforms it into a
# matrix the matrix has the headers :
# ['Time' ,'Open',	'High',	'Low', 'Close',""+Crypto+" Volume" , ""+Pair+" Volume"]
# if the downloaded file does not have results the function returns an empty
# array note that the "time" column contains value in timestamp format 86400 is
# the daily frequency in seconds


def json_to_matrix(file_path, Crypto="", Pair=""):
    raw_json = pd.read_json(file_path)
    if Crypto == "":
        Crypto = "Crypto"
    if Pair == "":
        Pair = "Pair"
    header = [
        "Time",
        "Open",
        "High",
        "Low",
        "Close Price",
        "" + Crypto + " Volume",
        "" + Pair + " Volume",
    ]
    if (
        "result" in raw_json.keys()
    ):  # testing if the file has the 'result' list of value
        matrix = pd.DataFrame(raw_json["result"]["86400"], columns=header)
        # for i, element in enumerate(matrix['time']):
        #     matrix['time'][i]=datetime.strptime(str(datetime.fromtimestamp(matrix['time'][i]))[:10], '%Y-%m-%d').strftime('%d/%m/%y')
    else:
        matrix = np.array([])

    return matrix


# return a sorted array of the size of reference_array.
# if there are more elements in ref array, broken_array is filled with the
# missing elements broken_array HAS TO BE smaller than reference array default
# sorting is in ascending way, if descending is needed specify versus='desc'


def fill_time_array(broken_array, ref_array, versus="asc"):

    difference = Diff(ref_array, broken_array)

    for element in difference:
        broken_array.add(element)
    broken_array = list(broken_array)

    if versus == "desc":
        broken_array.sort(reverse=True)

    else:
        broken_array.sort()

    return broken_array


# function that given a list of items, find the items and relative indexes in
# another list/vector if one or more items in list_to_find are not included
# in where_to_find the function return None as position the return matrix have
# items as first column and index as second column


def find_index(list_to_find, where_to_find):

    list_to_find = np.array(list_to_find)
    where_to_find = np.array(where_to_find)
    index = []
    item = []
    for element in list_to_find:

        if element in where_to_find:
            (i,) = np.where(where_to_find == element)
            index.append(i)
            item.append(element)
        else:
            index.append(None)
            item.append(element)

    index = np.array(index)
    item = np.array(item)
    indexed_item = np.column_stack((item, index))
    indexed_item = indexed_item[indexed_item[:, 0].argsort()]

    return indexed_item


# #############################################################################
# function takes as input a Dataframe with missing values referred to specific
# exchange, cryptocurrency and fiat pair and fix it; the dataframe passed as
# broken_matrix is a CryptoWatch series and is Fixed for the columns 'Time',
# 'Close Price', 'Crypto Volume', 'Pair Volume' the function, in order to fix,
# looks for the same crypto-fiat pair on all the exchanges and returns a volume
# weighted average of the found values the values of the other exchanges are
# searched in MongoDB database "index" and in the "rawdata" collection


def CW_series_fix_missing2(
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

    for element in exc_with_pair:

        # reducing the total matrix selecting only the element of the
        # selected exchange
        ex_matrix = matrix.loc[matrix.Exchange == element]

        # find variation of price and volume for the selected exchange
        variations_price, volumes = substitute_finder2(
            broken_array, reference_array, ex_matrix, "Close Price"
        )
        variations_cry_vol, volumes = substitute_finder2(
            broken_array, reference_array, ex_matrix, "Crypto Volume"
        )
        variations_pair_vol, volumes = substitute_finder2(
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
    # print(merged.loc[merged['Time'].isin(missing_item_time)])

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
        merged.loc[merged.Time == element, "Close Price"] = float(
            prev_val["Close Price"]
        ) * (1 + price_var)
        new_price = float(merged.loc[merged.Time == element, "Close Price"])
        merged.loc[merged.Time == element, "Crypto Volume"] = float(
            prev_val["Crypto Volume"]
        ) * (1 + crypto_vol_var)
        new_vol = float(merged.loc[merged.Time == element, "Crypto Volume"])
        merged.loc[merged.Time == element, "Pair Volume"] = new_price * new_vol

    # print(merged.loc[merged['Time'].isin(missing_item_time)])

    return merged


# given a matrix (where_to_lookup), a date reference array and, broken date
# array with missing date function returns two matrices:
# the first one is about the "position" information and can be "Close Price",
# "Crypto Volume" or "Pair Volume" where the first column contains the list
# of date that broken array misses and the second column contains the variations
# of the "position" info between T and T-1 the second one contains the volume
# variations as seconda column and date as first


def substitute_finder2(broken_array, reference_array, where_to_lookup, position):

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
    # select a sub-df containing only the pair of interest of the previous
    # and current dataframes
    pair_prev_df = tot_prev_df.loc[tot_prev_df.Pair == pair]
    pair_curr_df = tot_curr_df.loc[tot_curr_df.Pair == pair]

    # find the list of exchange that actually trade the crypto-fiat pair
    exc_with_pair = list(pair_prev_df["Exchange"].unique())
    exc_with_pair.remove(exchange)

    fixing_price = np.array([])
    fixing_p_vol = np.array([])

    for el in exc_with_pair:

        # find a subdataframe related with the single exchange of the loop
        ex_pair_prev_df = pair_prev_df.loc[pair_prev_df.Exchange == el]
        ex_pair_curr_df = pair_curr_df.loc[pair_curr_df.Exchange == el]

        weight_var, volume = daily_sub_finder(ex_pair_curr_df, ex_pair_prev_df)

        if fixing_price.size == 0:

            fixing_price = weight_var
            fixing_p_vol = volume

        else:

            fixing_price = np.column_stack((fixing_price, weight_var))
            fixing_p_vol = np.column_stack((fixing_p_vol, volume))

    # defining the dataframes containing the variations of price and volume

    fixing_price_df = pd.DataFrame(fixing_price)
    fixing_p_vol_df = pd.DataFrame(fixing_p_vol)

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
