# standard library import
from datetime import datetime, timedelta, timezone

# third party import
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

# local import
from . import data_setup
from cryptoindex.data_setup import (
    timestamp_to_human
)
from . import mongo_setup as mongo
from cryptoindex.mongo_setup import (
    df_reorder, query_mongo
)
from cryptoindex.config import (
    DB_NAME, MONGO_DICT, DAY_IN_SEC
)

# ###########################################################################
# ######################## DATE SETTINGS FUNCTIONS ##########################


# the function returns a series of value between "start" and "end" with
# "delta" increase. Can be used for date date as well
# the result is an iterable object of dates


def perdelta(start, end, delta=relativedelta(months=3)):

    current = start
    while current < end:
        yield current
        current += delta


# the function generates an array cointaing the first date of each quarter.
# it starts counting from the start_date (01-01-2016) to the stop_date
# (today as default) and returns a list of date in timestamp format
# the result is an numpy array caontaining dates in timestamp format


def start_q(
    start_date="01-01-2016", stop_date=None, timeST="Y", delta=relativedelta(months=3)
):

    if type(start_date) == str:

        start_date = datetime.strptime(start_date, "%m-%d-%Y")

    if stop_date is None:

        stop_date = datetime.now().strftime("%m-%d-%Y")
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    elif type(stop_date) == str:
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    date_range = perdelta(start_date, stop_date, delta)

    if timeST == "Y":

        start_day_arr = [
            int(x.replace(tzinfo=timezone.utc).timestamp()) for x in date_range
        ]

    else:

        start_day_arr = [x.strftime("%m-%d-%Y") for x in date_range]

    start_day_arr = np.array(start_day_arr)

    return start_day_arr


# the function generates an array cointaing the first date of each quarter.
# it starts counting from the start_date (01-01-2016) to first future start
# rebalance date and returns a list of date in timestamp format


def next_start(start_date="01-01-2016", stop_date=None, timeST="Y"):

    day_in_sec = 86400

    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%m-%d-%Y")

    if stop_date is None:

        stop_date = datetime.now().strftime("%m-%d-%Y")
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    # creating the arrays containing the start and stop date of each quarter
    start_quarter = start_q(start_date, stop_date, timeST)
    stop_quarter = stop_q(start_quarter)

    next_start_date = int(stop_quarter[len(stop_quarter) - 1]) + day_in_sec

    start_quarter = np.append(start_quarter, next_start_date)

    return start_quarter


# the function returns an array containing the stop_date of each quarter in ts
# each stop date is computed starting from the array resulting from the
# function "start_q" that contain a list of start date for each quarter.
# the start_q array resulting from the funtion is taken as input
# the first element of stop_q will be the first element of the start_array
# where 3 months has been added


def stop_q(start_q_array):

    day_in_sec = 86400
    stop_q_array = np.array([])

    for i in range(start_q_array.size - 1):

        stop_date = start_q_array[i + 1] - day_in_sec
        stop_q_array = np.append(stop_q_array, int(stop_date))

    delta = relativedelta(months=3)
    last_start = start_q_array[start_q_array.size - 1]
    last_stop = datetime.utcfromtimestamp(last_start)
    print(last_stop)
    last_stop = last_stop + delta
    # nb ritorna le 2 am per ultima data
    last_stop = int(last_stop.replace(
        tzinfo=timezone.utc).timestamp()) - day_in_sec

    stop_q_array = np.append(stop_q_array, last_stop)

    return stop_q_array


# this function generates an array cointaing the date of the board meeting.
# the meeting is placed every 3 months, on the 21th day, roughly 7 days
# before the next rebalancing date. If the selected day isn't a working
# day, the first previous business day is chosen
# the first meeting day is setting on default as '12-05-2015'


def board_meeting_day(
    start_date="12-21-2015", stop_date=None, delta=relativedelta(months=3), timeST="Y"
):

    # transforming start_date string into date
    start_date = datetime.strptime(start_date, "%m-%d-%Y")

    if stop_date is None:

        stop_date = datetime.now().strftime("%m-%d-%Y")
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    else:

        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    date_range = perdelta(start_date, stop_date, delta)

    if timeST == "Y":

        board_day = [
            int(previous_business_day(x).replace(
                tzinfo=timezone.utc).timestamp())
            for x in date_range
        ]

    else:

        board_day = [previous_business_day(x).strftime(
            "%m-%d-%Y") for x in date_range]

    board_day = np.array(board_day)

    return board_day


# the function returns an array containing the days before each board meeting
# no input is required, the function uses the above "board_day_meeting()"
# function and presents the same default input


def day_before_board(
    start_date="12-21-2015", stop_date=None, delta=relativedelta(months=3), timeST="Y"
):

    day_in_sec = 86400
    before_board_day = (
        board_meeting_day(start_date, stop_date, delta, timeST) - day_in_sec
    )

    return before_board_day


# function that returns/yields a couple of values representing the start date
# and end date of each quarter, the couple are displayed from start_date
# (2016/01/01 on default) to the last past stop_date


def quarterly_period(start_date="01-01-2016", stop_date=None, timeST="Y"):

    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%m-%d-%Y")

    if stop_date is None:

        stop_date = datetime.now().strftime("%m-%d-%Y")
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    # creating the arrays containing the start and stop date of each quarter
    start_quarter = start_q(start_date, stop_date, timeST)
    stop_quarter = stop_q(start_quarter)

    # ###########aggiungere check se end date > di today ############
    for i in range(start_quarter.size - 1):

        yield (start_quarter[i], stop_quarter[i])


# function that returns/yields a couple of values representing the start date
# and end date of each quarter, the couple are displayed from start_date
# (2016/01/01 on default) to the next future stop_date
# the last returned couple is the current quarter start_date and its future
# stop_date; the initial_val input (defualt = 1) allows to make the
# array start from a different point (e.g. 1 cut the first start/stop)


def next_quarterly_period(
    start_date="01-01-2016", stop_date=None, timeST="Y", initial_val=1
):

    if type(start_date) == str:
        start_date = datetime.strptime(start_date, "%m-%d-%Y")

    if stop_date is None:

        stop_date = datetime.now().strftime("%m-%d-%Y")
        stop_date = datetime.strptime(stop_date, "%m-%d-%Y")

    # creating the arrays containing the start and stop date of each quarter
    start_quarter = start_q(start_date, stop_date, timeST)
    stop_quarter = stop_q(start_quarter)

    # defining the current date and the past last stop_date
    today_str = datetime.now().strftime("%m-%d-%Y")
    today = datetime.strptime(today_str, "%m-%d-%Y")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    y_TS = today_TS - 86400
    last_stop = int(stop_quarter[len(stop_quarter) - 1])

    if last_stop > today_TS:

        stop_quarter[len(stop_quarter) - 1] = y_TS

    for i in range(initial_val, start_quarter.size):

        yield (start_quarter[i], stop_quarter[i])


# function that check if a date is a business day
# returns "True" is the date is a BD, "False" otherwise


def is_business_day(date):

    return bool(len(pd.bdate_range(date, date)))


# function returns the first previous business day of the imput date
# if the input date is not a business date
# the output is a date as str 'mm-dd-yyyy' format


def previous_business_day(date):

    while is_business_day(date) is False:
        date = date - timedelta(days=1)

    return date


# ###########################################################################

# #################### FIRST LOGIC MATRIX ###################################

# function that takes as input
# Curr_Exc_Vol: single Crypto volume matrix with exchanges as columns and
# date as rows; the matrix dimension has to be standardized not depending
# of the actual exchanges that trades the single crypto; then if a Crypto
# is not present in a Exchange the value will be artificially set to 0
# Exchanges: is the list of all the Exchanges used to retrieve values
# The function sums, for every exchange, the volume value of each day
# among one index rebalance and the boards day eve.
# The function returns a matrix where the first column contains the
# rebalancing date eve (timestamp format) and the others columns
# contain the percentage that each exchanges represent on the total
# volume for the considered period.
# Note that the returning matrix will have the same number of rows
# of start_q or stop_q arrays; Note that the function returns the
#  values relative to A SINGLE CRYPTOASSET


def perc_volumes_per_exchange(
    Crypto_Ex_Vol, Exchanges, start_date="01-01-2016", end_date=None, time_column="N"
):

    volume_fraction = np.array([])
    start_vector = np.array([])
    next_reb_start = next_start()

    # calling the function that creates the array
    # containing the boards date eve series
    board_eve = day_before_board()

    day_in_sec = 86400

    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())

    yesterday = today_TS - day_in_sec
    board_eve = np.append(board_eve, yesterday)

    # calling the function that yields the start and stop date couple
    rebalance_start = next_quarterly_period(start_date, end_date, initial_val=0)

    # for every start and stop date couple compute the relative logic matrix
    i = 1
    for start, stop in rebalance_start:

        # the board eve date is used to compute the values, the period of
        # computation goes from the previuos rebalance date to the eve of
        # the board date
        quarter_matrix = Crypto_Ex_Vol[Exchanges][
            Crypto_Ex_Vol["Time"].between(start, board_eve[i], inclusive=True)
        ]
        quarter_sum = quarter_matrix.sum()
        exchange_percentage = quarter_sum / quarter_sum.sum()

        if start_vector.size == 0:

            # start_vector = stop
            start_vector = np.array(int(next_reb_start[i]))
            volume_fraction = np.array(exchange_percentage)

        else:
            start_vector = np.row_stack((start_vector, int(next_reb_start[i])))
            volume_fraction = np.row_stack(
                (volume_fraction, np.array(exchange_percentage))
            )

        i = i + 1

    if time_column != "N":

        rebalance_date_perc = np.column_stack((start_vector, volume_fraction))
        header = ["Time"]
        header.extend(Exchanges)

    else:

        rebalance_date_perc = volume_fraction
        header = Exchanges

    rebalance_date_perc = pd.DataFrame(rebalance_date_perc, columns=header)

    return rebalance_date_perc


# This function creates an array of 0 and 1 checking if the first requirement
# is respected.
# ( 1st requirement: The crypto-asset has no more than 80% of its combined
# trading volume between the reconstitution day and the eve of tye committe
# meeting day on any single pricing source)
# If the requirement is respected the function will put 1 in the array,
# if not it will put a 0.


def first_logic_matrix(
    Crypto_Ex_Vol, Exchanges, start_date="01-01-2016", end_date=None
):

    exchange_vol_percentage = perc_volumes_per_exchange(
        Crypto_Ex_Vol, Exchanges, start_date, end_date, time_column="Y"
    )
    first_logic_matrix = np.array([])

    for stop_date in exchange_vol_percentage["Time"]:

        row = exchange_vol_percentage.loc[
            exchange_vol_percentage.Time == stop_date, Exchanges
        ]
        row = np.array(row)

        # check if any of the value in array row is > than 0.80.
        # If yes add a 0 value in the first_logic_matrix
        # if not add value 1 in the first_logic_matrix
        if np.any(row > 0.8):

            first_logic_matrix = np.append(first_logic_matrix, 0)

        else:

            if np.any(np.isnan(row)):

                first_logic_matrix = np.append(
                    first_logic_matrix, 0)  # consider 0

            else:

                first_logic_matrix = np.append(first_logic_matrix, 1)

    return first_logic_matrix


# function that returns the first logic matrix eith the period values
# repeated for the full array of date.
# and copies the boolean 1 and 0 for the period between start and stop date.
# this function allows the logic matrix to have the same dimension as the
# other matrix/dataframe and thus allowing calculation


def first_logic_matrix_reshape(
    first_logic_matrix,
    reference_date_array,
    Crypto_list,
    start_date="01-01-2016",
    end_date=None,
    time_column="N",
):

    reb_start = start_q()
    # rebalance_stop = stop_q(reb_start)

    # calling the function that yields the start and stop date couple
    reb_start_couple = next_quarterly_period(
        start_date, end_date, initial_val=0)

    # define the reshaped logic matrix as a dataframe with 'Time' in first
    # column and the reference date array as rows
    reshaped_matrix = pd.DataFrame(reference_date_array, columns=["Time"])

    # assigning 0 to new DataFrame columns, one for each crypto
    for crypto in Crypto_list:
        reshaped_matrix[crypto] = np.zeros(len(reference_date_array))

    # for every start and stop date couple fill the reshaped matrix with
    # the logic value finded in the input logic matrix
    i = 0
    for start, stop in reb_start_couple:

        if start == 1451606400:

            zero_element = np.zeros((1, 12))
            reshaped_matrix.loc[
                reshaped_matrix.Time.between(
                    start, stop, inclusive=True), Crypto_list
            ] = zero_element

        else:

            copied_element = np.array(
                first_logic_matrix.loc[first_logic_matrix.Time
                                       == start][Crypto_list]
            )
            reshaped_matrix.loc[
                reshaped_matrix.Time.between(
                    start, stop, inclusive=True), Crypto_list
            ] = copied_element
            i = i + 1

    if time_column == "N":

        reshaped_matrix = reshaped_matrix.drop(columns="Time")

    return reshaped_matrix


# add descr


def daily_perc_volumes(
    Crypto_Ex_Vol,
    Exchanges,
    Crypto,
    last_reb_start,
    next_reb_stop,
    curr_board_eve=None,
    start_date="01-01-2016",
    end_date=None,
    time_column="N"
):

    # day_in_sec = 86400
    #
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    # yesterday = today_TS - day_in_sec

    if curr_board_eve is None:

        curr_board_eve = today_TS

    Crypto_Ex_Vol["Crypto"] = Crypto

    # retrieving from MongoDB the df containg the volume of each Exchange
    db = "index"
    coll_volume = "all_exc_volume"
    tot_vol = mongo.query_mongo(db, coll_volume)
    # selecting only the element related to the specific "Crypto"
    tot_vol_c = tot_vol.loc[tot_vol.Crypto == Crypto]

    # append the new volume value to the df
    tot_vol_u = tot_vol_c.append(Crypto_Ex_Vol)

    # the board eve date is used to compute the values, the period of
    # computation goes from the previuos rebalance date to the eve of
    # the board date
    quarter_matrix = tot_vol_u.loc[tot_vol_u.Time.between(
        int(last_reb_start), int(curr_board_eve), inclusive=True)]
    quarter_matrix = quarter_matrix.drop(columns=["Time", "Crypto"])

    quarter_sum = quarter_matrix.sum()
    # finding relative percentage
    exchange_percentage = np.array(quarter_sum / quarter_sum.sum())
    exchange_percentage = np.transpose(exchange_percentage)

    if time_column != "N":

        rebalance_date_perc = np.column_stack(
            (next_reb_stop, exchange_percentage))
        header = ["Time"]
        header.extend(Exchanges)

    else:

        rebalance_date_perc = exchange_percentage
        header = Exchanges

    perc_df = pd.DataFrame(rebalance_date_perc)
    perc_df_T = perc_df.T
    perc_df_T.columns = header

    return perc_df_T


# add desc


def daily_first_logic(
    Crypto_Ex_Vol,
    Exchanges,
    Crypto,
    last_reb_start,
    next_reb_stop,
    curr_board_eve=None,
    start_date="01-01-2016"
):

    exchange_vol_percentage = daily_perc_volumes(
        Crypto_Ex_Vol, Exchanges, Crypto, last_reb_start,
        next_reb_stop, curr_board_eve
    )
    print(exchange_vol_percentage)
    first_logic_matrix = np.array([])

    # check if any of the value in array exchange_vol_percentage
    # is > than 0.80
    # If yes add a 0 value in the first_logic_matrix
    # if not add value 1 in the first_logic_matrix
    if np.any(exchange_vol_percentage > 0.8):

        first_logic_matrix = 0

    else:

        if np.any(np.isnan(exchange_vol_percentage)):

            first_logic_matrix = 0

        else:

            first_logic_matrix = 1

    return first_logic_matrix


# ############################################################################

# ###################### EXPONENTIAL MOVING AVERAGE FUNCTIONS ################

# function that returns an array with the value of the smoothing factor for
# 90 days (0-89) is utilized to calc the EWMA
# default lambda value is a standard and the period is set to be 90 days


def smoothing_factor(lambda_smooth=0.94, moving_average_period=90):

    # creates a vector of number between 0 and 89
    num_vector = np.array([range(moving_average_period)])
    num_vector = -np.sort(-num_vector)

    smooth_factor_array = np.array([])
    for index in num_vector:
        new_lambda = (1 - lambda_smooth) * (lambda_smooth ** (index))
        smooth_factor_array = np.append(smooth_factor_array, new_lambda)

    return smooth_factor_array


# function that compute the 90-days EWMA volume for each currency.
# it returns a dataframe with Crypto as columns and date as row
# each position has the daily EWMA of the relative cryptoasset.
# takes as input:
# Crypto_Volume_Matrix: Volume matrix where columns are crypto
# and rows timestamp format days
# Crypto_list: a list of all the Crypto Asset
# reference_date_array
# moving_average_period: the period that is set on 90 days as default
# if not otherwise specififed the returned dataframe do not the "Time" Column


def ewma_crypto_volume(
    Crypto_Volume_Matrix,
    Crypto_list,
    reference_date_array,
    start_date="01-01-2016",
    end_date=None,
    moving_average_period=90,
    time_column="N",
):

    ewma_matrix = np.array([])
    smoothing_array = smoothing_factor()

    for date in reference_date_array:
        stop = date
        start = date - (86400 * 89)
        try:

            period_volume = Crypto_Volume_Matrix.loc[
                Crypto_Volume_Matrix.Time.between(start, stop, inclusive=True),
                Crypto_list,
            ]
            period_average = (period_volume * smoothing_array[:, None]).sum()

            if ewma_matrix.size == 0:

                ewma_matrix = np.array(period_average)

            else:

                ewma_matrix = np.row_stack(
                    (ewma_matrix, np.array(period_average)))
        except:

            zero_array = np.zeros(len(Crypto_list))

            if ewma_matrix.size == 0:

                ewma_matrix = zero_array

            else:

                ewma_matrix = np.row_stack((ewma_matrix, zero_array))

    ewma_matrix = np.column_stack((reference_date_array, ewma_matrix))
    header = ["Time"]
    header.extend(Crypto_list)
    ewma_DF = pd.DataFrame(ewma_matrix, columns=header)

    if time_column == "N":

        ewma_DF = ewma_DF.drop(columns="Time")

    return ewma_DF


# daily ewma


def daily_ewma_crypto_volume(
    Crypto_Volume_Matrix, Crypto_list, stop=None, moving_average_period=90
):

    smoothing_array = smoothing_factor()
    if stop is None:

        last_row = Crypto_Volume_Matrix.tail(1)
        stop = int(last_row["Time"])

    else:

        stop = int(stop)

    start = stop - (86400 * 89)

    Crypto_Volume_Matrix["Time"] = pd.to_numeric(Crypto_Volume_Matrix["Time"])

    period_volume = Crypto_Volume_Matrix.loc[
        Crypto_Volume_Matrix.Time.between(
            start, stop, inclusive=True), Crypto_list
    ]
    period_average = np.array(
        (period_volume * smoothing_array[:, None]).sum(axis=0))

    period_avg_df = pd.DataFrame(period_average)
    ewma_DF = period_avg_df.T
    ewma_DF.columns = Crypto_list

    return ewma_DF


# function uses the first logic matrix and checks the ewma dataframe excluding
# the cryptoassets than cannot be eligible for every rebalancing periods
# (from quarter start date to quarter end date).
# it returns a dataframe of ewma where if the cryptoasset is eligible
# its value is untouched, otherwise, if the cryptoasset is to be excluded
# using the logic matrices, its value will be put to zero.


def ewma_first_logic_check(
    first_logic_matrix,
    ewma_dataframe,
    reference_date_array,
    Crypto_list,
    start_date="01-01-2016",
    end_date=None,
    time_column="N",
):

    # reshaping the logic matrix in order to make it of the
    # same dimensions of the ewma dataframe
    reshaped_logic_m = first_logic_matrix_reshape(
        first_logic_matrix, reference_date_array, Crypto_list
    )
    # reshaped_logic_m = reshaped_logic_m.drop(columns = 'Time')

    # multiplying the logic matrix and the ewma dataframe
    ewma_checked = ewma_dataframe * reshaped_logic_m

    if time_column == "Y":

        ewma_checked["Time"] = reference_date_array

    return ewma_checked


# ############################################################################

# ########################## SECOND LOGIC MATRIX #############################

# This function gives back the % of the EWMA-volume of any single cryptoasset
# compared to the aggregate EWMA-volume of all the cryptoasset over a defined
# interval, more specifically over the period between the reconstitution day
# (start of the quarter) and the eve of the board meeting day
# it takes as input:
# Crypto_Volume_Matrix: Volume matrix where columns are crypto and rows
# timestamp format days
# Crypto_list: a list of all the CrytpoAsset
# first logic matrix
# reference_date_array
# Note that the function returns the values required in order to verify
# if the 2nd requirement is respected.


def ewma_period_fraction(
    Crypto_Volume_Matrix,
    first_logic_matrix,
    Crypto_list,
    reference_date_array,
    start_date="01-01-2016",
    end_date=None,
    time_column="N",
):

    # defining the arrays of board eve date and start and stop of each quarter
    board_eve_array = day_before_board()

    ##
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    yesterday = today_TS - 86400
    board_eve_array = np.append(board_eve_array, yesterday)
    ##

    # find the EWMA of the volume
    ewma_crypto_vol = ewma_crypto_volume(
        Crypto_Volume_Matrix,
        Crypto_list,
        reference_date_array,
        start_date,
        end_date,
        time_column="N"
    )

    # check the EWMA dataframe using the first logic matrix
    ewma_logic = ewma_first_logic_check(
        first_logic_matrix,
        ewma_crypto_vol,
        reference_date_array,
        Crypto_list,
        start_date,
        end_date,
        time_column="Y"
    )

    ewma_volume_fraction = np.array([])
    start_vector = np.array([])
    next_reb_start = next_start()

    i = 1
    for start, stop in next_quarterly_period(initial_val=0):

        # taking the interval from start of quarter to the eve of the board day
        interval_ewma = ewma_logic[Crypto_list][
            ewma_logic["Time"].between(
                start, board_eve_array[i], inclusive=True)
        ]

        # sum the interval ewma and then find the percentage of
        # each crypto in term of ewma
        row_sum = interval_ewma.sum()
        percentage_row = row_sum / row_sum.sum()

        # add the single rebalance day to the matrix
        if start_vector.size == 0:

            # stop_vector = stop
            start_vector = np.array(int(next_reb_start[i]))
            ewma_volume_fraction = np.array(percentage_row)

        else:
            # stop_vector = np.row_stack((stop_vector, stop))
            start_vector = np.row_stack((start_vector, int(next_reb_start[i])))
            ewma_volume_fraction = np.row_stack(
                (ewma_volume_fraction, np.array(percentage_row))
            )

        i = i + 1

    if time_column != "N":

        ewma_volume_fraction = np.column_stack(
            (start_vector, ewma_volume_fraction))
        header = ["Time"]
        header.extend(Crypto_list)

    else:

        header = Crypto_list

    ewma_volume_fraction = pd.DataFrame(ewma_volume_fraction, columns=header)

    return ewma_volume_fraction


# add descr


def daily_ewma_fraction(
    ewma_row,
    first_logic_row,
    last_reb_start,
    board_date_eve,
    db_name=DB_NAME,
    coll_name="index_EWMA",
):

    before_eve = int(board_date_eve) - DAY_IN_SEC

    # retrieving the EWMA df from MongoDB
    ewma_df = query_mongo(db_name, coll_name)
    period_ewma = ewma_df.loc[
        ewma_df["Time"].between(int(last_reb_start),
                                int(before_eve), inclusive=True)
    ]

    period_ewma = period_ewma.drop(columns=["Time", "Date"])
    # adding the daily ewma row
    period_ewma = period_ewma.append(ewma_row)
    # reshaping the logic row
    log_head = first_logic_row.columns
    if "Time" in log_head:

        first_logic_row = first_logic_row.drop(columns="Time")

    crypto_list = period_ewma.columns
    logic_res = np.zeros_like(np.array(period_ewma))
    logic_res = pd.DataFrame(logic_res, columns=crypto_list)
    logic_res.loc[:, :] = np.array(first_logic_row)

    # multiplying the EWMA and the reshaped logic row
    ewma_logic = period_ewma * logic_res.values

    # sum the interval ewma and then find the percentage of
    # each crypto in term of ewma
    row_sum = ewma_logic.sum()
    percentage_row = np.array(row_sum) / np.array(row_sum.sum())

    # transforming into df
    percentage_row_df = pd.DataFrame(percentage_row)
    percentage_row_df_T = percentage_row_df.T
    percentage_row_df_T.columns = crypto_list

    return percentage_row_df_T


# This function creates a matrix of 1 and 0, depending wheter the
# second requirement is respected or not
# 2nd requirement: The crypto-asset's trailing trading volume,
# between the reconstitution day and the day before the committe meeting day,
# is not less (>=) than 2% of the aggregate  trading volume for the same
# period of available crypto-assets after the application of the precedent
# eligibility Rules.
# If the requirement is respected the function will put the value 1 on the
# matrix, if not it will put 0.


def second_logic_matrix(
    Crypto_Volume_Matrix,
    first_logic_matrix,
    Crypto_list,
    reference_date_array,
    time_column="Y"
):

    # finding the dataframe containing the relative ewma value at every
    # end date of the rebalancing period
    ewma_volume_fraction = ewma_period_fraction(
        Crypto_Volume_Matrix,
        first_logic_matrix,
        Crypto_list,
        reference_date_array,
        time_column="Y"
    )
    ewma_no_time = ewma_volume_fraction.drop(columns="Time")

    # create a dataframe where if the relative percentage is < 0.02 put 0
    # otherwise put 1
    second_logic_matrix = (ewma_no_time >= 0.02) * 1

    if time_column == "Y":

        second_logic_matrix["Time"] = ewma_volume_fraction["Time"]

    return second_logic_matrix


# the function returns the second logic matrix for the full array of date
# it takes as input the full second logic matrix (that has as many row as
# number of board meetings and as many columns as Cryptoasset in Cryptolist)
# and copies the boolean 1 and 0 for the period between start and stop date.
# this function allows the logic matrix to have the same dimension as the
# other matrix/dataframe and thus allowing calculation


def second_logic_matrix_reshape(
    second_logic_matrix,
    reference_date_array,
    Crypto_list,
    start_date="01-01-2016",
    end_date=None,
    time_column="N",
):

    rebalance_start = start_q()

    # calling the function that yields the start and stop date couple
    rebalance_start = next_quarterly_period(start_date, end_date, initial_val=0)

    # define the reshaped logic matrix as a dataframe with 'Time' in
    # first column and the reference date array as rows
    reshaped_matrix = pd.DataFrame(reference_date_array, columns=["Time"])

    # assigning 0 to new DataFrame columns, one for each crypto
    for crypto in Crypto_list:
        reshaped_matrix[crypto] = np.zeros(len(reference_date_array))

    # for every start and stop date couple fill the reshaped matrix
    # with the logic value finded in the input logic matrix
    i = 0
    for start, stop in rebalance_start:

        if start == 1451606400:

            zero_element = np.zeros((1, 12))
            reshaped_matrix.loc[
                reshaped_matrix.Time.between(
                    start, stop, inclusive=True), Crypto_list
            ] = zero_element

        else:
            copied_element = np.array(
                second_logic_matrix.loc[second_logic_matrix.Time
                                        == start][Crypto_list]
            )
            reshaped_matrix.loc[
                reshaped_matrix.Time.between(
                    start, stop, inclusive=True), Crypto_list
            ] = copied_element
            i = i + 1

    if time_column == "N":

        reshaped_matrix = reshaped_matrix.drop(columns="Time")

    return reshaped_matrix


# ############################################################################

# ######################### WEIGHTS COMPUTATION ##############################

# function uses the first and second logic matrix and checks the ewma
# dataframe excluding the cryptoassets than cannot be eligible for every
# rebalancing periods (from quarter start date to quarter end date).
# it returns a dataframe of ewma where if the cryptoasset is eligible its
# value is untouched, otherwise, if the cryptoasset is to be excluded using
# the logic matrices, its value will be put to zero.


def ewma_second_logic_check(
    first_logic_matrix,
    second_logic_matrix,
    ewma_dataframe,
    reference_date_array,
    Crypto_list,
    start_date="01-01-2016",
    end_date=None,
    time_column="N",
):

    # reshaping both the logic matrices in order to make them of the
    # same dimensions of the ewma dataframe
    reshaped_first_logic_m = first_logic_matrix_reshape(
        first_logic_matrix, reference_date_array, Crypto_list
    )
    print('here')
    print(reshaped_first_logic_m.tail(10))
    reshaped_second_logic_m = second_logic_matrix_reshape(
        second_logic_matrix, reference_date_array, Crypto_list
    )
    print(reshaped_second_logic_m.tail(10))
    # applying the first logic matrix to the ewma dataframe
    ewma_first_checked = ewma_dataframe * reshaped_first_logic_m

    # applying the second logic matrix to the previous result
    ewma_second_checked = ewma_first_checked * reshaped_second_logic_m

    if time_column != "N":

        ewma_second_checked["Time"] = reference_date_array

    return ewma_second_checked


# add descr


def daily_double_log_check(
    first_logic_row,
    second_logic_row,
    ewma_row,
    last_reb_start,
    board_date_eve,
    db_name=DB_NAME,
    coll_name="index_EWMA",
):

    before_eve = int(board_date_eve) - DAY_IN_SEC

    # retrieving the EWMA df from MongoDB
    ewma_df = query_mongo(db_name, coll_name)
    period_ewma = ewma_df.loc[
        ewma_df["Time"].between(int(last_reb_start),
                                int(before_eve), inclusive=True)
    ]
    period_ewma = period_ewma.drop(columns=["Time", "Date"])
    # adding the daily ewma row
    period_ewma = period_ewma.append(ewma_row)

    # reshaping the logic row
    log_head = first_logic_row.columns

    if "Time" in log_head:

        first_logic_row = first_logic_row.drop(columns="Time")

    crypto_list = period_ewma.columns

    first_res = pd.DataFrame(np.zeros_like(
        np.array(period_ewma)), columns=crypto_list)
    second_res = pd.DataFrame(np.zeros_like(
        np.array(period_ewma)), columns=crypto_list)

    f_l_array = np.array(first_logic_row)
    s_l_array = np.array(second_logic_row)
    first_res.loc[:, :] = f_l_array
    second_res.loc[:, :] = s_l_array

    # first logic check
    ewma_one = period_ewma * first_res.values

    # second logic check
    ewma_two = ewma_one * second_res.values

    return ewma_two


# function returns a dataframe (cryptoasset as columns, date as row) containing
# the weights referred to each cryptoasset; the function uses as input the
# double checked ewma allows to compute the weights considering only the
# eligible crypto assets
# takes as imput:
# ewma_double_logic_checked
# date : array containing date where the weights want to be computed


def quarter_weights(ewma_double_logic_checked, date, Crypto_list):

    quarter_weights = pd.DataFrame(date, columns=["Time"])

    # assigning 0 to new DataFrame columns, one for each crypto
    for crypto in Crypto_list:
        quarter_weights[crypto] = np.zeros(len(date))

    for day in date:

        row = np.array(
            ewma_double_logic_checked.loc[
                ewma_double_logic_checked.Time == day, Crypto_list
            ]
        )

        total_row = row.sum()
        weighted_row = row / total_row

        if row.size == len(Crypto_list):

            quarter_weights.loc[quarter_weights.Time
                                == day, Crypto_list] = weighted_row

    return quarter_weights


# ############################################################################

# ######################### SYNTHETIC MARKET CAP #############################

# function that return the syntethic weight for the index at the end of the
# day of the first day of each quarter


def quarterly_synt_matrix(
    Crypto_Price_Matrix,
    weights,
    reference_date_array,
    board_date_eve,
    Crypto_list,
    synt_ptf_value=100,
):

    # computing the percentage returns of cryptoasset prices (first row is NaN)
    # result is a pandas DF with columns = Crypto_list
    price_return = Crypto_Price_Matrix.pct_change()
    # adding the 'Time' column to price_return DF
    price_return["Time"] = reference_date_array

    rebalance_period = next_quarterly_period(initial_val=1)

    q_synt = np.array([])

    i = 1
    for start, stop in rebalance_period:

        start_weights = (
            weights[Crypto_list][weights["Time"] == board_date_eve[i]]
        ) * synt_ptf_value

        value_one = start_weights

        list_of_date = price_return.loc[
            price_return.Time.between(start, stop, inclusive=True), "Time"
        ]

        for date in list_of_date:

            increase_value = np.array(
                price_return[Crypto_list][price_return["Time"] == date]
            )
            increased_value = np.array((value_one) * (1 + increase_value))

            value_one = increased_value

            if q_synt.size == 0:

                q_synt = increased_value

            else:

                q_synt = np.row_stack((q_synt, increased_value))

        i = i + 1

    service_vector = np.zeros(
        (len(reference_date_array) - q_synt.shape[0], q_synt.shape[1])
    )
    q_synt = np.row_stack((service_vector, q_synt))
    q_synt_time = np.column_stack((reference_date_array, q_synt))

    header = ["Time"]
    header.extend(Crypto_list)
    q_synt_df = pd.DataFrame(q_synt_time, columns=header)

    return q_synt_df


# add descr


def daily_quart_synt_matrix(
    daily_price,
    quarter_weights,
    last_reb_start,
    curr_board_eve,
    Crypto_list,
    db_name="index",
    coll_name="crypto_price",
    synt_ptf_value=100,
):

    # retrieving the EWMA df from MongoDB
    price_tot = mongo.query_mongo(db_name, coll_name)
    before_start = last_reb_start - 86400
    before_eve = curr_board_eve - 86400
    period_price = price_tot.loc[
        price_tot["Time"].between(before_start, before_eve, inclusive=True)
    ]
    period_price.drop(columns=["Date"])
    # adding Time column to the daily price df
    daily_price["Time"] = curr_board_eve
    daily_price = daily_price[
        [
            "Time",
            "BTC",
            "ETH",
            "XRP",
            "LTC",
            "BCH",
            "EOS",
            "ETC",
            "ZEC",
            "ADA",
            "XLM",
            "XMR",
            "BSV",
        ]
    ]
    # append daily price to the period prices
    period_price = period_price.append(daily_price)

    # computing the price return of the period
    price_return = period_price.pct_change()
    price_return = price_return.loc[1:, :]
    # adding Time Column
    human_start = data_setup.timestamp_to_human(
        last_reb_start, date_format="%m-%d-%y")
    human_curr = data_setup.timestamp_to_human(
        curr_board_eve, date_format="%m-%d-%y")
    period_date_list = data_setup.date_gen(human_start, human_curr, EoD="N")
    price_return["Time"] = period_date_list

    #
    q_synt = np.array([])
    start_weights = quarter_weights.loc[:, Crypto_list] * synt_ptf_value
    value_one = start_weights

    for date in period_date_list:

        increase_value = np.array(
            price_return[Crypto_list][price_return["Time"] == date]
        )
        increased_value = np.array((value_one) * (1 + increase_value))

        value_one = increased_value

        if q_synt.size == 0:

            q_synt = increased_value

        else:

            q_synt = np.row_stack((q_synt, increased_value))

    # adding Time column
    q_synt_time = np.column_stack((period_date_list, q_synt))

    # transforming into dataframe
    header = ["Time"]
    header.extend(Crypto_list)
    q_synt_df = pd.DataFrame(q_synt_time, columns=header)

    return q_synt_df


# funcion that returns the syntethic matrix computed as percentage,
# specifically it takes the syntethic matrix as input and return
# a DF with the same number of rows and columns but instead of values
# starting from 100 in rebalance day, it returns the daily relative
# weights of each cryptoasset


def relative_syntethic_matrix(syntethic_matrix, Crypto_list):

    syntethic_matrix["row_sum"] = syntethic_matrix[Crypto_list].sum(axis=1)
    syntethic_matrix_new = syntethic_matrix.loc[:, Crypto_list].div(
        syntethic_matrix["row_sum"], axis=0
    )

    return syntethic_matrix_new


# ###########################################################################

# ######################### DIVISOR COMPUTATION #############################

# Return the Initial Divisor for the index. It identifies the position of the
# initial date in the Crypto_Volume_Matrix.
# At the moment the initial date is 2016/01/01 or 1451606400 as timestamp
# where:
# logic_matrix = second requirement Crypto_Volume_Matrix, composed by 0 if
# negative, 1 if positive
# Crypto_Price_Matrix = final price Crypto_Volume_Matrix of each currency
# sm = synthetic market cap derived weight


def initial_divisor(
    Crypto_Price_Matrix,
    Weights,
    Crypto_list,
    reference_date_array,
    starting_point=3,
    base=1000,
):

    # define the initial date
    rebalance_date = start_q()
    initial_date = rebalance_date[starting_point]
    print(initial_date)
    # computing the divisor
    price_row = np.array(
        Crypto_Price_Matrix.loc[Crypto_Price_Matrix.Time
                                == initial_date, Crypto_list]
    )
    weights_row = np.array(
        Weights.loc[Weights.Time == initial_date, Crypto_list])
    row = price_row * weights_row
    row_sum = row.sum()
    initial_divisor = np.array(row_sum) / base

    return initial_divisor


# function that returns an array with the divisor for each day
# the result is an array with time column as long as the reference
# date vector and the value of the divisor in the other column


def divisor_adjustment(
    Crypto_Price_Matrix, Weights, second_logic_matrix, Crypto_list, reference_date_array
):

    # use the function to compute the initial divisor
    old_divisor = initial_divisor(
        Crypto_Price_Matrix, Weights, Crypto_list, reference_date_array
    )
    print('old divisor')
    print(old_divisor)
    divisor_array = np.array(old_divisor)

    # start_quarter = start_q()
    next_start_quarter = next_start()

    try:

        second_logic_matrix["Time"] = next_start_quarter[1: len(
            next_start_quarter)]

    except ValueError:

        second_logic_matrix["Time"] = next_start_quarter[
            1: len(next_start_quarter) - 1
        ]

    # for loop that iterates through all the date (length of logic matrix)
    # returning a divisor for each day

    i = 3
    for date in next_start_quarter[4: len(next_start_quarter) - 1]:

        current_logic_row = np.array(
            second_logic_matrix.loc[second_logic_matrix.Time
                                    == date, Crypto_list]
        )

        previous_logic_row = np.array(
            second_logic_matrix.loc[
                second_logic_matrix.Time == next_start_quarter[i], Crypto_list
            ]
        )

        logic_compare = current_logic_row == previous_logic_row
        # check if the logic rows are the same
        # if yes the new divisor is the same as the old one
        if logic_compare.all() is True:

            new_divisor = old_divisor
            divisor_array = np.append(divisor_array, new_divisor)

        # if no compute the new divisor
        else:

            # retrive the price of the day before the
            # new rebalance date
            yesterday_price = np.array(
                Crypto_Price_Matrix.loc[
                    Crypto_Price_Matrix.Time == (int(date) - 86400), Crypto_list
                ]
            )
            # find current and old weights
            current_weights = np.array(
                Weights.loc[Weights.Time == date, Crypto_list])
            print(current_weights)
            previous_weights = np.array(
                Weights.loc[Weights.Time == next_start_quarter[i], Crypto_list]
            )
            print(previous_weights)
            # compute the new divisor of the quarter
            numer = np.array(current_logic_row
                             * yesterday_price * current_weights)
            denom = np.array(previous_logic_row
                             * yesterday_price * previous_weights)
            new_divisor = (numer.sum() / denom.sum()) * old_divisor

            # add the quarter divisor to the divisor array
            divisor_array = np.append(divisor_array, new_divisor)

        old_divisor = new_divisor
        i = i + 1

    print(divisor_array)
    divisor_array = np.column_stack(
        (next_start_quarter[3: len(next_start_quarter) - 1], divisor_array)
    )

    header = ["Time", "Divisor Value"]
    divisor_df = pd.DataFrame(divisor_array, columns=header)

    return divisor_df


# the function computes the new divisor of the new quarter
# it takes as input:
# yesterday_price: numpy array containing the list of prices
# Weights: dataframe containing all the historical weights
# second_logic_matrix: dataframe containing the complete
# historical 2nd logic matrix
# old_reb_start: previuos rebalance start date in timestamp
# new_reb_start: current rebalance srat date in timestamp
# the function returns a DF containing the new divisor and
# its relative time in timestamp (columns=['Time','Divisor Value'])


def new_divisor(
    yesterday_price,
    Weights,
    second_logic_matrix,
    Crypto_list,
    old_reb_start,
    new_reb_start=None,
    db_name="index",
    coll_name="index_divisor",
):

    if new_reb_start is None:

        day_in_sec = 86400
        today_str = datetime.now().strftime("%Y-%m-%d")
        today = datetime.strptime(today_str, "%Y-%m-%d")
        today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
        new_reb_start = today_TS - day_in_sec

    else:

        new_reb_start = int(new_reb_start)

    # use the function to compute the initial divisor
    old_divisor_df = mongo.query_mongo(db_name, coll_name)
    old_divisor = old_divisor_df.loc[
        old_divisor_df.Time == int(old_reb_start), ["Divisor Value"]
    ]

    old_divisor = np.array(old_divisor)

    # for loop that iterates through all the date (length of logic matrix)
    # returning a divisor for each day

    current_logic_row = np.array(
        second_logic_matrix.loc[second_logic_matrix.Time
                                == new_reb_start, Crypto_list]
    )
    previous_logic_row = np.array(
        second_logic_matrix.loc[second_logic_matrix.Time
                                == old_reb_start, Crypto_list]
    )
    logic_compare = current_logic_row == previous_logic_row
    # check if the logic rows are the same
    if logic_compare.all() is True:

        new_divisor = old_divisor

    else:

        current_weights = np.array(
            Weights.loc[Weights.Time == new_reb_start, Crypto_list]
        )
        previous_weights = np.array(
            Weights.loc[Weights.Time == old_reb_start, Crypto_list]
        )

        numer = np.array(current_logic_row * yesterday_price * current_weights)
        denom = np.array(previous_logic_row
                         * yesterday_price * previous_weights)

        new_divisor = (numer.sum() / denom.sum()) * old_divisor

    print(new_divisor)
    print(new_reb_start)
    new_divisor_mat = np.column_stack((new_reb_start, new_divisor))
    header = ["Time", "Divisor Value"]
    divisor_df = pd.DataFrame(new_divisor_mat, columns=header)

    return divisor_df


# ###########################################################################

# #################### INDEX VALUE COMPUTATION ##############################

# the function creates an array with the lenght of the reference date array
# and, from each start/stop quarter period, repeats the value of the
# divisor


def divisor_reshape(
    divisor_adjustment_array,
    reference_date_array,
    start_date="01-01-2016",
    end_date=None,
    time_column="Y",
):

    # calling the function that yields the start and stop date couple
    rebalance_start = next_quarterly_period(start_date, end_date, initial_val=0)

    # define the reshaped logic matrix as a dataframe with 'Time'
    # in first column and the reference date array as rows
    reshaped_matrix = pd.DataFrame(reference_date_array, columns=["Time"])

    # assigning 0 to new DataFrame columns 'Divisor Value'
    reshaped_matrix["Divisor Value"] = np.zeros(len(reference_date_array))
    column_name = "Divisor Value"

    # for every start and stop date couple fill the reshaped
    # matrix with the divisor value

    for start, stop in rebalance_start:

        copied_element = np.array(
            divisor_adjustment_array.loc[divisor_adjustment_array.Time == start][
                "Divisor Value"
            ]
        )

        if len(copied_element) == 0:

            pass

        else:

            reshaped_matrix.loc[
                reshaped_matrix.Time.between(
                    start, stop, inclusive=True), column_name
            ] = copied_element

    if time_column == "N":

        reshaped_matrix = reshaped_matrix.drop(columns="Time")

    return reshaped_matrix


# function that returns an array of the daily level of the Index, where:
# Crypto_Price_Matrix = final currencies price
# Crypto_Volume_Matrix = final currencies Volume
# relative_syntethic_matrix = matrix containing the synthetic daily weights
# divisor_adjustment_array = array containing the divisor for each period


def index_level_calc(
    Crypto_Price_Matrix,
    relative_syntethic_matrix,
    divisor_adjustment_array,
    reference_date_array,
    initial_date="01-01-2016",
):

    # find the divisor related to each day starting from initial date
    reshaped_divisor = divisor_reshape(
        divisor_adjustment_array, reference_date_array, initial_date
    )

    # drop the "Time" column for each dataframe
    Crypto_Price_Matrix = Crypto_Price_Matrix.drop(columns="Time")
    try:

        relative_syntethic_matrix = relative_syntethic_matrix.drop(
            columns="Time")

    except KeyError:

        pass

    reshaped_divisor = reshaped_divisor.drop(columns="Time")

    # filling the NaN of the synth matrix with zero
    relative_syntethic_matrix = relative_syntethic_matrix.fillna(0)

    # multiplyng the price matrix and the synth matrix
    numerator = np.array(Crypto_Price_Matrix) * \
        np.array(relative_syntethic_matrix)
    # performing the sum for each row
    numerator_sum = numerator.sum(axis=1)
    num = pd.DataFrame(numerator_sum)

    # changing 0 into nan in order to avoid ZerodivisionError
    den = pd.DataFrame(reshaped_divisor)
    den = den.where(den != 0.000000, np.nan)

    # index value computation
    index_value = np.array(num) / np.array(den)

    # stacking together reference_date array and the insex values
    index_value = np.column_stack((reference_date_array, np.array(index_value)))

    # put the data into a dataframe
    header = ["Time", "Index Value"]
    index_df = pd.DataFrame(index_value, columns=header)

    return index_df


# function that allows to compute the index level in base 1000 (default)


def index_based(index_df, base=1000):

    # defining time column and values column
    time_column = index_df["Time"]
    value_column = index_df["Index Value"]

    value_column = value_column.fillna(0)
    variation = value_column.pct_change()
    variation = variation.replace(np.inf, np.nan)
    variation = variation.fillna(0)
    variation = np.array(variation)
    variation = variation[1: len(variation)]

    index_1000_based = np.array([base])
    current_value = base

    for element in variation:

        next_value = current_value * (1 + element)
        index_1000_based = np.append(index_1000_based, next_value)

        current_value = next_value

    index_1000_based = np.column_stack((time_column, index_1000_based))
    header = ["Time", "Index Value"]
    index_1000_based = pd.DataFrame(index_1000_based, columns=header)

    return index_1000_based


# ##########################################################################
# ################### CONVERSION FUNCTION ##################################

def conv_into_usd(db, data_df, fiat_rate_df, stable_rate_df, fiat_list, stablecoin_list):

    fiat_rate_df = fiat_rate_df.rename({"Date": "Time"}, axis="columns")
    # leave out the rates referred to 2015-12-31
    fiat_rate_df = fiat_rate_df.loc[fiat_rate_df.Time != "1451520000"]

    # creating a column containing the fiat currency
    fiat_rate_df["fiat"] = [x[:3].lower() for x in fiat_rate_df["Currency"]]
    data_df["fiat"] = [x[3:].lower() for x in data_df["Pair"]]
    stable_rate_df["fiat"] = [x[:4].lower()
                              for x in stable_rate_df["Currency"]]

    # ############ creating a USD subset which will not be converted #########

    usd_matrix = data_df.loc[data_df["fiat"] == "usd"]
    usd_matrix = df_reorder(usd_matrix, "conversion")

    # ########### converting non-USD fiat currencies #########################

    conv_matrix = data_df.loc[data_df["fiat"].isin(fiat_list)]

    # merging the dataset on 'Time' and 'fiat' column
    conv_merged = pd.merge(conv_matrix, fiat_rate_df, on=["Time", "fiat"])

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
    conv_merged = df_reorder(conv_merged, "conversion")

    # ############## converting STABLECOINS currencies #################

    # creating a matrix for stablecoins
    stablecoin_matrix = data_df.loc[data_df["fiat"].isin(stablecoin_list)]

    # merging the dataset on 'Time' and 'fiat' column
    stable_merged = pd.merge(
        stablecoin_matrix, stable_rate_df, on=["Time", "fiat"])

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
    stable_merged = df_reorder(stable_merged, "conversion")

    # reunite the dataframes
    converted_data = conv_merged
    converted_data = converted_data.append(stable_merged)
    converted_data = converted_data.append(usd_matrix)

    converted_data = converted_data.sort_values(by=["Time"])

    return converted_data


# ############################################################################
# ######################### STABLE COIN RATES FUNCTIONS ######################


def btcusd_average(db, collection_mongo, exchange_list, exc_to_start="kraken", day_to_comp=None):

    Exchanges = exchange_list

    # defining the query details
    if day_to_comp is None:

        first_query = {"Pair": "btcusd", "Exchange": exc_to_start}

    else:

        first_query = {"Pair": "btcusd",
                       "Exchange": exc_to_start, "Time": str(day_to_comp)}

    # retrieving the
    print(day_to_comp)
    print(collection_mongo)
    first_call = query_mongo(
        db, MONGO_DICT.get(collection_mongo), first_query)
    print(first_call)
    # isolating some values in single variables
    time_arr = first_call[["Time"]]
    price_df = first_call[["Close Price"]]
    volume_df = first_call[["Pair Volume"]]
    price_df = price_df.rename(columns={"Close Price": exc_to_start})
    volume_df = volume_df.rename(columns={"Pair Volume": exc_to_start})
    Exchanges.remove("kraken")

    for exchange in Exchanges:

        if day_to_comp is None:

            query = {"Pair": "btcusd", "Exchange": exchange}

        else:

            query = {"Pair": "btcusd", "Exchange": exchange,
                     "Time": str(day_to_comp)}

        single_ex = query_mongo(
            db, MONGO_DICT.get(collection_mongo), query)

    try:
        single_price = single_ex["Close Price"]
        single_vol = single_ex["Pair Volume"]
        price_df[exchange] = single_price
        volume_df[exchange] = single_vol

    except TypeError:
        pass

    num = (price_df * volume_df).sum(axis=1)
    den = volume_df.sum(axis=1)

    average_usd = num / den
    average_df = pd.DataFrame(average_usd, columns=["average usd"])
    average_df["Time"] = time_arr
    average_df = average_df.replace([np.inf, -np.inf], np.nan)
    average_df.fillna(0, inplace=True)

    return average_df


def stable_single_exc(db, collection_mongo, exchange, stable_coin, average_df, day_to_comp=None):

    pair_for_query = "btc" + stable_coin.lower()

    if day_to_comp is None:

        query_dict = {"Exchange": exchange, "Pair": pair_for_query}

    else:

        query_dict = {"Exchange": exchange,
                      "Pair": pair_for_query, "Time": str(day_to_comp)}

    # querying on MongoDB
    exc_df = query_mongo(db, MONGO_DICT.get(collection_mongo), query_dict)

    # computing the desired values using the obtained df and the usd average df
    exc_df["rate"] = exc_df["Close Price"] / average_df["average usd"]
    exc_df.fillna(0, inplace=True)

    # isolating the useful information in two diffewrent df
    exc_df_weighted = exc_df["rate"] * exc_df["Pair Volume"]
    exc_df_vol = exc_df["Pair Volume"]

    return exc_df_vol, exc_df_weighted


def stable_rate_calc(db, collection_mongo, stable_coin, stable_exc_list, average_df, day_to_comp=None):

    tot_df_w = []
    tot_df_v = []

    i = 0
    for exc in stable_exc_list:

        single_exc_vol, single_exc_weighted = stable_single_exc(
            db, collection_mongo, exc, stable_coin, average_df, day_to_comp)

        if i == 0:

            tot_df_w = single_exc_weighted
            tot_df_v = single_exc_vol

        else:

            tot_df_w = pd.concat([tot_df_w, single_exc_weighted], axis=1)
            tot_df_v = pd.concat([tot_df_v, single_exc_vol], axis=1)

        i = i + 1

    tot_df_w["sum"] = tot_df_w.sum(axis=1)
    tot_df_v["sum"] = tot_df_v.sum(axis=1)

    rates_arr = 1 / np.array((tot_df_w["sum"] / tot_df_v["sum"]))

    # tranforming the data structure into a dataframe
    rates_df = pd.DataFrame(rates_arr, columns=["Rate"])
    rates_df = rates_df.replace([np.inf, -np.inf], np.nan)
    rates_df.fillna(0, inplace=True)

    # adding Currency (USDT/USD), Time (timestamp),
    # and Standard Date (YYYY-MM-DD) columns
    rates_df["Currency"] = np.zeros(len(rates_df["Rate"]))
    rates_df["Currency"] = [
        str(x).replace("0.0", stable_coin + "/USD") for x in rates_df["Currency"]
    ]
    rates_df["Time"] = average_df["Time"]

    # correcting potential lenght mismatches
    rates_df.fillna("NaN", inplace=True)
    index_to_remove = rates_df[rates_df.Time == "NaN"].index
    rates_df = rates_df.drop(index_to_remove)

    # adding a column with human readable date
    rates_df["Standard Date"] = timestamp_to_human(average_df["Time"])

    # correcting the date 2016-10-02 for USDT using the previous day rate

    if day_to_comp is None:

        if stable_coin == "USDT":

            prev_rate = np.array(
                rates_df.loc[rates_df.Time == '1475280000', "Rate"])
            rates_df.loc[rates_df.Time == '1475366400', "Rate"] = prev_rate

    return rates_df


# #####################################################################
# ############### LOGIC MATRIX OF KEYS ################################

def key_list_creation(exchange_list, asset_list, fiat_list):

    # creating the list containing all the possible exchange-pair key

    all_key = []

    for exc in exchange_list:

        for cry in asset_list:

            for fiat in fiat_list:

                all_key.append(exc + "&" + cry.lower() + fiat)

    return all_key


def key_log_mat(db, collection, time_to_query, exchange_list, asset_list, fiat_list):

    # retriving the needed information on MongoDB
    q_dict = {"Time": str(time_to_query)}
    matrix_last_day = query_mongo(
        db, MONGO_DICT.get(collection), q_dict)

    # defining the list of header of the matrix
    old_head = matrix_last_day.columns

    # composing the "key" and assigining 1 as "logic value"
    matrix_last_day["key"] = matrix_last_day["Exchange"] + \
        "&" + matrix_last_day["Pair"]
    matrix_last_day["logic_value"] = 1

    # deleting the unuseful header
    matrix_last_day = matrix_last_day.drop(columns=old_head)

    # creating the list of every possible keys
    all_key = key_list_creation(exchange_list, asset_list, fiat_list)

    # creating the DF with every keys and assigning each a zero as logic value
    header = ["key", "logic_value"]
    key_df = pd.DataFrame(columns=header)
    key_df["key"] = all_key
    key_df["logic_value"] = 0

    # deleting from the DF all the keys that are present
    # in the matrix_last_day df
    key_df = key_df.loc[~key_df.key.isin(matrix_last_day["key"])]

    # appending the keys present in the matrix_last_day df
    key_df = key_df.append(matrix_last_day)

    return key_df
