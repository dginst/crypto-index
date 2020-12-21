# standard library import
from datetime import datetime, timezone

# third party import
import numpy as np
import pandas as pd

# local import

from cryptoindex.calc import (start_q, stop_q, day_before_board,
                              next_start, daily_first_logic,
                              daily_ewma_crypto_volume, daily_ewma_fraction,
                              daily_double_log_check, quarter_weights,
                              new_divisor_comp
                              )
from cryptoindex.data_setup import (
    date_gen, timestamp_to_human
)
from cryptoindex.mongo_setup import (
    mongo_indexing, mongo_upload,
    query_mongo, mongo_daily_delete
)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, PAIR_ARRAY,
    CRYPTO_ASSET, EXCHANGES, DB_NAME,
    DAY_IN_SEC, SYNT_PTF_VALUE
)


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


def loop_crypto_fiat(ccy_fiat_vol, ccy_fiat_price_vol):

    # first if condition: the crypto-fiat is in more than 1 exchange
    if ccy_fiat_vol.size != 0 and ccy_fiat_vol.size > 1:

        price_vol_sum = ccy_fiat_price_vol.sum(axis=1)
        vol_sum = ccy_fiat_vol.sum(axis=1)
        ccy_fiat_price = np.divide(
            price_vol_sum, vol_sum, out=np.zeros_like(vol_sum), where=vol_sum != 0.0)

        # computing the total volume of the exchange
        ccy_fiat_vol = ccy_fiat_vol.sum(axis=1)
        # computing price X volume of the exchange
        ccy_fiat_price_vol = ccy_fiat_price * ccy_fiat_vol

    # second if condition: the crypto-fiat is traded in just one exchange
    elif ccy_fiat_vol.size != 0 and ccy_fiat_vol.size == 1:

        np.seterr(all=None, divide="warn")
        ccy_fiat_price = np.divide(
            ccy_fiat_price_vol,
            ccy_fiat_vol,
            out=np.zeros_like(ccy_fiat_vol),
            where=ccy_fiat_vol != 0.0,
        )
        ccy_fiat_price = np.nan_to_num(ccy_fiat_price)
        ccy_fiat_price_vol = ccy_fiat_price * ccy_fiat_vol

    # third if condition: the crypto-fiat is not traded at all
    else:

        ccy_fiat_price = np.array([])
        ccy_fiat_vol = np.array([])
        ccy_fiat_price_vol = np.array([])

    return ccy_fiat_price, ccy_fiat_vol, ccy_fiat_price_vol


def loop_single_exc(exc_price, exc_vol, exc_price_vol,
                    ccy_fiat_price, ccy_fiat_vol,
                    ccy_fiat_price_vol):

    # creating every loop the matrices containing the data referred to all
    # the exchanges exc_price contains the crypto ("cp") prices in all
    # the different Exchanges, exc_vol contains the crypto ("cp")
    # volume in all the different Exchanges if no values is found,
    # script put "0" instead
    if exc_price.size == 0:

        if ccy_fiat_vol.size != 0:

            exc_price = ccy_fiat_price
            exc_vol = ccy_fiat_vol
            exc_price_vol = ccy_fiat_price_vol

        else:

            exc_price = np.zeros(1)
            exc_vol = np.zeros(1)
            exc_price_vol = np.zeros(1)

    else:

        if ccy_fiat_vol.size != 0:

            exc_price = np.column_stack(
                (exc_price, ccy_fiat_price))
            exc_vol = np.column_stack(
                (exc_vol, ccy_fiat_vol))
            exc_price_vol = np.column_stack((exc_price_vol, ccy_fiat_price_vol))

        else:

            exc_price = np.column_stack((exc_price, np.zeros(1)))
            exc_vol = np.column_stack(
                (exc_vol, np.zeros(1)))
            exc_price_vol = np.column_stack((exc_price_vol, np.zeros(1)))

    return exc_price, exc_vol, exc_price_vol


def exc_all_vol(single_crypto, exc_vol_df, exc_vol_tot_old):

    exc_vol_p = exc_vol_df
    exc_vol_p["Crypto"] = single_crypto
    exc_vol_tot_new = exc_vol_tot_old.append(exc_vol_p)

    return exc_vol_tot_new


def index_board_logic_arr(logic_one_arr,
                          exc_vol_df, single_crypto,
                          exc_list, last_reb_start,
                          next_reb_stop, curr_board_eve):

    # for each CryptoAsset compute the first logic array
    first_logic_value = daily_first_logic(
        exc_vol_df, exc_list, single_crypto, last_reb_start,
        next_reb_stop, curr_board_eve
    )

    # put the logic array into the logic matrix
    if logic_one_arr.size == 0:
        logic_one_arr = np.array(first_logic_value)
    else:
        logic_one_arr = np.column_stack(
            (logic_one_arr, first_logic_value))

    return logic_one_arr


def loop_exc_value(single_crypto, exc_price, exc_vol,
                   exc_price_vol, exc_vol_tot_old,
                   day_TS, exc_list, logic_one_arr,
                   last_reb_start=None, next_reb_stop=None,
                   curr_board_eve=None, day_type=None):

    # dataframes that contain volume and price of a single crytpo
    # for all the exchanges. If an exchange does not have value
    # in the crypto will be insertd a column with zero
    exc_vol_df = pd.DataFrame(exc_vol, columns=EXCHANGES)
    exc_price_df = pd.DataFrame(exc_price, columns=EXCHANGES)

    # adding "Time" column to both Exchanges dataframe
    exc_vol_df["Time"] = int(day_TS)
    exc_price_df["Time"] = int(day_TS)

    exc_vol_tot_new = exc_all_vol(single_crypto, exc_vol_df, exc_vol_tot_old)

    if day_type is None:

        pass

    elif day_type == "board":

        logic_one_arr = index_board_logic_arr(logic_one_arr, exc_vol_df, single_crypto,
                                              exc_list, last_reb_start,
                                              next_reb_stop, curr_board_eve)
    elif day_type == "q_start":

        pass

    try:

        # computing the volume weighted average price of the single
        # Crypto_Asset ("single_crypto") into a single vector
        exc_price_num = exc_price_vol.sum(axis=1)
        exc_price_den = exc_vol.sum(axis=1)
        exc_price = np.divide(
            exc_price_num,
            exc_price_den,
            out=np.zeros_like(exc_price_num),
            where=exc_price_num != 0.0,
        )
        # computing the total volume  average price of the
        # single Crypto_Asset ("single_crypto") into a single vector
        exc_vol = exc_vol.sum(axis=1)

    except np.AxisError:

        exc_price = exc_price
        exc_vol = exc_vol

    return exc_price, exc_vol, exc_vol_tot_new, logic_one_arr


def crypto_fiat_gen(single_crypto, crypto_fiat_arr, pair_list):

    for pair in pair_list:

        crypto_fiat_arr.append(single_crypto.lower() + pair)

    return crypto_fiat_arr


def index_daily_loop(data_matrix, crypto_asset, exc_list,
                     pair_list, day, last_reb_start=None,
                     next_reb_stop=None,
                     curr_board_eve=None, day_type=None):

    # initialize the matrices that will contain the prices
    # and volumes of all the single_cryptosset
    crypto_asset_price = np.matrix([])
    crypto_asset_vol = np.matrix([])

    logic_one_arr = np.matrix([])

    # initialize the matrix that contain the volumes per Exchange
    exc_head = exc_list
    exc_head.append("Time")
    exc_head.append("Crypto")
    exc_vol_tot = pd.DataFrame(columns=exc_head)

    for single_crypto in crypto_asset:

        # initialize useful matrices
        crypto_fiat_arr = []
        exc_price = np.matrix([])
        exc_vol = np.matrix([])
        exc_price_vol = np.matrix([])

        crypto_fiat_arr = crypto_fiat_gen(
            single_crypto, crypto_fiat_arr, pair_list)

        for exchange in exc_list:

            # initialize the matrices that will contain the data related
            # to all currencypair for the single exchange
            ccy_fiat_price_vol = np.matrix([])
            ccy_fiat_vol = np.matrix([])
            ccy_fiat_price = np.matrix([])

            for cp in crypto_fiat_arr:
                print(cp)

                crypto = cp[:3]
                fiat_curr = cp[3:]

                # ######### LEAVING OUT NEW CRYPTO-FIAT PAIRS ##################

                c_1 = exchange == "bittrex" and fiat_curr == "eur"
                c_2 = exchange == "bittrex" and crypto == "ltc" and fiat_curr == "usd"
                c_3 = exchange == "poloniex" and crypto == "bch" and fiat_curr == "usdc"

                if c_1 or c_2 or c_3:

                    continue

                # ##############################################################

                # selecting the data referring to specific exchange and crypto-fiat pair
                matrix = data_matrix.loc[
                    (data_matrix["Exchange"] == exchange) & (
                        data_matrix["Pair"] == cp)
                ]

                if matrix.empty is False:

                    price = np.array(float(matrix["Close Price"]))
                    volume = np.array(float(matrix["Pair Volume"]))
                    price_vol = np.array(price * volume)

                    # every "cp" the loop adds a column in the matrices referred
                    # to the single "exchange"
                    if ccy_fiat_price_vol.size == 0:
                        ccy_fiat_price_vol = price_vol
                        ccy_fiat_vol = volume
                    else:
                        ccy_fiat_price_vol = np.column_stack(
                            (ccy_fiat_price_vol, price_vol)
                        )
                        ccy_fiat_vol = np.column_stack((ccy_fiat_vol, volume))

                else:
                    pass

            # computing the volume weighted average price of the single exchange

            ccy_fiat_price, ccy_fiat_vol, ccy_fiat_price_vol = loop_crypto_fiat(
                ccy_fiat_vol, ccy_fiat_price_vol)

            exc_price, exc_vol, exc_price_vol = loop_single_exc(
                exc_price, exc_vol, exc_price_vol, ccy_fiat_price,
                ccy_fiat_vol, ccy_fiat_price_vol)

        exc_price, exc_vol, exc_vol_tot, logic_one_arr = loop_exc_value(
            single_crypto, exc_price, exc_vol, exc_price_vol,
            exc_vol_tot, day, exc_list, logic_one_arr,
            last_reb_start, next_reb_stop,
            curr_board_eve, day_type)

        # creating every loop the matrices of all the single_cryptossets
        # Crypto_Asset_Price contains the prices of all the cryptocurrencies
        # crypto_asset_vol contains the volume of all the cryptocurrencies
        crypto_asset_price, crypto_asset_vol = loop_crypto_asset(
            exc_price, exc_vol, crypto_asset_price, crypto_asset_vol)

    return crypto_asset_price, crypto_asset_vol, exc_vol_tot, logic_one_arr


def loop_crypto_asset(exc_price, exc_vol, crypto_asset_price, crypto_asset_vol):

    if crypto_asset_price.size == 0:

        crypto_asset_price = exc_price
        crypto_asset_vol = exc_vol

    else:

        crypto_asset_price = np.column_stack(
            (crypto_asset_price, exc_price))
        crypto_asset_vol = np.column_stack(
            (crypto_asset_vol, exc_vol))

    return crypto_asset_price, crypto_asset_vol


def index_daily_operation(crypto_asset, crypto_asset_price,
                          crypto_asset_vol, day_before_TS,
                          curr_reb_start, next_reb_start,
                          curr_board_eve, logic_one_arr=None,
                          day_type=None, day=None):

    two_before_TS = day_before_TS - DAY_IN_SEC
    two_before_human = timestamp_to_human([two_before_TS])

    # turn prices and volumes into pandas dataframe
    crypto_asset_price = pd.DataFrame(crypto_asset_price, columns=crypto_asset)
    crypto_asset_vol = pd.DataFrame(crypto_asset_vol, columns=crypto_asset)

    # compute the price return of the day
    two_before_price = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_price"), {"Time": two_before_TS})
    two_before_price = two_before_price.drop(columns=["Time", "Date"])
    return_df = two_before_price.append(crypto_asset_price)
    price_ret = return_df.pct_change()
    price_ret = price_ret.iloc[[1]]
    price_ret = price_ret.replace(-1, 0)
    price_ret.fillna(0, inplace=True)
    # then add the 'Time' column
    time_header = ["Time"]
    time_header.extend(crypto_asset)
    crypto_asset_price = pd.DataFrame(crypto_asset_price, columns=time_header)
    crypto_asset_price["Time"] = int(day_before_TS)
    crypto_asset_vol = pd.DataFrame(crypto_asset_vol, columns=time_header)
    crypto_asset_vol["Time"] = int(day_before_TS)
    # adding the Time column to the price ret df
    price_ret["Time"] = int(day_before_TS)

    # computing the Exponential Weighted Moving Average of the day
    hist_volume = query_mongo(DB_NAME, MONGO_DICT.get("coll_volume"))
    hist_volume = hist_volume.drop(columns=["Date"])
    hist_volume = hist_volume.append(crypto_asset_vol)
    daily_ewma = daily_ewma_crypto_volume(hist_volume, crypto_asset)

    if day_type == "board":

        (first_logic_row, second_logic_row,
         weights_for_board, daily_ewma_double_check) = index_board_logic_op(
            crypto_asset, logic_one_arr, daily_ewma,
            curr_reb_start, next_reb_start, curr_board_eve)

    else:

        daily_ewma_double_check = index_norm_logic_op(
            crypto_asset, daily_ewma)
        first_logic_row = []
        second_logic_row = []

    # compute the daily syntethic matrix

    if day_type == "start_q":

        daily_synth = new_synth_op(crypto_asset, price_ret, SYNT_PTF_VALUE)

    else:

        yesterday_synt_matrix = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_synt"), {"Date": two_before_human[0]}
        )
        yesterday_synt_matrix = yesterday_synt_matrix.drop(
            columns=["Date", "Time"])
        daily_return = np.array(price_ret.loc[:, crypto_asset])
        new_synt = (1 + daily_return) * np.array(yesterday_synt_matrix)
        daily_synth = pd.DataFrame(new_synt, columns=crypto_asset)

    # compute the daily relative syntethic matrix
    daily_tot = np.array(daily_synth.sum(axis=1))

    daily_rel = np.array(daily_synth) / daily_tot
    daily_rel = pd.DataFrame(daily_rel, columns=crypto_asset)

    # daily index value computation

    if day_type == "start_q":

        new_divisor = new_divisor_comp(DB_NAME, two_before_price, crypto_asset)
        curr_divisor = new_divisor

    else:

        curr_divisor = query_mongo(
            DB_NAME, MONGO_DICT.get("coll_divisor_res"), {
                "Date": two_before_human[0]}
        )

    curr_div_val = np.array(curr_divisor["Divisor Value"])

    index_numerator = np.array(
        crypto_asset_price[crypto_asset]) * np.array(daily_rel)
    numerator_sum = index_numerator.sum(axis=1)
    num = pd.DataFrame(numerator_sum)
    daily_index_value = np.array(num) / curr_div_val
    raw_index_df = pd.DataFrame(daily_index_value, columns=["Index Value"])

    # retrieving from mongoDB the yesterday value of the raw index
    yesterday_raw_index = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_raw_index"), {"Date": two_before_human[0]}
    )
    yesterday_raw_index = yesterday_raw_index.drop(columns=["Date", "Time"])
    raw_curr = yesterday_raw_index.append(raw_index_df)
    variation = raw_curr.pct_change()
    variation = np.array(variation.iloc[1])

    # retrieving from mongoDB the yesterday value of the raw index
    yesterday_1000_index = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_1000_index"), {
            "Date": two_before_human[0]}
    )
    daily_index_1000 = np.array(
        yesterday_1000_index["Index Value"]) * (1 + variation)
    daily_index_1000_df = pd.DataFrame(
        daily_index_1000, columns=["Index Value"])

    return (crypto_asset_price, crypto_asset_vol, price_ret, daily_ewma,
            daily_ewma_double_check, daily_synth, daily_rel, curr_divisor,
            first_logic_row, second_logic_row, weights_for_board,
            daily_index_1000_df, raw_index_df, new_divisor)


def new_synth_op(crypto_asset, daily_return_df, synt_ptf_value):

    past_start_quarter_list = start_q()
    new_start_q = past_start_quarter_list[len(past_start_quarter_list) - 1]

    # downloading from mongoDB the current weights
    tot_weights = query_mongo(DB_NAME, MONGO_DICT.get("coll_weights"))
    new_weights = tot_weights.loc[tot_weights.Time == int(
        new_start_q), crypto_asset]

    # daily syntethic matrix computation
    daily_ret_arr = np.array(daily_return_df[crypto_asset])
    synt_weights = np.array(new_weights) * synt_ptf_value
    new_synth = synt_weights * (1 + daily_ret_arr)
    daily_synthh = pd.DataFrame(new_synth, columns=crypto_asset)

    return daily_synthh


def index_board_logic_op(crypto_asset, logic_one_arr, daily_ewma,
                         curr_reb_start, next_reb_start,
                         curr_board_eve):

    # turn the first logic row into a dataframe and add the 'Time' column
    # the first logic row will be used for the next quarter weights computation
    first_logic_row = pd.DataFrame(logic_one_arr, columns=crypto_asset)

    # computing the new second logic row that is used to compute the
    # weights relative to the next rebalance period
    ewma_fraction = daily_ewma_fraction(
        daily_ewma, first_logic_row, curr_reb_start, curr_board_eve
    )
    print("ewma_fraction")
    print(ewma_fraction)

    daily_ewma_double_row = index_norm_logic_op(crypto_asset, daily_ewma)

    print(daily_ewma_double_row)

    second_logic_row = (ewma_fraction >= 0.02) * 1

    double_checked_ewma = daily_double_log_check(
        first_logic_row, second_logic_row, daily_ewma,
        curr_reb_start, curr_board_eve)

    # adding the Time columns to the double checked ewma
    human_curr_start = timestamp_to_human(
        [curr_reb_start], date_format="%m-%d-%y")
    human_curr_board = timestamp_to_human(
        [curr_board_eve], date_format="%m-%d-%y")
    period_date_list = date_gen(
        human_curr_start[0], human_curr_board[0], EoD="N")
    double_checked_ewma["Time"] = period_date_list

    print(double_checked_ewma)
    # giving "Time" and "Date" columns to the df containing the logic rows
    human_next_start = timestamp_to_human(
        [next_reb_start], date_format="%m-%d-%y")
    first_logic_row["Time"] = next_reb_start
    first_logic_row["Date"] = human_next_start
    second_logic_row["Time"] = next_reb_start
    second_logic_row["Date"] = human_next_start

    # computing the new weights that will be used starting from the
    # next rebalance date
    weights_for_board = quarter_weights(
        double_checked_ewma, [int(curr_board_eve)], crypto_asset
    )
    weights_for_board["Time"] = next_reb_start
    weights_for_board["Date"] = human_next_start

    return first_logic_row, second_logic_row, weights_for_board, daily_ewma_double_row


def index_norm_logic_op(crypto_asset, daily_ewma):

    past_start_quarter_list = start_q()
    last_start_q = past_start_quarter_list[len(past_start_quarter_list) - 1]
    # downloading from mongoDB the current logic matrices (1 e 2)
    logic_one = query_mongo(DB_NAME, MONGO_DICT.get("coll_log1"))
    # taking only the logic value referred to the current period
    # current_logic_one = logic_one.iloc[[len(logic_one["Date"]) - 2]]
    current_logic_one = logic_one.iloc[logic_one.Time == int(last_start_q)]
    current_logic_one = current_logic_one.drop(columns=["Date", "Time"])
    logic_two = query_mongo(DB_NAME, MONGO_DICT.get("coll_log2"))
    # taking only the logic value referred to the current period
    # current_logic_two = logic_two.iloc[[len(logic_two["Date"]) - 2]]
    current_logic_two = logic_two.iloc[logic_two.Time == int(last_start_q)]
    current_logic_two = current_logic_two.drop(columns=["Date", "Time"])

    # computing the ewma checked with both the first and second logic matrices
    daily_ewma_first_check = np.array(daily_ewma) * np.array(current_logic_one)
    daily_ewma_double_check = daily_ewma_first_check * \
        np.array(current_logic_two)
    daily_ewma_double_check = pd.DataFrame(
        daily_ewma_double_check, columns=crypto_asset)

    return daily_ewma_double_check


def index_start_q_day(crypto_asset, exc_list, pair_list, coll_to_use, day=None):

    mongo_indexing()

    day_before_TS, _ = days_variable(day)

    # define all the useful arrays containing the rebalance start
    # date, stop date, board meeting date
    rebalance_start_date = start_q(START_DATE)
    rebalance_stop_date = stop_q(rebalance_start_date)
    board_date_eve = day_before_board()
    next_rebalance_date = next_start()

    # call the function that creates a object containing
    # the couple of quarterly start-stop date
    next_reb_start = str(int(next_rebalance_date[len(rebalance_start_date)]))
    curr_reb_start = str(
        int(rebalance_start_date[len(rebalance_start_date) - 1]))
    last_reb_start = str(
        int(rebalance_start_date[len(rebalance_start_date) - 2]))
    next_reb_stop = str(int(rebalance_stop_date[len(rebalance_stop_date) - 2]))
    curr_board_eve = str(int(board_date_eve[len(board_date_eve) - 1]))

    # defining the dictionary for the MongoDB query
    query_dict = {"Time": int(day_before_TS)}
    # retriving the needed information on MongoDB
    daily_mat = query_mongo(
        DB_NAME, MONGO_DICT.get(coll_to_use), query_dict)

    crypto_asset_price, crypto_asset_vol, exc_vol_tot, _ = index_daily_loop(
        daily_mat, crypto_asset, exc_list, pair_list, day_before_TS,
        last_reb_start, next_reb_stop, curr_board_eve)

    (crypto_asset_price, crypto_asset_vol, price_ret,
     daily_ewma, daily_ewma_double_check, daily_synth,
     daily_rel, curr_divisor, first_logic_row,
     second_logic_row, _,
     daily_index_1000_df, raw_index_df,
     new_divisor) = index_daily_operation(
        crypto_asset, crypto_asset_price,
        crypto_asset_vol, day_before_TS,
        curr_reb_start, next_reb_start,
        curr_board_eve,
        day_type="start_q", day=day)

    print(daily_ewma_double_check)
    index_daily_uploader(crypto_asset_price, crypto_asset_vol,
                         exc_vol_tot, price_ret, daily_ewma,
                         daily_ewma_double_check, daily_synth,
                         daily_rel, curr_divisor,
                         daily_index_1000_df, raw_index_df,
                         new_logic_one=first_logic_row,
                         new_logic_two=second_logic_row,
                         new_divisor=new_divisor, day=day)

    return None


def index_board_day(crypto_asset, exc_list, pair_list, coll_to_use, day=None):

    mongo_indexing()

    day_before_TS, _ = days_variable(day)

    # define all the useful arrays containing the rebalance start
    # date, stop date, board meeting date
    rebalance_start_date = start_q(START_DATE)
    rebalance_stop_date = stop_q(rebalance_start_date)
    board_date_eve = day_before_board()
    next_rebalance_date = next_start()

    # call the function that creates a object containing
    # the couple of quarterly start-stop date
    next_reb_start = str(int(next_rebalance_date[len(rebalance_start_date)]))
    curr_reb_start = str(
        int(rebalance_start_date[len(rebalance_start_date) - 1]))
    last_reb_start = str(
        int(rebalance_start_date[len(rebalance_start_date) - 2]))
    next_reb_stop = str(int(rebalance_stop_date[len(rebalance_stop_date) - 2]))
    curr_board_eve = str(int(board_date_eve[len(board_date_eve) - 1]))

    # defining the dictionary for the MongoDB query
    query_dict = {"Time": int(day_before_TS)}
    # retriving the needed information on MongoDB
    daily_mat = query_mongo(
        DB_NAME, MONGO_DICT.get(coll_to_use), query_dict)

    crypto_asset_price, crypto_asset_vol, exc_vol_tot, logic_one_arr = index_daily_loop(
        daily_mat, crypto_asset, exc_list, pair_list, day_before_TS,
        last_reb_start, next_reb_stop, curr_board_eve, day_type="board")

    (crypto_asset_price, crypto_asset_vol, price_ret,
     daily_ewma, daily_ewma_double_check, daily_synth,
     daily_rel, curr_divisor, first_logic_row,
     second_logic_row, weights_for_board,
     daily_index_1000_df, raw_index_df, _) = index_daily_operation(
        crypto_asset, crypto_asset_price,
        crypto_asset_vol, day_before_TS,
        curr_reb_start, next_reb_start,
        curr_board_eve, logic_one_arr=logic_one_arr,
        day_type="board", day=day)

    print(daily_ewma_double_check)
    index_daily_uploader(crypto_asset_price, crypto_asset_vol,
                         exc_vol_tot, price_ret, daily_ewma,
                         daily_ewma_double_check, daily_synth,
                         daily_rel, curr_divisor,
                         daily_index_1000_df, raw_index_df,
                         new_logic_one=first_logic_row,
                         new_logic_two=second_logic_row,
                         new_weights=weights_for_board, day=day)

    return None


def index_normal_day(crypto_asset, exc_list, pair_list, coll_to_use, day=None):

    # create the indexing for MongoDB
    mongo_indexing()

    day_before_TS, two_before_TS = days_variable(day)
    two_before_human = timestamp_to_human([two_before_TS])

    # if day is None:

    #     day_str = datetime.now().strftime("%Y-%m-%d")
    #     day_date = datetime.strptime(day_str, "%Y-%m-%d")
    #     day_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
    #     day_before_TS = day_TS - DAY_IN_SEC
    #     two_before_TS = day_before_TS - DAY_IN_SEC
    #     two_before_human = timestamp_to_human([two_before_TS])

    # else:

    #     day_date = datetime.strptime(day, "%Y-%m-%d")
    #     day_before_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
    #     two_before_TS = day_before_TS - DAY_IN_SEC
    #     two_before_human = timestamp_to_human([two_before_TS])

    # defining the dictionary for the MongoDB query
    query_dict = {"Time": int(day_before_TS)}
    # retriving the needed information on MongoDB
    daily_mat = query_mongo(
        DB_NAME, MONGO_DICT.get(coll_to_use), query_dict)

    (crypto_asset_price, crypto_asset_vol, exc_vol_tot, _) = index_daily_loop(
        daily_mat, crypto_asset, exc_list, pair_list, day_before_TS)

    # turn prices and volumes into pandas dataframe
    crypto_asset_price = pd.DataFrame(crypto_asset_price, columns=CRYPTO_ASSET)
    crypto_asset_vol = pd.DataFrame(crypto_asset_vol, columns=CRYPTO_ASSET)

    # compute the price return of the day
    two_before_price = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_price"), {"Time": two_before_TS})
    two_before_price = two_before_price.drop(columns=["Time", "Date"])
    return_df = two_before_price.append(crypto_asset_price)
    price_ret = return_df.pct_change()
    price_ret = price_ret.iloc[[1]]
    print(price_ret)
    price_ret = price_ret.replace(-1, 0)
    print(price_ret)
    price_ret.fillna(0, inplace=True)
    print(price_ret)
    # then add the 'Time' column
    time_header = ["Time"]
    time_header.extend(CRYPTO_ASSET)
    crypto_asset_price = pd.DataFrame(crypto_asset_price, columns=time_header)
    crypto_asset_price["Time"] = int(day_before_TS)
    crypto_asset_vol = pd.DataFrame(crypto_asset_vol, columns=time_header)
    crypto_asset_vol["Time"] = int(day_before_TS)
    # adding the Time column to the price ret df
    price_ret["Time"] = int(day_before_TS)

    # computing the Exponential Weighted Moving Average of the day
    hist_volume = query_mongo(DB_NAME, MONGO_DICT.get("coll_volume"))
    hist_volume = hist_volume.drop(columns=["Date"])
    hist_volume = hist_volume.append(crypto_asset_vol)
    daily_ewma = daily_ewma_crypto_volume(hist_volume, CRYPTO_ASSET)

    # downloading from mongoDB the current logic matrices (1 e 2)
    logic_one = query_mongo(DB_NAME, MONGO_DICT.get("coll_log1"))
    # taking only the logic value referred to the current period
    current_logic_one = logic_one.iloc[[len(logic_one["Date"]) - 2]]
    current_logic_one = current_logic_one.drop(columns=["Date", "Time"])
    logic_two = query_mongo(DB_NAME, MONGO_DICT.get("coll_log2"))
    # taking only the logic value referred to the current period
    current_logic_two = logic_two.iloc[[len(logic_two["Date"]) - 2]]
    current_logic_two = current_logic_two.drop(columns=["Date", "Time"])

    # computing the ewma checked with both the first and second logic matrices
    daily_ewma_first_check = np.array(daily_ewma) * np.array(current_logic_one)
    daily_ewma_double_check = daily_ewma_first_check * \
        np.array(current_logic_two)
    daily_ewma_double_check = pd.DataFrame(
        daily_ewma_double_check, columns=CRYPTO_ASSET)

    # compute the daily syntethic matrix
    yesterday_synt_matrix = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_synt"), {"Date": two_before_human[0]}
    )
    yesterday_synt_matrix = yesterday_synt_matrix.drop(columns=["Date", "Time"])
    daily_return = np.array(price_ret.loc[:, CRYPTO_ASSET])
    new_synt = (1 + daily_return) * np.array(yesterday_synt_matrix)
    daily_synth = pd.DataFrame(new_synt, columns=CRYPTO_ASSET)

    # compute the daily relative syntethic matrix
    daily_tot = np.array(daily_synth.sum(axis=1))

    daily_rel = np.array(daily_synth) / daily_tot
    daily_rel = pd.DataFrame(daily_rel, columns=CRYPTO_ASSET)

    # daily index value computation
    curr_divisor = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_divisor_res"), {
            "Date": two_before_human[0]}
    )
    curr_div_val = np.array(curr_divisor["Divisor Value"])
    index_numerator = np.array(
        crypto_asset_price[CRYPTO_ASSET]) * np.array(daily_rel)
    numerator_sum = index_numerator.sum(axis=1)
    num = pd.DataFrame(numerator_sum)
    daily_index_value = np.array(num) / curr_div_val
    raw_index_df = pd.DataFrame(daily_index_value, columns=["Index Value"])

    # retrieving from mongoDB the yesterday value of the raw index
    yesterday_raw_index = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_raw_index"), {"Date": two_before_human[0]}
    )
    yesterday_raw_index = yesterday_raw_index.drop(columns=["Date", "Time"])
    raw_curr = yesterday_raw_index.append(raw_index_df)
    variation = raw_curr.pct_change()
    variation = np.array(variation.iloc[1])

    # retrieving from mongoDB the yesterday value of the raw index
    yesterday_1000_index = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_1000_index"), {
            "Date": two_before_human[0]}
    )
    daily_index_1000 = np.array(
        yesterday_1000_index["Index Value"]) * (1 + variation)
    daily_index_1000_df = pd.DataFrame(
        daily_index_1000, columns=["Index Value"])

    index_daily_uploader(crypto_asset_price, crypto_asset_vol,
                         exc_vol_tot, price_ret, daily_ewma,
                         daily_ewma_double_check, daily_synth,
                         daily_rel, curr_divisor,
                         daily_index_1000_df, raw_index_df, day=day)

    return None


def index_daily_uploader(crypto_asset_price, crypto_asset_vol,
                         exc_vol_tot, price_ret, daily_ewma,
                         daily_ewma_double_check, daily_synth,
                         daily_rel, curr_divisor,
                         daily_index_1000_df, raw_index_df,
                         new_divisor=None, new_logic_one=None,
                         new_logic_two=None, new_weights=None,
                         day=None):
    if day is None:

        day_str = datetime.now().strftime("%Y-%m-%d")
        day_date = datetime.strptime(day_str, "%Y-%m-%d")
        day_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
        day_before_TS = day_TS - DAY_IN_SEC
        yesterday_human = timestamp_to_human([day_before_TS])

    else:

        day_date = datetime.strptime(day, "%Y-%m-%d")
        day_before_TS = int(day_date.replace(tzinfo=timezone.utc).timestamp())
        yesterday_human = timestamp_to_human([day_before_TS])

    # put the "crypto_asset_price" dataframe on MongoDB
    crypto_asset_price["Date"] = yesterday_human
    mongo_upload(crypto_asset_price, "collection_price",
                 reorder="Y", column_set_val="complete")

    # put the "crypto_asset_vols" dataframe on MongoDB
    crypto_asset_vol["Date"] = yesterday_human
    mongo_upload(crypto_asset_vol, "collection_volume",
                 reorder="Y", column_set_val="complete")

    # put the exchange volumes on MongoDB
    mongo_upload(exc_vol_tot, "collection_all_exc_vol")

    # put the "price_ret" dataframe on MongoDB
    price_ret["Date"] = yesterday_human
    mongo_upload(price_ret, "collection_price_ret",
                 reorder="Y", column_set_val="complete")

    # put the EWMA dataframe on MongoDB
    daily_ewma["Date"] = yesterday_human
    daily_ewma["Time"] = day_before_TS
    mongo_upload(daily_ewma, "collection_EWMA",
                 reorder="Y", column_set_val="complete")

    # put the double checked EWMA on MongoDB
    daily_ewma_double_check["Date"] = yesterday_human
    daily_ewma_double_check["Time"] = day_before_TS
    mongo_upload(daily_ewma_double_check, "collection_EWMA_check",
                 reorder="Y", column_set_val="complete")

    # put the synth matrix on MongoDB
    daily_synth["Date"] = yesterday_human
    daily_synth["Time"] = day_before_TS
    mongo_upload(daily_synth, "collection_synth",
                 reorder="Y", column_set_val="complete")

    # put the relative synth matrix on MongoDB
    daily_rel["Date"] = yesterday_human
    daily_rel["Time"] = day_before_TS
    mongo_upload(daily_rel, "collection_relative_synth",
                 reorder="Y", column_set_val="complete")

    # put the reshaped divisor array on MongoDB
    curr_divisor["Date"] = yesterday_human
    curr_divisor["Time"] = day_before_TS
    mongo_upload(curr_divisor, "collection_divisor_reshaped",
                 reorder="Y", column_set_val="divisor")

    # put the index level 1000 on MongoDB
    daily_index_1000_df["Date"] = yesterday_human
    daily_index_1000_df["Time"] = day_before_TS
    mongo_upload(daily_index_1000_df, "collection_index_level_1000",
                 reorder="Y", column_set_val="index")

    # put the index level raw on MongoDB
    raw_index_df["Date"] = yesterday_human
    raw_index_df["Time"] = day_before_TS
    mongo_upload(raw_index_df, "collection_index_level_raw",
                 reorder="Y", column_set_val="index")

    if new_divisor is None:
        pass

    else:

        new_divisor["Date"] = yesterday_human
        new_divisor["Time"] = day_before_TS
        mongo_upload(new_divisor, "collection_divisor_reshaped",
                     reorder="Y", column_set_val="divisor")

    if new_logic_one is None:
        pass

    else:

        mongo_upload(new_logic_one, "collection_logic_one",
                     reorder="Y", column_set_val="complete")

    if new_logic_two is None:
        pass

    else:

        mongo_upload(new_logic_two, "collection_logic_one",
                     reorder="Y", column_set_val="complete")

    if new_weights is None:
        pass

    else:

        mongo_upload(new_weights, "collection_weights",
                     reorder="Y", column_set_val="complete")

    return None


def index_daily(coll_to_use="coll_data_feed", day=None):

    start_q_list = next_start()
    board_eve_list = day_before_board()

    day_TS = days_variable(day)

    if day_TS in start_q_list:

        print("First day of the quarter")
        index_start_q_day(CRYPTO_ASSET, EXCHANGES, PAIR_ARRAY,
                          coll_to_use, day=day)

    elif day_TS in board_eve_list:

        print("Board day")
        index_board_day(CRYPTO_ASSET, EXCHANGES, PAIR_ARRAY,
                        coll_to_use, day=day)

    else:

        print("Normal day")
        index_normal_day(CRYPTO_ASSET, EXCHANGES, PAIR_ARRAY,
                         coll_to_use, day=day)


# ##############################################àà
# HISTORICAL INDEX
# ################################################
