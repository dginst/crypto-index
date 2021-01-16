# standard library import
from datetime import datetime, timezone

# third party import
import numpy as np
import pandas as pd

# local import

from cryptoindex.calc import (start_q, stop_q, board_meeting_day,
                              day_before_board, next_start,
                              quarterly_period, next_quarterly_period,
                              first_logic_matrix, second_logic_matrix,
                              ewma_crypto_volume, divisor_adjustment,
                              ewma_second_logic_check, quarter_weights,
                              relative_syntethic_matrix, quarterly_synt_matrix,
                              divisor_reshape, index_based, index_level_calc
                              )
from cryptoindex.data_setup import (date_gen, timestamp_to_human)
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload, query_mongo)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME, DAY_IN_SEC)

from cryptoindex.index_func import (
    loop_crypto_fiat, loop_single_exc, loop_exc_value, crypto_fiat_gen,
    loop_crypto_asset, exc_all_vol)


def loop_crypto_fiat_hist(ccy_fiat_vol, ccy_fiat_price_vol, ref_date_arr):

    # first if condition: the crypto-fiat is in more than 1 exchange
    if ccy_fiat_vol.size != 0 and ccy_fiat_vol.size > ref_date_arr.size:

        price_vol_sum = ccy_fiat_price_vol.sum(axis=1)
        vol_sum = ccy_fiat_vol.sum(axis=1)
        ccy_fiat_price = np.divide(
            price_vol_sum, vol_sum, out=np.zeros_like(vol_sum), where=vol_sum != 0.0)

        # computing the total volume of the exchange
        ccy_fiat_vol = ccy_fiat_vol.sum(axis=1)
        # computing price X volume of the exchange
        ccy_fiat_price_vol = ccy_fiat_price * ccy_fiat_vol

    # second if condition: the crypto-fiat is traded in just one exchange
    elif ccy_fiat_vol.size != 0 and ccy_fiat_vol.size == ref_date_arr.size:

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


def loop_single_exc_hist(exc_price, exc_vol, exc_price_vol,
                         ccy_fiat_price, ccy_fiat_vol,
                         ccy_fiat_price_vol, ref_date_arr):

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

            exc_price = np.zeros(ref_date_arr.size)
            exc_vol = np.zeros(ref_date_arr.size)
            exc_price_vol = np.zeros(ref_date_arr.size)

    else:

        if ccy_fiat_vol.size != 0:

            exc_price = np.column_stack(
                (exc_price, ccy_fiat_price))
            exc_vol = np.column_stack(
                (exc_vol, ccy_fiat_vol))
            exc_price_vol = np.column_stack((exc_price_vol, ccy_fiat_price_vol))

        else:

            exc_price = np.column_stack(
                (exc_price, np.zeros(ref_date_arr.size)))
            exc_vol = np.column_stack(
                (exc_vol, np.zeros(ref_date_arr.size)))
            exc_price_vol = np.column_stack(
                (exc_price_vol, np.zeros(ref_date_arr.size)))

    return exc_price, exc_vol, exc_price_vol


def loop_exc_value_hist(single_crypto, exc_price, exc_vol,
                        exc_price_vol, exc_vol_tot_old,
                        exc_list, logic_matrix_one,
                        ref_date_arr):

    # dataframes that contain volume and price of a single crytpo
    # for all the exchanges. If an exchange does not have value
    # in the crypto will be insertd a column with zero
    exc_vol_df = pd.DataFrame(exc_vol, columns=EXCHANGES)
    exc_price_df = pd.DataFrame(exc_price, columns=EXCHANGES)

    # adding "Time" column to both Exchanges dataframe
    exc_vol_df["Time"] = ref_date_arr
    exc_price_df["Time"] = ref_date_arr
    exc_vol_tot_new = exc_all_vol(single_crypto, exc_vol_df, exc_vol_tot_old)

    # for each CryptoAsset compute the first logic array
    first_logic_array = first_logic_matrix(exc_vol_df, EXCHANGES)

    # put the logic array into the logic matrix
    if logic_matrix_one.size == 0:
        logic_matrix_one = first_logic_array
    else:
        logic_matrix_one = np.column_stack(
            (logic_matrix_one, first_logic_array))

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

    return exc_price, exc_vol, exc_vol_tot_new, logic_matrix_one


def index_hist_loop(data_matrix, crypto_asset, exc_list,
                    pair_list):

    ref_date_arr = date_gen()

    # initialize the matrices that will contain the prices
    # and volumes of all the single_cryptosset
    crypto_asset_price = np.matrix([])
    crypto_asset_vol = np.matrix([])

    logic_matrix_one = np.matrix([])

    # initialize the matrix that contain the volumes per Exchange
    exc_head = [
        "coinbase-pro",
        "poloniex",
        "bitstamp",
        "gemini",
        "bittrex",
        "kraken",
        "bitflyer",
    ]
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
            print(exchange)
            # initialize the matrices that will contain the data related
            # to all currencypair for the single exchange
            ccy_fiat_price_vol = np.matrix([])
            ccy_fiat_vol = np.matrix([])
            ccy_fiat_price = np.matrix([])

            for cp in crypto_fiat_arr:
                print(cp)
                # selecting the data referring to specific exchange and crypto-fiat pair
                matrix = data_matrix.loc[
                    (data_matrix["Exchange"] == exchange) & (
                        data_matrix["Pair"] == cp)
                ]
                print(matrix.shape)
                if matrix.empty is False:

                    price = np.array((matrix["Close Price"]))
                    volume = np.array((matrix["Pair Volume"]))
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

            ccy_fiat_price, ccy_fiat_vol, ccy_fiat_price_vol = loop_crypto_fiat_hist(
                ccy_fiat_vol, ccy_fiat_price_vol, ref_date_arr)

            exc_price, exc_vol, exc_price_vol = loop_single_exc_hist(
                exc_price, exc_vol, exc_price_vol, ccy_fiat_price,
                ccy_fiat_vol, ccy_fiat_price_vol, ref_date_arr)

        (exc_price,
         exc_vol,
         exc_vol_tot,
         logic_matrix_one) = loop_exc_value_hist(single_crypto, exc_price, exc_vol,
                                                 exc_price_vol, exc_vol_tot,
                                                 exc_list, logic_matrix_one,
                                                 ref_date_arr)

        # creating every loop the matrices of all the single_cryptossets
        # Crypto_Asset_Price contains the prices of all the cryptocurrencies
        # crypto_asset_vol contains the volume of all the cryptocurrencies
        crypto_asset_price, crypto_asset_vol = loop_crypto_asset(
            exc_price, exc_vol, crypto_asset_price, crypto_asset_vol)

    return crypto_asset_price, crypto_asset_vol, exc_vol_tot, logic_matrix_one


def hist_time_array_set(start_date=START_DATE):

    # define the variable containing all the date from start_date to yesterday.
    # the date are displayed as timestamp and each day refers to 12:00 am UTC
    reference_date_arr = date_gen(start_date)

    # define all the useful arrays containing the rebalance
    # start date, stop date, board meeting date
    reb_start_date = start_q(start_date)
    reb_stop_date = stop_q(reb_start_date)
    board_date = board_meeting_day()
    board_date_eve = day_before_board()
    next_reb_date = next_start()

    return (reference_date_arr, reb_start_date, reb_stop_date,
            board_date, board_date_eve, next_reb_date)


def hist_time_single_date(start_date=START_DATE):

    (_, reb_start_date, reb_stop_date, _,
     board_date_eve, _) = hist_time_array_set(start_date)

    last_reb_start = str(int(reb_start_date[len(reb_start_date) - 1]))
    next_reb_stop = str(int(reb_stop_date[len(reb_stop_date) - 1]))
    last_reb_stop = str(int(reb_stop_date[len(reb_stop_date) - 2]))
    curr_board_eve = str(int(board_date_eve[len(board_date_eve) - 1]))

    return (last_reb_start, last_reb_stop, next_reb_stop, curr_board_eve)


def index_hist_uploader(crypto_asset_price, crypto_asset_vol, exc_vol_tot,
                        price_ret, weights_for_board, first_logic_matrix_df,
                        second_logic_matrix_df, ewma_df, double_checked_EWMA,
                        syntethic, syntethic_relative_matrix, divisor_array,
                        reshaped_divisor, index_values, index_1000_base
                        ):

    # creating the array with human readable Date
    ref_date_arr = date_gen()
    human_date = timestamp_to_human(ref_date_arr)

    # put the "Crypto_Asset_Prices" dataframe on MongoDB
    crypto_asset_price["Date"] = human_date
    mongo_upload(crypto_asset_price, "collection_price",
                 reorder="Y", column_set_val="complete")

    # put the "Crypto_Asset_Volumes" dataframe on MongoDB
    crypto_asset_vol["Date"] = human_date
    mongo_upload(crypto_asset_vol, "collection_volume",
                 reorder="Y", column_set_val="complete")

    # put the exchange volumes on MongoDB
    mongo_upload(exc_vol_tot, "collection_all_exc_vol")

    # put the "price_ret" dataframe on MongoDB
    price_ret["Date"] = human_date
    mongo_upload(price_ret, "collection_price_ret",
                 reorder="Y", column_set_val="complete")

    # put the "weights" dataframe on MongoDB
    weight_human_date = timestamp_to_human(weights_for_board["Time"])
    weights_for_board["Date"] = weight_human_date
    mongo_upload(weights_for_board, "collection_weights",
                 reorder="Y", column_set_val="complete")

    # put the first logic matrix on MongoDB
    first_date = timestamp_to_human(first_logic_matrix_df["Time"])
    first_logic_matrix_df["Date"] = first_date
    mongo_upload(first_logic_matrix_df, "collection_logic_one",
                 reorder="Y", column_set_val="complete")

    # put the second logic matrix on MongoDB
    second_date = timestamp_to_human(second_logic_matrix_df["Time"])
    second_logic_matrix_df["Date"] = second_date
    mongo_upload(second_logic_matrix_df, "collection_logic_two",
                 reorder="Y", column_set_val="complete")

    # put the EWMA dataframe on MongoDB
    ewma_df["Date"] = human_date
    ewma_df["Time"] = ref_date_arr
    mongo_upload(ewma_df, "collection_EWMA",
                 reorder="Y", column_set_val="complete")

    # put the double checked EWMA on MongoDB
    double_checked_EWMA["Date"] = human_date
    mongo_upload(double_checked_EWMA, "collection_EWMA_check",
                 reorder="Y", column_set_val="complete")

    # put the synth matrix on MongoDB
    syntethic["Date"] = human_date
    syntethic["Time"] = ref_date_arr
    mongo_upload(syntethic, "collection_synth",
                 reorder="Y", column_set_val="complete")

    # put the relative synth matrix on MongoDB
    syntethic_relative_matrix["Date"] = human_date
    syntethic_relative_matrix["Time"] = ref_date_arr
    mongo_upload(syntethic_relative_matrix, "collection_relative_synth",
                 reorder="Y", column_set_val="complete")

    # put the divisor array on MongoDB
    divisor_date = timestamp_to_human(divisor_array["Time"])
    divisor_array["Date"] = divisor_date
    mongo_upload(divisor_array, "collection_divisor",
                 reorder="Y", column_set_val="divisor")

    # put the reshaped divisor array on MongoDB
    reshaped_divisor_date = timestamp_to_human(reshaped_divisor["Time"])
    reshaped_divisor["Date"] = reshaped_divisor_date
    mongo_upload(reshaped_divisor, "collection_divisor_reshaped",
                 reorder="Y", column_set_val="divisor")

    # put the index level raw on MongoDB
    index_values["Date"] = human_date
    index_values["Time"] = ref_date_arr
    mongo_upload(index_values, "collection_index_level_raw",
                 reorder="Y", column_set_val="index")

    # put the index level 1000 on MongoDB
    index_1000_base["Date"] = human_date
    index_1000_base["Time"] = ref_date_arr
    mongo_upload(index_1000_base, "collection_index_level_1000",
                 reorder="Y", column_set_val="index")

    return None


def index_hist_op(crypto_asset_price_arr,
                  crypto_asset_vol_arr,
                  logic_matrix_one,
                  ):

    (ref_date_arr, reb_start_date, _,
     _, board_date_eve, next_reb_date) = hist_time_array_set()

    (_, last_reb_stop, _,
     curr_board_eve) = hist_time_single_date()

    reference_day_TS = ref_day_TS()

    # turn prices and volumes into pandas dataframe
    crypto_asset_price = pd.DataFrame(
        crypto_asset_price_arr, columns=CRYPTO_ASSET)
    crypto_asset_vol = pd.DataFrame(
        crypto_asset_vol_arr, columns=CRYPTO_ASSET)

    # compute the price returns over the defined period
    price_ret = crypto_asset_price.pct_change()
    price_ret.fillna(0, inplace=True)

    # add the 'Time' column
    crypto_asset_price["Time"] = ref_date_arr
    crypto_asset_vol["Time"] = ref_date_arr
    price_ret["Time"] = ref_date_arr

    # turn the first logic matrix into a dataframe and add the 'Time' column
    # containg the stop_date of each quarter as in "rebalance_stop_date"
    # array the 'Time' column does not take into account the last value
    # because it refers to a period that has not been yet calculated
    # (and will be this way until today == new quarter start_date)
    first_logic_matrix_df = pd.DataFrame(logic_matrix_one, columns=CRYPTO_ASSET)

    first_logic_matrix_df["Time"] = next_reb_date[1: len(
        next_reb_date)]

    # computing the Exponential Moving Weighted Average of the selected period
    ewma_df = ewma_crypto_volume(
        crypto_asset_vol, CRYPTO_ASSET, ref_date_arr, time_column="N"
    )

    # computing the second logic matrix
    second_logic_matrix_df = second_logic_matrix(
        crypto_asset_vol,
        first_logic_matrix_df,
        CRYPTO_ASSET,
        ref_date_arr,
        time_column="Y",
    )

    # computing the ewma checked with both the first and second logic matrices
    double_checked_EWMA = ewma_second_logic_check(
        first_logic_matrix_df,
        second_logic_matrix_df,
        ewma_df,
        ref_date_arr,
        CRYPTO_ASSET,
        time_column="Y"
    )

    # computing the Weights that each CryptoAsset should have
    # starting from each new quarter every weigfhts is computed
    # in the period that goes from present quarter start_date to
    # present quarter board meeting date eve
    weights_for_board = quarter_weights(
        double_checked_EWMA, board_date_eve[1:], CRYPTO_ASSET
    )

    # compute the syntethic matrix and the relative syntethic matrix
    syntethic = quarterly_synt_matrix(
        crypto_asset_price,
        weights_for_board,
        ref_date_arr,
        board_date_eve,
        CRYPTO_ASSET,
    )
    syntethic.fillna(0, inplace=True)

    syntethic_relative_matrix = relative_syntethic_matrix(
        syntethic, CRYPTO_ASSET)

    # changing the "Time" column of the second logic matrix
    # using the rebalance date
    second_logic_matrix_df["Time"] = next_reb_date[1: len(
        next_reb_date)]

    if reference_day_TS == reb_start_date[len(reb_start_date) - 1]:

        second_logic_matrix_df = second_logic_matrix_df[:-1]

    # changing the "Time" column of the weights in order to
    # display the quarter start date of each row
    weights_for_period = weights_for_board

    if reference_day_TS >= int(curr_board_eve) and reference_day_TS <= int(last_reb_stop):

        weights_for_period['Time'] = next_reb_date[1:]

    else:

        weights_for_period["Time"] = reb_start_date[1:]

    divisor_array = divisor_adjustment(
        crypto_asset_price,
        weights_for_period,
        second_logic_matrix_df,
        CRYPTO_ASSET,
        ref_date_arr,
    )

    reshaped_divisor = divisor_reshape(divisor_array, ref_date_arr)

    index_values = index_level_calc(
        crypto_asset_price, syntethic_relative_matrix, divisor_array, ref_date_arr
    )

    index_1000_base = index_based(index_values)

    return (crypto_asset_price, crypto_asset_vol,
            price_ret, weights_for_board, first_logic_matrix_df,
            second_logic_matrix_df, ewma_df, double_checked_EWMA,
            syntethic, syntethic_relative_matrix, divisor_array,
            reshaped_divisor, index_values, index_1000_base)


def index_hist_total(coll_to_use="coll_data_feed", crypto_asset=CRYPTO_ASSET,
                     exc_list=EXCHANGES, pair_list=PAIR_ARRAY):

    # drop the pre-existing collections
    mongo_coll_drop("index_hist")

    # define the mongo indexing
    mongo_indexing()

    data_df = query_mongo(DB_NAME, MONGO_DICT.get(coll_to_use))

    (crypto_asset_price_arr, crypto_asset_vol_arr,
     exc_vol_tot, logic_matrix_one) = index_hist_loop(data_df, crypto_asset, exc_list,
                                                      pair_list)

    (crypto_asset_price, crypto_asset_vol,
     price_ret, weights_for_board, first_logic_matrix_df,
     second_logic_matrix_df, ewma_df, double_checked_EWMA,
     syntethic, syntethic_relative_matrix, divisor_array,
     reshaped_divisor, index_values, index_1000_base) = index_hist_op(crypto_asset_price_arr,
                                                                      crypto_asset_vol_arr,
                                                                      logic_matrix_one
                                                                      )

    index_hist_uploader(crypto_asset_price, crypto_asset_vol, exc_vol_tot,
                        price_ret, weights_for_board, first_logic_matrix_df,
                        second_logic_matrix_df, ewma_df, double_checked_EWMA,
                        syntethic, syntethic_relative_matrix, divisor_array,
                        reshaped_divisor, index_values, index_1000_base
                        )

    return None


def ref_day_TS(lag="Y"):

    # define today and yesterady date as timestamp
    today_str = datetime.now().strftime("%Y-%m-%d")
    today = datetime.strptime(today_str, "%Y-%m-%d")
    today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
    if lag == "Y":

        ref_TS = today_TS - DAY_IN_SEC

    else:

        ref_TS = today_TS

    return ref_TS
