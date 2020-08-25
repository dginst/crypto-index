# standard library import
from datetime import datetime, timezone

# third party import
import numpy as np
import pandas as pd

# local import

from cryptoindex.calc import (start_q, stop_q, board_meeting_day,
                              day_before_board, next_start,
                              quarterly_period,
                              daily_ewma_crypto_volume
                              )
from cryptoindex.data_setup import (date_gen, timestamp_to_human)
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing, mongo_upload, query_mongo)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME, DAY_IN_SEC)

# ################# setup mongo connection ################

# create the indexing for MongoDB and define the variable containing the
# MongoDB collections where to upload data
mongo_indexing()
collection_dict_upload = mongo_coll()

# ################ DATE SETTINGS ########################

# define today in various format
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC
two_before_TS = y_TS - DAY_IN_SEC
today_human = timestamp_to_human([today_TS])
yesterday_human = timestamp_to_human([y_TS])
two_before_human = timestamp_to_human([two_before_TS])

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = date_gen(START_DATE)

# define all the useful arrays containing the rebalance start
# date, stop date, board meeting date
rebalance_start_date = start_q(START_DATE)
rebalance_stop_date = stop_q(rebalance_start_date)
board_date = board_meeting_day()
board_date_eve = day_before_board()
next_rebalance_date = next_start()

# call the function that creates a object containing
# the couple of quarterly start-stop date
quarterly_date = quarterly_period()

# ############# MAIN PART #########################

# downloading the daily value from MongoDB and put it into a dataframe

# defining the dictionary for the MongoDB query
query_dict = {"Time": str(y_TS)}
# retriving the needed information on MongoDB
daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_final"), query_dict)
print(daily_mat)

# initialize the matrices that will contain the prices
# and volumes of all the cryptoasset
Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])
# initialize the matrix that will contain the complete first logic matrix
logic_matrix_one = np.matrix([])

# initialize the matrix that contain the volumes per Exchange
exc_head = EXCHANGES
exc_head.append("Time")
exc_head.append("Crypto")
exc_vol_tot = pd.DataFrame(columns=exc_head)

for CryptoA in CRYPTO_ASSET:

    print(CryptoA)
    # initialize useful matrices
    ccy_pair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])

    # create the crypto-fiat strings useful to download from CW
    for pair in PAIR_ARRAY:
        ccy_pair_array.append(CryptoA.lower() + pair)

    for exchange in EXCHANGES:
        print(exchange)
        # initialize the matrices that will contain the data related
        # to all currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])

        for cp in ccy_pair_array:
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

            # selecting the data referring to specific
            # exchange and crypto-fiat pair
            matrix = daily_mat.loc[
                (daily_mat["Exchange"] == exchange) & (daily_mat["Pair"] == cp)
            ]

            if matrix.empty is False:

                price = np.array(float(matrix["Close Price"]))
                volume = np.array(float(matrix["Pair Volume"]))
                priceXvolume = np.array(price * volume)

                # every "cp" the loop adds a column in the matrices referred
                # to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack(
                        (Ccy_Pair_PriceVolume, priceXvolume)
                    )
                    Ccy_Pair_Volume = np.column_stack((Ccy_Pair_Volume, volume))

            else:
                pass

        # computing the volume weighted average price of the single exchange

        # first if condition: the crypto-fiat is in more than 1 exchange
        if Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size > 1:

            PxV = Ccy_Pair_PriceVolume.sum(axis=1)
            V = Ccy_Pair_Volume.sum(axis=1)
            Ccy_Pair_Price = np.divide(
                PxV, V, out=np.zeros_like(V), where=V != 0.0)

            # computing the total volume of the exchange
            Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis=1)
            # computing price X volume of the exchange
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # second if condition: the crypto-fiat is traded in just one exchange
        elif Ccy_Pair_Volume.size != 0 and Ccy_Pair_Volume.size == 1:

            np.seterr(all=None, divide="warn")
            Ccy_Pair_Price = np.divide(
                Ccy_Pair_PriceVolume,
                Ccy_Pair_Volume,
                out=np.zeros_like(Ccy_Pair_Volume),
                where=Ccy_Pair_Volume != 0.0,
            )
            Ccy_Pair_Price = np.nan_to_num(Ccy_Pair_Price)
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # third if condition: the crypto-fiat is not traded at all
        else:

            Ccy_Pair_Price = np.array([])
            Ccy_Pair_Volume = np.array([])
            Ccy_Pair_PxV = np.array([])

        # creating every loop the matrices containing the data referred to all
        # the exchanges Exchange_Price contains the crypto ("cp") prices in all
        # the different Exchanges, Exchange_Volume contains the crypto ("cp")
        # volume in all the different Exchanges if no values is found,
        # script put "0" instead
        if Exchange_Price.size == 0:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = Ccy_Pair_Price
                Exchange_Volume = Ccy_Pair_Volume
                Ex_PriceVol = Ccy_Pair_PxV

            else:

                Exchange_Price = np.zeros(1)
                Exchange_Volume = np.zeros(1)
                Ex_PriceVol = np.zeros(1)

        else:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = np.column_stack(
                    (Exchange_Price, Ccy_Pair_Price))
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, Ccy_Pair_Volume))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, Ccy_Pair_PxV))

            else:

                Exchange_Price = np.column_stack((Exchange_Price, np.zeros(1)))
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, np.zeros(1)))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, np.zeros(1)))

    # dataframes that contain volume and price of a single crytpo
    # for all the exchanges. If an exchange does not have value
    # in the crypto will be insertd a column with zero
    Exchange_Vol_DF = pd.DataFrame(Exchange_Volume, columns=EXCHANGES)
    Exchange_Price_DF = pd.DataFrame(Exchange_Price, columns=EXCHANGES)

    # adding "Time" column to both Exchanges dataframe
    Exchange_Vol_DF["Time"] = str(today_TS)
    Exchange_Price_DF["Time"] = str(today_TS)

    exc_vol_p = Exchange_Vol_DF
    exc_vol_p["Crypto"] = CryptoA
    exc_vol_tot = exc_vol_tot.append(exc_vol_p)

    try:

        # computing the volume weighted average price of the single
        # Crypto_Asset ("CryptoA") into a single vector
        Ex_price_num = Ex_PriceVol.sum(axis=1)
        Ex_price_den = Exchange_Volume.sum(axis=1)
        Exchange_Price = np.divide(
            Ex_price_num,
            Ex_price_den,
            out=np.zeros_like(Ex_price_num),
            where=Ex_price_num != 0.0,
        )
        # computing the total volume  average price of the
        # single Crypto_Asset ("CryptoA") into a single vector
        Exchange_Volume = Exchange_Volume.sum(axis=1)

    except np.AxisError:

        Exchange_Price = Exchange_Price
        Exchange_Volume = Exchange_Volume

    # creating every loop the matrices of all the Cryptoassets
    # Crypto_Asset_Price contains the prices of all the cryptocurrencies
    # Crypto_Asset_Volume contains the volume of all the cryptocurrencies
    if Crypto_Asset_Prices.size == 0:

        Crypto_Asset_Prices = Exchange_Price
        Crypto_Asset_Volume = Exchange_Volume

    else:

        Crypto_Asset_Prices = np.column_stack(
            (Crypto_Asset_Prices, Exchange_Price))
        Crypto_Asset_Volume = np.column_stack(
            (Crypto_Asset_Volume, Exchange_Volume))

# turn prices and volumes into pandas dataframe
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=CRYPTO_ASSET)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=CRYPTO_ASSET)

# compute the price return of the day
two_before_price = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_price"), {"Time": two_before_TS})
two_before_price = two_before_price.drop(columns=["Time", "Date"])
return_df = two_before_price.append(Crypto_Asset_Prices)
price_ret = return_df.pct_change()
price_ret = price_ret.iloc[[1]]
# then add the 'Time' column
time_header = ["Time"]
time_header.extend(CRYPTO_ASSET)
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=time_header)
Crypto_Asset_Prices["Time"] = int(y_TS)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=time_header)
Crypto_Asset_Volume["Time"] = int(y_TS)
# adding the Time column to the price ret df
price_ret["Time"] = int(y_TS)

# computing the Exponential Weighted Moving Average of the day
hist_volume = query_mongo(DB_NAME, MONGO_DICT.get("coll_volume"))
hist_volume = hist_volume.drop(columns=["Date"])
hist_volume = hist_volume.append(Crypto_Asset_Volume)
daily_ewma = daily_ewma_crypto_volume(hist_volume, CRYPTO_ASSET)
# daily_ewma = ewma_df.iloc[[len(reference_date_vector) - 1]]


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
daily_ewma_double_check = daily_ewma_first_check * np.array(current_logic_two)
daily_ewma_double_check = pd.DataFrame(
    daily_ewma_double_check, columns=CRYPTO_ASSET)

# downloading from mongoDB the current weights
weights = query_mongo(DB_NAME, MONGO_DICT.get("coll_weights"))

# compute the daily syntethic matrix
yesterday_synt_matrix = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_synt"), {"Date": two_before_human[0]}
)
yesterday_synt_matrix = yesterday_synt_matrix.drop(columns=["Date", "Time"])
daily_return = np.array(price_ret.loc[:, CRYPTO_ASSET])
new_synt = (1 + daily_return) * np.array(yesterday_synt_matrix)
daily_synt = pd.DataFrame(new_synt, columns=CRYPTO_ASSET)

# compute the daily relative syntethic matrix
daily_tot = np.array(daily_synt.sum(axis=1))

daily_rel = np.array(daily_synt) / daily_tot
daily_rel = pd.DataFrame(daily_rel, columns=CRYPTO_ASSET)

# daily index value computation
current_divisor = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_divisor_res"), {"Date": two_before_human[0]}
)
curr_div_val = np.array(current_divisor["Divisor Value"])
index_numerator = np.array(
    Crypto_Asset_Prices[CRYPTO_ASSET]) * np.array(daily_rel)
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
    DB_NAME, MONGO_DICT.get("coll_1000_index"), {"Date": two_before_human[0]}
)
daily_index_1000 = np.array(
    yesterday_1000_index["Index Value"]) * (1 + variation)
daily_index_1000_df = pd.DataFrame(daily_index_1000, columns=["Index Value"])

# ############ MONGO DB UPLOADS ############################################
# creating the array with human readable Date
human_date = timestamp_to_human(reference_date_vector)

# put the "Crypto_Asset_Prices" dataframe on MongoDB
Crypto_Asset_Prices["Date"] = yesterday_human
mongo_upload(Crypto_Asset_Prices, "collection_price",
             reorder="Y", column_set_val="complete")

# put the "Crypto_Asset_Volumes" dataframe on MongoDB
Crypto_Asset_Volume["Date"] = yesterday_human
mongo_upload(Crypto_Asset_Volume, "collection_volume",
             reorder="Y", column_set_val="complete")

# put the exchange volumes on MongoDB
mongo_upload(exc_vol_tot, "collection_all_exc_vol")

# put the "price_ret" dataframe on MongoDB
price_ret["Date"] = yesterday_human
mongo_upload(price_ret, "collection_price_ret",
             reorder="Y", column_set_val="complete")

# put the EWMA dataframe on MongoDB
daily_ewma["Date"] = yesterday_human
daily_ewma["Time"] = y_TS
mongo_upload(daily_ewma, "collection_EWMA",
             reorder="Y", column_set_val="complete")

# put the double checked EWMA on MongoDB
daily_ewma_double_check["Date"] = yesterday_human
daily_ewma_double_check["Time"] = y_TS
mongo_upload(daily_ewma_double_check, "collection_EWMA_check",
             reorder="Y", column_set_val="complete")

# put the synth matrix on MongoDB
daily_synt["Date"] = yesterday_human
daily_synt["Time"] = y_TS
mongo_upload(daily_synt, "collection_synth",
             reorder="Y", column_set_val="complete")

# put the relative synth matrix on MongoDB
daily_rel["Date"] = yesterday_human
daily_rel["Time"] = y_TS
mongo_upload(daily_rel, "collection_relative_synth",
             reorder="Y", column_set_val="complete")


# put the reshaped divisor array on MongoDB
current_divisor["Date"] = yesterday_human
current_divisor["Time"] = y_TS
mongo_upload(current_divisor, "collection_divisor_reshaped",
             reorder="Y", column_set_val="divisor")

# put the index level 1000 on MongoDB
daily_index_1000_df["Date"] = yesterday_human
daily_index_1000_df["Time"] = y_TS
mongo_upload(daily_index_1000_df, "collection_index_level_1000",
             reorder="Y", column_set_val="index")

# put the index level raw on MongoDB
raw_index_df["Date"] = yesterday_human
raw_index_df["Time"] = y_TS
mongo_upload(raw_index_df, "collection_index_level_raw",
             reorder="Y", column_set_val="index")
