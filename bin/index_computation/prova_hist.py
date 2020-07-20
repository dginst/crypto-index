# standard library import
from datetime import datetime, timezone

# third party import<<<<
from pymongo import MongoClient
import numpy as np
import pandas as pd

# local import
import cryptoindex.mongo_setup as mongo
import cryptoindex.data_setup as data_setup
import cryptoindex.calc as calc
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME)

# ############# INITIAL SETTINGS ################################

pair_array = PAIR_ARRAY

Crypto_Asset = CRYPTO_ASSET

Exchanges = EXCHANGES

# ################## setup mongo connection ################


# define database name and collection name
db_name = DB_NAME


# drop the pre-existing collection (if there is one)
mongo_coll_drop("index_hist")

mongo_indexing()

collection_dict_upload = mongo_coll()

# ################### DATE SETTINGS ####################

# define the start date as MM-DD-YYYY
start_date = START_DATE

# define today and yesterady date as timestamp
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - 86400

# define end date as as MM-DD-YYYY
end_date = datetime.now().strftime("%m-%d-%Y")

# define the variable containing all the date from start_date to yesterday.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.date_gen(start_date, end_date)

# define all the useful arrays containing the rebalance
# start date, stop date, board meeting date
rebalance_start_date = calc.start_q("01-01-2016")
rebalance_stop_date = calc.stop_q(rebalance_start_date)
board_date = calc.board_meeting_day()
board_date_eve = calc.day_before_board()
next_rebalance_date = calc.next_start()
print(rebalance_start_date)
print(rebalance_stop_date)
print(next_rebalance_date)
print(board_date_eve)
# defining time variables
last_reb_start = str(int(rebalance_start_date[len(rebalance_start_date) - 1]))
next_reb_stop = str(int(rebalance_stop_date[len(rebalance_stop_date) - 1]))
last_reb_stop = str(int(rebalance_stop_date[len(rebalance_stop_date) - 2]))
curr_board_eve = str(int(board_date_eve[len(board_date_eve) - 1]))
print(last_reb_start)
print(next_reb_stop)
print(curr_board_eve)
# call the function that creates a object containing the
# couple of quarterly start-stop date
quarterly_date = calc.quarterly_period()
next_quarterly_date = calc.next_quarterly_period(initial_val=0)
for i, j in next_quarterly_date:
    print(i)
    print(j)
# #################### MAIN PART ###############################


# initialize the matrices that will contain the
# prices and volumes of all the cryptoasset
Crypto_Asset_Prices = np.matrix([])
Crypto_Asset_Volume = np.matrix([])
# initialize the matrix that will contain the complete first logic matrix
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

for CryptoA in Crypto_Asset:

    print(CryptoA)
    # initialize useful matrices
    ccy_pair_array = []
    Exchange_Price = np.matrix([])
    Exchange_Volume = np.matrix([])
    Ex_PriceVol = np.matrix([])

    # create the crypto-fiat strings useful to download from CW
    for pair in pair_array:
        ccy_pair_array.append(CryptoA.lower() + pair)

    for exchange in Exchanges:
        # print(exchange)
        # initialize the matrices that will contain the data related to all
        # currencypair for the single exchange
        Ccy_Pair_PriceVolume = np.matrix([])
        Ccy_Pair_Volume = np.matrix([])
        Ccy_Pair_Price = np.matrix([])

        for cp in ccy_pair_array:

            crypto = cp[:3]
            fiat_curr = cp[3:]
            # ######### LEAVING OUT NEW CRYPTO-FIAT PAIRS ##################
            c_1 = exchange == "bittrex" and fiat_curr == "eur"
            c_2 = exchange == "bittrex" and crypto == "ltc" and fiat_curr == "usd"
            c_3 = exchange == "poloniex" and crypto == "bch" and fiat_curr == "usdc"

            if c_1 or c_2 or c_3:
                continue

            # ###############################################################

            # defining the dictionary for the MongoDB query
            query_dict = {"Exchange": exchange, "Pair": cp}
            # retriving the needed information on MongoDB
            matrix = mongo.query_mongo(
                db_name, MONGO_DICT.get("coll_cw_final"), query_dict)

            try:

                matrix = matrix.drop(columns=["Low", "High", "Open"])

            except:

                pass

            try:

                cp_matrix = matrix.to_numpy()

                # retrieves the wanted data from the matrix
                # 2 for crypto vol, 3 for pair volume
                priceXvolume = cp_matrix[:, 1] * cp_matrix[:, 3]
                volume = cp_matrix[:, 3]  # 2 for crypto vol, 3 for pair volume
                price = cp_matrix[:, 1]

                # every "cp" the for loop adds a column in the matrices
                # referred to the single "exchange"
                if Ccy_Pair_PriceVolume.size == 0:
                    Ccy_Pair_PriceVolume = priceXvolume
                    Ccy_Pair_Volume = volume
                else:
                    Ccy_Pair_PriceVolume = np.column_stack(
                        (Ccy_Pair_PriceVolume, priceXvolume)
                    )
                    Ccy_Pair_Volume = np.column_stack((Ccy_Pair_Volume, volume))

            except AttributeError:

                pass

        # computing the volume weighted average price of the single exchange
        if (
            Ccy_Pair_Volume.size != 0
            and Ccy_Pair_Volume.size > reference_date_vector.size
        ):

            PxV = Ccy_Pair_PriceVolume.sum(axis=1)
            V = Ccy_Pair_Volume.sum(axis=1)
            Ccy_Pair_Price = np.divide(
                PxV, V, out=np.zeros_like(V), where=V != 0.0)

            # computing the total volume of the exchange
            Ccy_Pair_Volume = Ccy_Pair_Volume.sum(axis=1)
            # computing price X volume of the exchange
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        # case when just one crypto-fiat has the values in that exchange
        elif (
            Ccy_Pair_Volume.size != 0
            and Ccy_Pair_Volume.size == reference_date_vector.size
        ):

            np.seterr(all=None, divide="warn")
            Ccy_Pair_Price = np.divide(
                Ccy_Pair_PriceVolume,
                Ccy_Pair_Volume,
                out=np.zeros_like(Ccy_Pair_Volume),
                where=Ccy_Pair_Volume != 0.0,
            )
            Ccy_Pair_Price = np.nan_to_num(Ccy_Pair_Price)
            Ccy_Pair_PxV = Ccy_Pair_Price * Ccy_Pair_Volume

        else:

            Ccy_Pair_Price = np.array([])
            Ccy_Pair_Volume = np.array([])
            Ccy_Pair_PxV = np.array([])

        # creating every loop the matrices containing the data
        # referred to all the exchanges Exchange_Price contains
        # the crypto ("cp") prices in all the different Exchanges
        # Exchange_Volume contains the crypto ("cp") volume
        # in all the different Exchanges
        if Exchange_Price.size == 0:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = Ccy_Pair_Price
                Exchange_Volume = Ccy_Pair_Volume
                Ex_PriceVol = Ccy_Pair_PxV

            else:

                Exchange_Price = np.zeros(reference_date_vector.size)
                Exchange_Volume = np.zeros(reference_date_vector.size)
                Ex_PriceVol = np.zeros(reference_date_vector.size)

        else:

            if Ccy_Pair_Volume.size != 0:

                Exchange_Price = np.column_stack(
                    (Exchange_Price, Ccy_Pair_Price))
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, Ccy_Pair_Volume))
                Ex_PriceVol = np.column_stack((Ex_PriceVol, Ccy_Pair_PxV))

            else:

                Exchange_Price = np.column_stack(
                    (Exchange_Price, np.zeros(reference_date_vector.size))
                )
                Exchange_Volume = np.column_stack(
                    (Exchange_Volume, np.zeros(reference_date_vector.size))
                )
                Ex_PriceVol = np.column_stack(
                    (Ex_PriceVol, np.zeros(reference_date_vector.size))
                )

    # dataframes that contain volume and price of a single crytpo
    # for all the exchanges; if an exchange does not have value
    # in the crypto will be insertd a column with zero
    Exchange_Vol_DF = pd.DataFrame(Exchange_Volume, columns=Exchanges)
    Exchange_Price_DF = pd.DataFrame(Exchange_Price, columns=Exchanges)

    # adding "Time" column to both Exchanges dataframe
    Exchange_Vol_DF["Time"] = reference_date_vector
    Exchange_Price_DF["Time"] = reference_date_vector

    exc_vol_p = Exchange_Vol_DF
    exc_vol_p["Crypto"] = CryptoA
    exc_vol_tot = exc_vol_tot.append(exc_vol_p)
    # for each CryptoAsset compute the first logic array
    first_logic_array = calc.first_logic_matrix(Exchange_Vol_DF, Exchanges)

    # put the logic array into the logic matrix
    if logic_matrix_one.size == 0:
        logic_matrix_one = first_logic_array
    else:
        logic_matrix_one = np.column_stack(
            (logic_matrix_one, first_logic_array))

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
        # single Crypto_Asset into a single vector
        Exchange_Volume = Exchange_Volume.sum(axis=1)
    except np.AxisError:
        Exchange_Price = Exchange_Price
        Exchange_Volume = Exchange_Volume

    # creating every loop the matrices of all Cryptoassets
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

# print(exc_vol_tot)
# turn prices and volumes into pandas dataframe
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=Crypto_Asset)
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=Crypto_Asset)

# compute the price returns over the defined period
price_ret = Crypto_Asset_Prices.pct_change()

# add the 'Time' column
time_header = ["Time"]
time_header.extend(Crypto_Asset)
Crypto_Asset_Prices = pd.DataFrame(Crypto_Asset_Prices, columns=time_header)
Crypto_Asset_Prices["Time"] = reference_date_vector
Crypto_Asset_Volume = pd.DataFrame(Crypto_Asset_Volume, columns=time_header)
Crypto_Asset_Volume["Time"] = reference_date_vector
price_ret["Time"] = reference_date_vector

# turn the first logic matrix into a dataframe and add the 'Time' column
# containg the stop_date of each quarter as in "rebalance_stop_date"
# array the 'Time' column does not take into account the last value
# because it refers to a period that has not been yet calculated
# (and will be this way until today == new quarter start_date)
first_logic_matrix = pd.DataFrame(logic_matrix_one, columns=Crypto_Asset)

first_logic_matrix["Time"] = next_rebalance_date[1: len(next_rebalance_date)]
print(first_logic_matrix)

# computing the Exponential Moving Weighted Average of the selected period
ewma_df = calc.ewma_crypto_volume(
    Crypto_Asset_Volume, Crypto_Asset, reference_date_vector, time_column="N"
)

# computing the second logic matrix
second_logic_matrix = calc.second_logic_matrix(
    Crypto_Asset_Volume,
    first_logic_matrix,
    Crypto_Asset,
    reference_date_vector,
    time_column="Y",
)
print(second_logic_matrix)
# computing the ewma checked with both the first and second logic matrices
double_checked_EWMA = calc.ewma_second_logic_check(
    first_logic_matrix,
    second_logic_matrix,
    ewma_df,
    reference_date_vector,
    Crypto_Asset,
    time_column="Y"
)

# computing the Weights that each CryptoAsset should have
# starting from each new quarter every weigfhts is computed
# in the period that goes from present quarter start_date to
# present quarter board meeting date eve
weights_for_board = calc.quarter_weights(
    double_checked_EWMA, board_date_eve[1:], Crypto_Asset
)

print(weights_for_board)

# compute the syntethic matrix and the relative syntethic matrix
syntethic = calc.quarterly_synt_matrix(
    Crypto_Asset_Prices,
    weights_for_board,
    reference_date_vector,
    board_date_eve,
    Crypto_Asset,
)

syntethic_relative_matrix = calc.relative_syntethic_matrix(
    syntethic, Crypto_Asset)

# changing the "Time" column of the second logic matrix
# using the rebalance date
second_logic_matrix["Time"] = next_rebalance_date[1: len(
    next_rebalance_date)]

if y_TS == rebalance_start_date[len(rebalance_start_date) - 1]:

    second_logic_matrix = second_logic_matrix[:-1]

print(second_logic_matrix)

# changing the "Time" column of the weights in order to
# display the quarter start date of each row
weights_for_period = weights_for_board
print(weights_for_period)
if y_TS >= int(curr_board_eve) and y_TS <= int(last_reb_stop):

    weights_for_period['Time'] = next_rebalance_date[1:]

else:

    weights_for_period["Time"] = rebalance_start_date[1:]

print(weights_for_period)

divisor_array = calc.divisor_adjustment(
    Crypto_Asset_Prices,
    weights_for_period,
    second_logic_matrix,
    Crypto_Asset,
    reference_date_vector,
)

print(divisor_array)

reshaped_divisor = calc.divisor_reshape(divisor_array, reference_date_vector)

index_values = calc.index_level_calc(
    Crypto_Asset_Prices, syntethic_relative_matrix, divisor_array, reference_date_vector
)

index_1000_base = calc.index_based(index_values)
# pd.set_option('display.max_rows', None)

# #################### MONGO DB UPLOADS ###########################
# creating the array with human readable Date
human_date = data_setup.timestamp_to_human(reference_date_vector)

# put the "Crypto_Asset_Prices" dataframe on MongoDB
Crypto_Asset_Prices["Date"] = human_date
mongo_upload(Crypto_Asset_Prices, "collection_price",
             reorder="Y", column_set_val="complete")
# price_up = mongo.df_reorder(Crypto_Asset_Prices, column_set="complete")
# price_up = price_up.to_dict(orient="records")
# collection_dict_upload.get("collection_price").insert_many(price_up)

# put the "Crypto_Asset_Volumes" dataframe on MongoDB
Crypto_Asset_Volume["Date"] = human_date
volume_up = mongo.df_reorder(Crypto_Asset_Volume, column_set="complete")
volume_up = volume_up.to_dict(orient="records")
collection_dict_upload.get("collection_volume").insert_many(volume_up)

# put the exchange volumes on MongoDB
exc_vol_up = exc_vol_tot.to_dict(orient="records")
collection_dict_upload.get("collection_all_exc_vol").insert_many(exc_vol_up)

# put the "price_ret" dataframe on MongoDB
price_ret["Date"] = human_date
price_ret_up = mongo.df_reorder(price_ret, column_set="complete")
price_ret_up = price_ret_up.to_dict(orient="records")
collection_dict_upload.get("collection_price_ret").insert_many(price_ret_up)

# put the "weights" dataframe on MongoDB
weight_human_date = data_setup.timestamp_to_human(weights_for_board["Time"])
weights_for_board["Date"] = weight_human_date
weights_for_board = mongo.df_reorder(weights_for_board, column_set="complete")
up_weights = weights_for_board.to_dict(orient="records")
collection_dict_upload.get("collection_weights").insert_many(up_weights)

# put the first logic matrix on MongoDB
first_date = data_setup.timestamp_to_human(first_logic_matrix["Time"])
first_logic_matrix["Date"] = first_date
first_up = mongo.df_reorder(first_logic_matrix, column_set="complete")
first_up = first_up.to_dict(orient="records")
collection_dict_upload.get("collection_logic_one").insert_many(first_up)

# put the second logic matrix on MongoDB
second_date = data_setup.timestamp_to_human(second_logic_matrix["Time"])
second_logic_matrix["Date"] = second_date
second_up = mongo.df_reorder(second_logic_matrix, column_set="complete")
second_up = second_up.to_dict(orient="records")
collection_dict_upload.get("collection_logic_two").insert_many(second_up)

# put the EWMA dataframe on MongoDB
ewma_df["Date"] = human_date
ewma_df["Time"] = reference_date_vector
ewma_df_up = mongo.df_reorder(ewma_df, column_set="complete")
ewma_df_up = ewma_df_up.to_dict(orient="records")
collection_dict_upload.get("collection_EWMA").insert_many(ewma_df_up)

# put the double checked EWMA on MongoDB
double_checked_EWMA["Date"] = human_date
double_EWMA_up = mongo.df_reorder(double_checked_EWMA, column_set="complete")
double_EWMA_up = double_EWMA_up.to_dict(orient="records")
collection_dict_upload.get("collection_EWMA_check").insert_many(double_EWMA_up)

# put the synth matrix on MongoDB
syntethic["Date"] = human_date
syntethic["Time"] = reference_date_vector
syntethic_up = syntethic
syntethic_up = mongo.df_reorder(syntethic_up, column_set="complete")
syntethic_up = syntethic_up.to_dict(orient="records")
collection_dict_upload.get("collection_synth").insert_many(syntethic_up)

# put the relative synth matrix on MongoDB
syntethic_relative_matrix["Date"] = human_date
syntethic_relative_matrix["Time"] = reference_date_vector
synth_up = syntethic_relative_matrix
synth_up = mongo.df_reorder(synth_up, column_set="complete")
synth_up = synth_up.to_dict(orient="records")
collection_dict_upload.get("collection_relative_synth").insert_many(synth_up)

# put the divisor array on MongoDB
divisor_date = data_setup.timestamp_to_human(divisor_array["Time"])
divisor_array["Date"] = divisor_date
divisor_up = mongo.df_reorder(divisor_array, column_set="divisor")
divisor_up = divisor_up.to_dict(orient="records")
collection_dict_upload.get(
    "collection_divisor").insert_many(divisor_up)

# put the reshaped divisor array on MongoDB
reshaped_divisor_date = data_setup.timestamp_to_human(reshaped_divisor["Time"])
reshaped_divisor["Date"] = reshaped_divisor_date
reshaped_divisor_up = mongo.df_reorder(reshaped_divisor, column_set="divisor")
reshaped_divisor_up = reshaped_divisor_up.to_dict(orient="records")
collection_dict_upload.get(
    "collection_divisor_reshaped").insert_many(reshaped_divisor_up)

# put the index level 1000 on MongoDB
index_1000_base["Date"] = human_date
index_1000_base["Time"] = reference_date_vector
index_val_up = mongo.df_reorder(index_1000_base, column_set="index")
index_val_up = index_val_up.to_dict(orient="records")
collection_dict_upload.get(
    "collection_index_level_1000").insert_many(index_val_up)

# put the index level raw on MongoDB
index_values["Date"] = human_date
index_values["Time"] = reference_date_vector
index_val_up = mongo.df_reorder(index_values, column_set="index")
index_val_up = index_val_up.to_dict(orient="records")
collection_dict_upload.get(
    "collection_index_level_raw").insert_many(index_val_up)

# ####### some printing ##########
print("Crypto_Asset_Prices")
print(Crypto_Asset_Prices)
print("Crypto_Asset_Volume")
print(Crypto_Asset_Volume)
print("Price Returns")
print(price_ret)
print("EWMA DataFrame")
print(ewma_df)
print("WEIGHTS for board")
print(weights_for_board)
print("Syntethic relative matrix")
print(syntethic_relative_matrix)
print("index value")
print(index_values)
#################################