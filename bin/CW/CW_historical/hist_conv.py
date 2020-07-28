# ############################################################################
# The script is divided in three main parts.
# 1) USDT and USDC rates computation
# using the weighted average value of btc/usd rates (computed using all the
# avalilable exchanges) and the average btc/usdc and btc/usdc rates, the script
# computes USDT/USD and USDC/USD rate. The historical series of rates is
# uploaded on MongoDB in the collection "stable_coin_rates"
# 2) non-USD  values conversion into USD
# The file retrieves data from MongoDB collection "CW_cleandata" and, for each
# Crypto-Fiat historical series, converts the data into USD values using
# the ECB rates stored on MongoDB in the collection "ecb_clean" and the
# computed stablecoin rates saved in the "stable_coin_rates" collection.
# Once everything has been converted, the script upload the dataframe on
# MongoDB collection "converted_data".
# 3) filling the zero-volume values
# the script, if the historical series have date where the crypto-pair is
# traded but the volume displayed is zero, takes the previous day value
# and fills the missing volume.
# ###############################################################################

# standard library import
import time
from datetime import datetime, timezone

# third party import
import pandas as pd

# local import

from cryptoindex.calc import (
    conv_into_usd, btcusd_average, stable_rate_calc, key_log_mat)
from cryptoindex.data_setup import (
    date_gen, fix_zero_value)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll_drop, mongo_coll,
    mongo_indexing, mongo_upload)
from cryptoindex.config import (
    START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CONVERSION_FIAT, STABLE_COIN,
    USDT_EXC_LIST, USDC_EXC_LIST
)

start = time.time()

# ################## initial settings #####################################

# define today date as timestamp
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC


# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = date_gen(START_DATE)


# ################ setup MongoDB connection ################

# drop the pre-existing collection
mongo_coll_drop("cw_hist_conv")

# creating the empty collection cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()

# ########## USDC/USD and USDT/USD computation #####################

average_btcusd = btcusd_average(DB_NAME, "coll_cw_clean", EXCHANGES)

usdt_rates = stable_rate_calc(
    DB_NAME, "coll_cw_clean", "USDT", USDT_EXC_LIST, average_btcusd)

usdc_rates = stable_rate_calc(
    DB_NAME, "coll_cw_clean", "USDC", USDC_EXC_LIST, average_btcusd)

mongo_upload(usdt_rates, "collection_stable_rate")
mongo_upload(usdc_rates, "collection_stable_rate")


end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# ##############################################################

# ################# DATA CONVERSION  PART ##################

start = time.time()

# querying the data from mongo
matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"))
matrix_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_clean"))
matrix_rate_stable = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_stable_rate"))

# converting the "matrix_rate" df
converted_data = conv_into_usd(
    DB_NAME, matrix_data, matrix_rate,
    matrix_rate_stable, CONVERSION_FIAT, STABLE_COIN)

# uploading converted data on MongoDB
mongo_upload(converted_data, "collection_cw_converted")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# #####################################################################
# ############### LOGIC MATRIX OF KEYS ################################

start = time.time()

key_df = key_log_mat(DB_NAME, "coll_cw_conv", y_TS,
                     EXCHANGES, CRYPTO_ASSET, PAIR_ARRAY)

mongo_upload(key_df, "collection_CW_key")
mongo_upload(key_df, "collection_EXC_key")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# ################ ZERO VOLUMES VALUE FILLING #####################

# retriving the needed information on MongoDB
matrix = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_conv"))
# print(matrix)
# matrix = pd.DataFrame(list(matrix))
# print(matrix)
# matrix = matrix.drop(columns=["_id"])

matrix["Crypto"] = matrix["Pair"].str[:3]
head = [
    "Time",
    "Close Price",
    "Crypto Volume",
    "Pair Volume",
    "Exchange",
    "Pair",
    "Crypto",
]
final_matrix = pd.DataFrame(columns=head)

for Crypto in CRYPTO_ASSET:

    cry_matrix = matrix.loc[matrix.Crypto == Crypto.lower()]
    exc_list = list(matrix["Exchange"].unique())

    for exchange in exc_list:

        ex_matrix = cry_matrix.loc[cry_matrix.Exchange == exchange]
        ex_matrix.drop(columns=["Crypto"])
        # finding the crypto-fiat pair in the selected exchange
        pair_list = list(ex_matrix["Pair"].unique())

        for cp in pair_list:

            cp_matrix = ex_matrix.loc[ex_matrix.Pair == cp]
            # checking if the matrix is not empty
            try:

                if cp_matrix.shape[0] > 1:

                    cp_matrix = fix_zero_value(cp_matrix)

                    final_matrix = final_matrix.append(cp_matrix)

            except AttributeError:
                pass

# put the manipulated data on MongoDB
mongo_upload(final_matrix, "collection_cw_final_data")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
