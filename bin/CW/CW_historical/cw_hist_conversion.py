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
import numpy as np

# local import
from cryptoindex.data_setup import (
    date_gen, timestamp_to_human, fix_zero_value)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll_drop, mongo_coll,
    mongo_indexing, mongo_upload, df_reorder)
from cryptoindex.config import (
    START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME)

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

start = time.time()

Exchanges = EXCHANGES

# taking BTC/USD pair historical
first_query = {"Pair": "btcusd", "Exchange": "kraken"}
first_call = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), first_query)
# isolating some values in single variables
time_arr = first_call[["Time"]]
price_df = first_call[["Close Price"]]
volume_df = first_call[["Pair Volume"]]
price_df = price_df.rename(columns={"Close Price": "kraken"})
volume_df = volume_df.rename(columns={"Pair Volume": "kraken"})
Exchanges.remove("kraken")

for exchange in Exchanges:

    query = {"Pair": "btcusd", "Exchange": exchange}
    single_ex = query_mongo(
        DB_NAME, MONGO_DICT.get("coll_cw_clean"), query)

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
average_df["Time"] = first_call["Time"]
average_df = average_df.replace([np.inf, -np.inf], np.nan)
average_df.fillna(0, inplace=True)

# first_query = {'Pair': 'btcusd', 'Exchange': 'kraken'}
# first_call = mongo.query_mongo(database, collection, first_query)
# average_df = first_call
# average_df['average usd'] = average_df['Close Price']

# ############# USDT exchange rates computation ##########
# BTC/USDT is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historical values from 01/01/2016

# POLONIEX usdt/usd exchange rate
query_usdt = {"Exchange": "poloniex", "Pair": "btcusdt"}
usdt_poloniex = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdt)

# KRAKEN usdt/usd exchange rate
query_usdt = {"Exchange": "kraken", "Pair": "btcusdt"}
usdt_kraken = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdt)

# BITTREX usdt/usd exchange rate
query_usdt = {"Exchange": "bittrex", "Pair": "btcusdt"}
usdt_bittrex = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdt)

# computing the rate on each exchange
usdt_kraken["rate"] = usdt_kraken["Close Price"] / average_df["average usd"]
usdt_kraken.fillna(0, inplace=True)

usdt_bittrex["rate"] = usdt_bittrex["Close Price"] / average_df["average usd"]
usdt_bittrex.fillna(0, inplace=True)

usdt_poloniex["rate"] = usdt_poloniex["Close Price"] / average_df["average usd"]
usdt_poloniex.fillna(0, inplace=True)

# USDT rate weighted average computation
poloniex_weighted = usdt_poloniex["rate"] * usdt_poloniex["Pair Volume"]
kraken_weighted = usdt_kraken["rate"] * usdt_kraken["Pair Volume"]
bittrex_weighted = usdt_bittrex["rate"] * usdt_bittrex["Pair Volume"]

total_weights = (
    usdt_kraken["Pair Volume"]
    + usdt_bittrex["Pair Volume"]
    + usdt_poloniex["Pair Volume"]
)

usdt_rates = (kraken_weighted + bittrex_weighted
              + poloniex_weighted) / total_weights

usdt_rates = 1 / usdt_rates

# tranforming the data structure into a dataframe
usdt_rates = pd.DataFrame(usdt_rates, columns=["Rate"])
usdt_rates = usdt_rates.replace([np.inf, -np.inf], np.nan)
usdt_rates.fillna(0, inplace=True)

# adding Currency (USDT/USD), Time (timestamp),
# and Standard Date (YYYY-MM-DD) columns
usdt_rates["Currency"] = np.zeros(len(usdt_rates["Rate"]))
usdt_rates["Currency"] = [
    str(x).replace("0.0", "USDT/USD") for x in usdt_rates["Currency"]
]
usdt_rates["Time"] = first_call["Time"]

usdt_rates.fillna("NaN", inplace=True)

index_to_remove = usdt_rates[usdt_rates.Time == "NaN"].index

usdt_rates = usdt_rates.drop(index_to_remove)
print(usdt_rates)
usdt_rates["Standard Date"] = timestamp_to_human(first_call["Time"])
print(usdt_rates)
# correcting the date 2016-10-02 using the previous day rate
prev_rate = np.array(usdt_rates.loc[usdt_rates.Time == '1475280000', "Rate"])
usdt_rates.loc[usdt_rates.Time == '1475366400', "Rate"] = prev_rate

# USDT mongoDB upload
mongo_upload(usdt_rates, "collection_stable_rate")

# usdt_data = usdt_rates.to_dict(orient="records")
# collection_stable.insert_many(usdt_data)


# ############# USDC exchange rates computation ############
# BTC/USDC is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historoical values from 01/01/2016

# POLONIEX usdc/usd exchange rate
query_usdc = {"Exchange": "poloniex", "Pair": "btcusdc"}
usdc_poloniex = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdc)

# KRAKEN usdc/usd exchange rate
query_usdc = {"Exchange": "kraken", "Pair": "btcusdc"}
usdc_kraken = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdc)

# COINBASE_PRO usdc exchange rate
query_usdc_coinbase = {"Exchange": "coinbase-pro", "Pair": "btcusdc"}
usdc_coinbase = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_clean"), query_usdc_coinbase)

# computing the rate on each exchange
usdc_poloniex["rate"] = usdc_poloniex["Close Price"] / average_df["average usd"]
usdc_poloniex.fillna(0, inplace=True)

usdc_kraken["rate"] = usdc_kraken["Close Price"] / average_df["average usd"]
usdc_kraken.fillna(0, inplace=True)

usdc_coinbase["rate"] = usdc_coinbase["Close Price"] / average_df["average usd"]
usdc_coinbase.fillna(0, inplace=True)

# USDC rates weighted average computation
poloniex_weighted = usdc_poloniex["rate"] * usdc_poloniex["Pair Volume"]
kraken_weighted = usdc_kraken["rate"] * usdc_kraken["Pair Volume"]
coinbase_weighted = usdc_coinbase["rate"] * usdc_coinbase["Pair Volume"]

total_weights = (
    usdc_kraken["Pair Volume"]
    + usdc_coinbase["Pair Volume"]
    + usdc_poloniex["Pair Volume"]
)

usdc_rates = (kraken_weighted + coinbase_weighted
              + poloniex_weighted) / total_weights
usdc_rates = 1 / usdc_rates

# tranforming the data structure into a dataframe
usdc_rates = pd.DataFrame(usdc_rates, columns=["Rate"])
usdc_rates = usdc_rates.replace([np.inf, -np.inf], np.nan)
usdc_rates.fillna(0, inplace=True)

# adding Currency (USDC/USD), Time (timestamp),
# and Standard Date (YYYY-MM-DD) columns
usdc_rates["Currency"] = np.zeros(len(usdc_rates["Rate"]))
usdc_rates["Currency"] = [
    str(x).replace("0.0", "USDC/USD") for x in usdc_rates["Currency"]
]
usdc_rates["Time"] = first_call["Time"]
usdc_rates["Standard Date"] = timestamp_to_human(first_call["Time"])

# USDC mongoDB upload
mongo_upload(usdc_rates, "collection_stable_rate")
# usdc_data = usdc_rates.to_dict(orient="records")
# collection_stable.insert_many(usdc_data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# ##############################################################

# ################# DATA CONVERSION MAIN PART ##################

start = time.time()

# querying the data from mongo
matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"))
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_rate = matrix_rate.loc[matrix_rate.Time != "1451520000"]
matrix_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_clean"))
matrix_rate_stable = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_stable_rate"))

# creating a column containing the fiat currency
matrix_rate["fiat"] = [x[:3].lower() for x in matrix_rate["Currency"]]
matrix_data["fiat"] = [x[3:].lower() for x in matrix_data["Pair"]]
matrix_rate_stable["fiat"] = [x[:4].lower()
                              for x in matrix_rate_stable["Currency"]]

# ############ creating a USD subset which will not be converted #########

usd_matrix = matrix_data.loc[matrix_data["fiat"] == "usd"]
usd_matrix = df_reorder(usd_matrix, "conversion")

# ########### converting non-USD fiat currencies #########################

# creating a matrix for conversion
conv_fiat = ["gbp", "eur", "cad", "jpy"]
conv_matrix = matrix_data.loc[matrix_data["fiat"].isin(conv_fiat)]

# merging the dataset on 'Time' and 'fiat' column
conv_merged = pd.merge(conv_matrix, matrix_rate, on=["Time", "fiat"])

# converting the prices in usd
conv_merged["Close Price"] = conv_merged["Close Price"] / conv_merged["Rate"]
conv_merged["Close Price"] = conv_merged["Close Price"].replace(
    [np.inf, -np.inf], np.nan
)
conv_merged["Close Price"].fillna(0, inplace=True)
conv_merged["Pair Volume"] = conv_merged["Pair Volume"] / conv_merged["Rate"]
conv_merged["Pair Volume"] = conv_merged["Pair Volume"].replace(
    [np.inf, -np.inf], np.nan
)
conv_merged["Pair Volume"].fillna(0, inplace=True)


# subsetting the dataset with only the relevant columns
conv_merged = df_reorder(conv_merged, "conversion")


# ############## converting STABLECOINS currencies #########################

# creating a matrix for stablecoins
stablecoin = ["usdc", "usdt"]
stablecoin_matrix = matrix_data.loc[matrix_data["fiat"].isin(stablecoin)]

# merging the dataset on 'Time' and 'fiat' column
stable_merged = pd.merge(
    stablecoin_matrix, matrix_rate_stable, on=["Time", "fiat"])

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

# reunite the dataframes and put data on MongoDB
converted_data = conv_merged
converted_data = converted_data.append(stable_merged)
converted_data = converted_data.append(usd_matrix)

converted_data = converted_data.sort_values(by=["Time"])

mongo_upload(converted_data, "collection_cw_converted")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# #####################################################################
# ############### LOGIC MATRIX OF KEYS ################################

start = time.time()

# retriving the needed information on MongoDB
q_dict = {"Time": str(y_TS)}
matrix_last_day = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_cw_conv"), q_dict)

old_head = matrix_last_day.columns
matrix_last_day["key"] = matrix_last_day["Exchange"] + \
    "&" + matrix_last_day["Pair"]
matrix_last_day["logic_value"] = 1
matrix_last_day = matrix_last_day.drop(columns=old_head)
print(matrix_last_day)
# creating the list containing all the possible exchange-pair key

all_key = []
for exc in EXCHANGES:

    for cry in CRYPTO_ASSET:

        for i in PAIR_ARRAY:

            all_key.append(exc + "&" + cry.lower() + i)

# creating the logic check dataframe
header = ["key", "logic_value"]
key_df = pd.DataFrame(columns=header)
key_df["key"] = all_key

#
key_df["logic_value"] = 0
# key_df['logic_value'] = np.zeros(len(all_key) - 1)
key_df = key_df.loc[~key_df.key.isin(matrix_last_day["key"])]

key_df = key_df.append(matrix_last_day)
#

# merged = pd.merge(key_df, matrix_last_day, on="key", how="left")
# merged.fillna(0, inplace=True)

# key_df["logic_value"] = merged["logic"]

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
