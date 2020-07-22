# standard library import
from datetime import datetime, timezone

# third party import
import pandas as pd
import numpy as np

# local import
from cryptoindex.data_setup import (
    date_gen)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll,
    mongo_indexing, mongo_upload)
from cryptoindex.config import (
    START_DATE, DAY_IN_SEC, MONGO_DICT, DB_NAME)


# ############# INITIAL SETTINGS ################################

# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC
two_before_TS = y_TS - DAY_IN_SEC

# defining the array containing all the date from start_period until today
date_complete_int = date_gen(START_DATE)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_complete_int]

# #################### setup mongo connection ##################

# creating the empty collections cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()


# ################# DAILY DATA CONVERSION MAIN PART ##################

# querying the data from mongo
query_data = {"Time": str(y_TS)}
query_rate = {"Date": str(y_TS)}
query_stable = {"Time": str(y_TS)}
matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"), query_rate)
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_clean"), query_data)
print(matrix_data)
matrix_rate_stable = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_stable_rate"), query_stable)

# creating a column containing the fiat currency
matrix_rate["fiat"] = [x[:3].lower() for x in matrix_rate["Currency"]]
matrix_data["fiat"] = [x[3:].lower() for x in matrix_data["Pair"]]
matrix_rate_stable["fiat"] = [x[:4].lower()
                              for x in matrix_rate_stable["Currency"]]

# ############ creating a USD subset which will not be converted #########

usd_matrix = matrix_data.loc[matrix_data["fiat"] == "usd"]
usd_matrix = usd_matrix[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]
]

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
conv_merged = conv_merged[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]
]

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
stable_merged = stable_merged[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]
]

# reunite the dataframes and put data on MongoDB
converted_data = conv_merged
converted_data = converted_data.append(stable_merged)
converted_data = converted_data.append(usd_matrix)
print(converted_data)
mongo_upload(converted_data, "collection_exc_final_data")
