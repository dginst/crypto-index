# standard library import
import time
from datetime import datetime, timezone

# third party import
import pandas as pd
import numpy as np

# local import
from cryptoindex.data_setup import (
    date_gen, exc_pair_cleaning, exc_value_cleaning)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload, df_reorder)
from cryptoindex.config import (
    EXC_START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME, CLEAN_DATA_HEAD)


# ########################## initial settings #################################

# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC

# creating the timestamp array at 12:00 AM
date_array = date_gen(EXC_START_DATE)
date_array_str = [str(el) for el in date_array]
print(date_array_str)

# defining the crypto-fiat pairs array
cryptofiat_array = []

for crypto in CRYPTO_ASSET:

    for fiat in PAIR_ARRAY:

        cryptofiat_array.append(crypto.lower() + fiat)

# ############################ setup mongo connection ##################

# drop the pre-existing collection (if there is one)
mongo_coll_drop("exc")

mongo_indexing()

collection_dict_upload = mongo_coll()


# ################### creation of EXC_cleandata collection ##################

# querying all raw data from EXC_rawdata
all_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_raw"))

# keeping only the columns of interest among all the
# information in rawdata
all_data = all_data.loc[:, CLEAN_DATA_HEAD]

# changing the "Time" values format from integer to string
all_data["Time"] = [str(element) for element in all_data["Time"]]

# creating a column containing the hour of extraction
# all_data["hour"] = all_data["date"].str[11:16]

# selecting the date corresponding to 12:00 AM
all_data = all_data.loc[all_data["Time"].isin(date_array_str)]

# changing some features in "Pair" field
all_data = exc_pair_cleaning(all_data)

# selecting the crypto-fiat pairs used in the index computation
all_data = all_data.loc[all_data["Pair"].isin(cryptofiat_array)]

# selecting the exchange used in the index computation
all_data = all_data.loc[all_data["Exchange"].isin(EXCHANGES)]

# correcting the "Pair Volume" field
all_data = exc_value_cleaning(all_data)

# ########## DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix containing the exchange-pair logic values
logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_keys"))

# creating the exchange-pair couples key for the daily matrix
all_data["key"] = all_data["Exchange"] + "&" + all_data["Pair"]

# ########## adding the dead series to the daily values ##################

# selecting only the exchange-pair couples present in the historical series
key_present = logic_key.loc[logic_key.logic_value == 1]
key_present = key_present.drop(columns=["logic_value"])

# selecting the last day of the EXC "historical" series
last_day = all_data.loc[all_data.Time == str(y_TS)]

# applying a left join between the prresent keys matrix and the last_day
# matrix, this operation returns a matrix containing all the keys in
# "key_present" and, if some keys are missing in "all_data" put NaN
merged = pd.merge(key_present, last_day, on="key", how="left")

# selecting only the absent keys
merg_absent = merged.loc[merged["Close Price"].isnull()]

header = ["Pair", "Exchange", "Time",
          "Close Price", "Crypto Volume",
          "Pair Volume", "key"]

# create the historical series for each selected element
for k in merg_absent["key"]:

    mat_to_add = pd.DataFrame(columns=header)
    mat_to_add["Time"] = date_array_str
    split_val = k.split("&")
    mat_to_add["Exchange"] = split_val[0]
    mat_to_add["Pair"] = split_val[1]
    mat_to_add["Close Price"] = 0.0
    mat_to_add["Crypto Volume"] = 0.0
    mat_to_add["Pair Volume"] = 0.0
    all_data = all_data.append(mat_to_add)

# uploading the cleaned data on MongoDB in the collection EXC_cleandata
all_data = all_data.drop(columns=["key"])
mongo_upload(all_data, "collection_exc_clean")
data = all_data.to_dict(orient="records")


# ################# DATA CONVERSION MAIN PART ##################

start = time.time()


# querying the data from mongo
matrix_rate = query_mongo(DB_NAME, MONGO_DICT.get("coll_ecb_clean"))
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_rate = matrix_rate.loc[matrix_rate.Time.isin(date_array_str)]

matrix_data = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_clean"))

matrix_rate_stable = query_mongo(
    DB_NAME, MONGO_DICT.get("coll_stable_rate"))
matrix_rate_stable = matrix_rate_stable.loc[matrix_rate_stable.Time.isin(
    date_array_str)]

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

mongo_upload(converted_data, "collection_exc_final_data")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
