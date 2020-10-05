# standard library import
import time
from datetime import datetime, timezone

# third party import
import pandas as pd
import numpy as np

# local import
from cryptoindex.calc import (
    conv_into_usd
)
from cryptoindex.data_setup import (
    date_gen, exc_pair_cleaning, exc_pair_cleaning)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload, df_reorder)
from cryptoindex.config import (
    EXC_START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CLEAN_DATA_HEAD, STABLE_COIN,
    CONVERSION_FIAT
)
from cryptoindex.exc_func import (
    exc_time_split
)

# ########################## initial settings #################################

# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC

# creating the timestamp array at 12:00 AM
date_array = date_gen(EXC_START_DATE)
date_array_str = [str(el) for el in date_array]

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


# changing the "Time" values format from integer to string
all_data["Time"] = [str(element) for element in all_data["Time"]]
all_data["date"] = [str(element) for element in all_data["date"]]
# creating a column containing the hour of extraction
all_data["hour"] = all_data["date"].str[11:16]

all_00, _, _, _ = exc_time_split(all_data)

# keeping only the columns of interest among all the
# information in rawdata
all_00 = all_00.loc[:, CLEAN_DATA_HEAD]

# selecting the date corresponding to 12:00 AM
# all_data = all_data.loc[all_data["Time"].isin(date_array_str)]

# changing some features in "Pair" field
all_00_clean = exc_pair_cleaning(all_00)

# selecting the crypto-fiat pairs used in the index computation
all_00_clean = all_00_clean.loc[all_00_clean["Pair"].isin(cryptofiat_array)]

# selecting the exchange used in the index computation
all_00_clean = all_00_clean.loc[all_00_clean["Exchange"].isin(EXCHANGES)]

# correcting the "Pair Volume" field
all_00_clean["Close Price"] = [float(element)
                               for element in all_00_clean["Close Price"]]
all_00_clean["Crypto Volume"] = [float(element)
                                 for element in all_00_clean["Crypto Volume"]]

all_00_clean["Pair Volume"] = all_00_clean["Close Price"] * \
    all_00_clean["Crypto Volume"]


# ########## DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix containing the exchange-pair logic values
logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_keys"))

# creating the exchange-pair couples key for the daily matrix
all_00_clean["key"] = all_00_clean["Exchange"] + "&" + all_00_clean["Pair"]

# ########## adding the dead series to the daily values ##################

# selecting only the exchange-pair couples present in the historical series
key_present = logic_key.loc[logic_key.logic_value == 1]
key_present = key_present.drop(columns=["logic_value"])


# selecting the last day of the EXC "historical" series
all_00_clean = all_00_clean.loc[all_00_clean.Time != str(today_TS)]
last_day_with_val = max(all_00_clean.Time)
last_day = all_00_clean.loc[all_00_clean.Time
                            == last_day_with_val]

# applying a left join between the prresent keys matrix and the last_day
# matrix, this operation returns a matrix containing all the keys in
# "key_present" and, if some keys are missing in "all_data" put NaN
merged = pd.merge(key_present, last_day, on="key", how="left")

# selecting only the absent keys
merg_absent = merged.loc[merged["Close Price"].isnull()]


header = CLEAN_DATA_HEAD
header.extend(["key"])

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
    all_00_clean = all_00_clean.append(mat_to_add)

# uploading the cleaned data on MongoDB in the collection EXC_cleandata
all_00_clean = all_00_clean.drop(columns=["key"])
all_00_clean["Time"] = [int(element) for element in all_00_clean["Time"]]

all_00_clean = all_00_clean.loc[all_00_clean.Time != 1587081600]


mongo_upload(all_00_clean, "collection_exc_clean")

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


converted_data = conv_into_usd(DB_NAME, matrix_data, matrix_rate,
                               matrix_rate_stable, CONVERSION_FIAT, STABLE_COIN)

print(converted_data)


converted_data["Time"] = [int(element) for element in converted_data["Time"]]
mongo_upload(converted_data, "collection_exc_final_data")

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
