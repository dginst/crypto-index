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
    date_gen, exc_pair_cleaning,
    exc_pair_cleaning, homogenize_series
)
from cryptoindex.mongo_setup import (
    query_mongo, mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload, df_reorder)
from cryptoindex.config import (
    EXC_START_DATE, DAY_IN_SEC, MONGO_DICT,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
    DB_NAME, CLEAN_DATA_HEAD, STABLE_COIN,
    CONVERSION_FIAT
)
from cryptoindex.cw_hist_func import (
    crypto_fiat_pair_gen
)
from cryptoindex.exc_func import (
    exc_time_split, exc_daily_cleaning
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

# fixing 1598486400 day
prev_day = 1598486400 - 86400
prev_day_mat = all_00_clean.loc[all_00_clean["Time"] == str(prev_day)]
prev_day_new_time = prev_day_mat

prev_day_new_time["Time"] = str(1598486400)

all_00_clean = all_00_clean.loc[all_00_clean.Time != str(1598486400)]
all_00_clean = all_00_clean.append(prev_day_new_time)

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

# homogeneize
new_clean = pd.DataFrame(columns=CLEAN_DATA_HEAD)

for crypto in CRYPTO_ASSET:

    pair_arr = crypto_fiat_pair_gen(crypto)

    for exchange in EXCHANGES:
        print(exchange)
        ex_matrix = all_00_clean.loc[all_00_clean.Exchange == exchange]

        for cp in pair_arr:
            print(cp)
            crypto = cp[:3]

            cp_matrix = ex_matrix.loc[ex_matrix["Pair"] == cp]
            cp_matrix = cp_matrix.drop(columns=["Exchange", "Pair"])
            # checking if the matrix is not empty
            if cp_matrix.shape[0] > 1:

                # check if the historical series start at the same date as
                # the start date if not fill the dataframe with zero values
                cp_matrix = homogenize_series(
                    cp_matrix, date_array)

                if cp_matrix.shape[0] != len(date_array_str):

                    if exchange == "poloniex":
                        print(cp_matrix)
                    print("fixing")
                    date_df = pd.DataFrame(columns=["Time"])
                    date_df["Time"] = np.array(date_array)
                    merged_cp = pd.merge(
                        date_df, cp_matrix, on="Time", how="left")
                    if exchange == "poloniex":
                        print(merged_cp)
                    merged_cp.fillna("NaN", inplace=True)
                    nan_list = list(
                        merged_cp.loc[merged_cp["Close Price"] == "NaN", "Time"])
                    for nan in nan_list:

                        prev_price = merged_cp.loc[merged_cp.Time
                                                   == nan - 86400, "Close Price"]
                        prev_p_vol = merged_cp.loc[merged_cp.Time
                                                   == nan - 86400, "Pair Volume"]
                        prev_c_vol = merged_cp.loc[merged_cp.Time
                                                   == nan - 86400, "Crypto Volume"]
                        merged_cp.loc[merged_cp.Time
                                      == nan, "Close Price"] = prev_price
                        merged_cp.loc[merged_cp.Time
                                      == nan, "Pair Volume"] = prev_p_vol
                        merged_cp.loc[merged_cp.Time
                                      == nan, "Crypto Volume"] = prev_c_vol

                    cp_matrix = merged_cp
                    print(cp_matrix.shape)

            cp_matrix["Exchange"] = exchange
            cp_matrix["Pair"] = cp

            new_clean = new_clean.append(cp_matrix)

new_clean = new_clean.drop(columns=["key"])
# ##########

mongo_upload(new_clean, "collection_exc_clean")

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
