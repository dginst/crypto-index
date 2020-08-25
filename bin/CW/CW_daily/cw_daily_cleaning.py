# #############################################################################
# The script manages the daily raw data downloaded from CryptoWatch and upload
# the fixed values on "CW_cleandata" collection on MongoDB
# 1) Computing the "Pair Volume" information
# the script artificially computes the "Pair Volume" information multiplying
# the "Close Price" and the "Crypto Volume"; the data is uploaded on the
# MongoDB collection 'volume_checked_data'
# 2) Daily data management
# The main rules for the manipulation of raw data are the followings:
# - check if the daily downloaded data contains all the crypto-fiat couples
#   that are present in the historical series; the check is performed using
#   the collection "exchange_pair_key" that contains a list of all possible
#   keys (formed by 'exchange' + '&' + 'cryptofiat') and a related logic value
#   (1 if the historical series exists, 0 if not):
#   - DEAD KEYS --> the script may find that one or more existing keys
#     (logic value 1) are not present un the daily downloaded data;
#     the reason may be that the the key is no longer traded and, for the
#     sake of equal matrix dimension, an artificial dataframe with 0 values
#     is uploaded on MongoDB;
#   - NEW KEYS --> the script may find that the downloaded data presents one
#     or more keys that have not been traded in the past (logic value 0);
#     for all the new keys the collection "exchange_pair_key" is updated
#     (logic value from 0 to 1) and is creted an artificial historical
#     series with 0 values from 01-01-2016 to "yesterday"
# - compare the previous day value for all the keys in which the script put
#   0 in order to manage a potential dead series:
#   - if the previous day "Close Price" is 0, then nothing changes
#   - if the previous day "Close Price" is != 0 then the script computes
#     the weighted average variation of the same crypto-fiat pair on others
#     exchanges and apply the computed variation to the previous day "Close
#     Price" obtaining an artificial "Close Price";
#    - N.B. Pair Volume ad Crypto Volume are not fixed
# Once the data has been managed the script put the daily data on MongoDB
# in the collection "CW_cleandata"
# ############################################################################

# standard library import
from datetime import datetime, timezone
from typing import Dict

# third party import
import pandas as pd
import numpy as np

# local import
from cryptoindex.data_setup import (
    date_gen, Diff, timestamp_to_human, daily_fix_miss)
# import cryptoindex.mongo_setup as mongo
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing, query_mongo, mongo_upload)
from cryptoindex.config import (
    START_DATE, MONGO_DICT, DAY_IN_SEC,
    PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, DB_NAME)


# ########## MongoDB setup ################################

# create the indexing for MongoDB and define the variable containing the
# MongoDB collections where to upload data
mongo_indexing()
collection_dict_upload = mongo_coll()


myquery = {'Time': 1597881600}
my = {'Time': "1597881600"}
collection_dict_upload.get("collection_cw_vol_check").delete_many(myquery)
collection_dict_upload.get("collection_cw_clean").delete_many(my)

# ############################ missing days check #############################

# this section allows to check if CW_clean data contains the new values of the
# day, the check is based on a 5-days period and allows

# assign date of interest to variables
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - DAY_IN_SEC
two_before_TS = y_TS - DAY_IN_SEC

# defining the array containing all the date from start_period until today
date_complete_int = date_gen(START_DATE)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_complete_int]

# searching only the last five days
last_five_days = date_tot[(len(date_tot) - 5): len(date_tot)]

# defining the details to query on MongoDB
query = {"Exchange": "coinbase-pro", "Pair": "ethusd"}

# retrieving the wanted data on MongoDB collection
matrix = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_clean"), query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_add = Diff(last_five_days, last_five_days_mongo)
print(date_to_add)

if date_to_add != []:

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = timestamp_to_human(
            [date_to_add[0]], date_format="%m-%d-%Y"
        )
        start_date = start_date[0]
        end_date = timestamp_to_human(
            [date_to_add[len(date_to_add) - 1]], date_format="%m-%d-%Y"
        )
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime("%m-%d-%Y")
        end_date = start_date

    rel_ref_vector = date_gen(start_date, end_date, EoD="N")

    # creating a date array of support that allows to manage the one-day
    # missing data
    if start_date == end_date:

        day_before = int(rel_ref_vector[0]) - DAY_IN_SEC
        sup_date_array = np.array([day_before])
        sup_date_array = np.append(sup_date_array, int(rel_ref_vector[0]))


# ################### fixing the "Pair Volume" information #################

# defining the query details
q_dict: Dict[str, int] = {}
q_dict = {"Time": y_TS}

# querying oin MongoDB collection
daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_raw"), q_dict)
daily_mat = daily_mat.loc[daily_mat.Time != 0]
daily_mat = daily_mat.drop(columns=["Low", "High", "Open"])

for Crypto in CRYPTO_ASSET:

    ccy_pair_array = []

    for i in PAIR_ARRAY:

        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in EXCHANGES:

        for cp in ccy_pair_array:

            mat = daily_mat.loc[daily_mat["Exchange"] == exchange]
            mat = mat.loc[mat["Pair"] == cp]
            # checking if the matrix is not empty
            if mat.shape[0] > 1:

                mat["Pair Volume"] = mat["Close Price"] * mat["Crypto Volume"]

            # put the manipulated data on MongoDB
            try:

                mongo_upload(mat, "collection_cw_vol_check")

            except TypeError:
                pass

# ############################################################################
# ########### DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

# defining the query details
q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix with the daily values and the
# matrix containing the exchange-pair logic values
daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_vol_chk"), q_dict)
logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_keys"))

# creating the exchange-pair couples key for the daily matrix
daily_mat["key"] = daily_mat["Exchange"] + "&" + daily_mat["Pair"]

# ########## adding the dead series to the daily values ##################

# selecting only the exchange-pair couples present in the historical series
key_present = logic_key.loc[logic_key.logic_value == 1]
key_present = key_present.drop(columns=["logic_value"])
# applying a left join between the prresent keys matrix and the daily
# matrix, this operation returns a matrix containing all the keys in
# "key_present" and, if some keys are missing in "daily_mat" put NaN
merged = pd.merge(key_present, daily_mat, on="key", how="left")
# assigning some columns values and substituting NaN with 0
# in the "merged" df
merged["Time"] = y_TS
split_val = merged["key"].str.split("&", expand=True)
merged["Exchange"] = split_val[0]
merged["Pair"] = split_val[1]
merged.fillna(0, inplace=True)
print(daily_mat)
print(merged)

# ########## checking potential new exchange-pair couple ##################

key_absent = logic_key.loc[logic_key.logic_value == 0]
key_absent.drop(columns=["logic_value"])

merg_absent = pd.merge(key_absent, daily_mat, on="key", how="left")
merg_absent.fillna("NaN", inplace=True)
new_key = merg_absent.loc[merg_absent["Close Price"] != "NaN"]

if new_key.empty is False:

    print("Message: New exchange-pair couple(s) found.")
    new_key_list = new_key["key"]
    print("new keys list")
    print(new_key_list)

    for key in new_key_list:

        q_del = {"key": key}
        collection_dict_upload.get("collection_CW_key").delete_many(q_del)

        # updating the logic matrix of exchange-pair couple
        new_key_log = pd.DataFrame(np.column_stack(
            (key, int(1))), columns=["key", "logic_value"])
        new_key_log["logic_value"] = 1

        # create the historical series of the new couple(s)
        # composed by zeros
        splited_key = key.split("&")
        key_hist_df = pd.DataFrame(date_tot, columns=["Time"])
        key_hist_df["Close Price"] = 0
        key_hist_df["Pair Volume"] = 0
        key_hist_df["Crypto Volume"] = 0
        key_hist_df["Exchange"] = splited_key[0]
        key_hist_df["Pair"] = splited_key[1]

        # uploading on MongoDB collections "CW_converted_data" and "CW_final_data"
        # the new series of zero except for the last value (yesterday)
        mongo_upload(key_hist_df, "collection_cw_converted")
        mongo_upload(key_hist_df, "collection_cw_final_data")

        query_for_yst = {"Time": str(y_TS)}
        collection_dict_upload.get(
            "collection_cw_converted").delete_many(query_for_yst)
        collection_dict_upload.get(
            "collection_cw_final_data").delete_many(query_for_yst)

        # inserting the today value of the new couple(s)
        new_price = np.array(new_key.loc[new_key.key == key, "Close Price"])
        new_p_vol = np.array(new_key.loc[new_key.key == key, "Pair Volume"])
        new_c_vol = np.array(new_key.loc[new_key.key == key, "Crypto Volume"])
        key_hist_df.loc[key_hist_df.Time == str(
            y_TS), "Close Price"] = new_price
        key_hist_df.loc[key_hist_df.Time == str(
            y_TS), "Pair Volume"] = new_p_vol
        key_hist_df.loc[key_hist_df.Time == str(
            y_TS), "Crypto Volume"] = new_c_vol

        # upload the dataframe on MongoDB collection "CW_cleandata"
        mongo_upload(key_hist_df, "collection_cw_clean")

        # uploading the updated keys df on the CW_keys collection
        mongo_upload(new_key_log, "collection_CW_key")

else:
    pass

# ###########################################################################
# ######################## MISSING DATA FIXING ##############################

# defining the query details
q_dict_time: Dict[str, str] = {}
q_dict_time = {"Time": str(two_before_TS)}

# downloading from MongoDB the matrix referring to the previuos day
day_bfr_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_cw_clean"), q_dict_time)

# add the "key" column
day_bfr_mat["key"] = day_bfr_mat["Exchange"] + "&" + day_bfr_mat["Pair"]
print(day_bfr_mat)
# looping through all the daily keys looking for potential missing value
for key_val in day_bfr_mat["key"]:

    print(key_val)
    new_val = merged.loc[merged.key == key_val]
    print(new_val)
    # if the new 'Close Price' referred to a certain key is 0 the script
    # check the previous day value: if is == 0 then pass, if is != 0
    # the values related to the selected key needs to be corrected
    # ###### IF A EXC-PAIR DIES AT A CERTAIN POINT, THE SCRIPT
    # CHANGES THE VALUES. MIGHT BE WRONG #######################
    if np.array(new_val["Close Price"]) == 0.0:

        d_before_val = day_bfr_mat.loc[day_bfr_mat.key == key_val]
        print(d_before_val)

        if np.array(d_before_val["Close Price"]) != 0.0:

            price_var = daily_fix_miss(new_val, merged, day_bfr_mat)
            # applying the weighted variation to the day before 'Close Price'
            new_price = (1 + price_var) * d_before_val["Close Price"]
            # changing the 'Close Price' value using the new computed price
            merged.loc[merged.key == key_val, "Close Price"] = new_price

        else:
            pass

    else:
        pass


# put the manipulated data on MongoDB
merged.drop(columns=["key"])
merged["Time"] = [str(d) for d in merged["Time"]]
mongo_upload(merged, "collection_cw_clean")
