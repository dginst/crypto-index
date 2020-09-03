# standard library import
import sys
from datetime import datetime, timezone
from typing import Dict

import numpy as np
# third party import
import pandas as pd

from cryptoindex.config import (DAY_IN_SEC, DB_NAME, EXCHANGES, MONGO_DICT,
                                START_DATE)
# local import
from cryptoindex.data_setup import (daily_fix_miss, date_gen,
                                    exc_pair_cleaning, exc_value_cleaning)
from cryptoindex.mongo_setup import (mongo_coll, mongo_indexing, mongo_upload,
                                     query_mongo)

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


# checking if the collection are already updated
q_dict: Dict[str, str] = {}
q_dict = {"Time": str(y_TS)}
daily_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_clean"), q_dict)

if daily_mat != []:

      sys.exit('The exc data are already manipulated')

# ################### fixing the "Pair Volume" information #################

daily_mat = daily_mat[
    ["Pair", "Exchange", "Close Price", "Time", "Crypto Volume", "date"]
]

# selecting the exchange used in the index computation
daily_mat = daily_mat.loc[daily_mat["Exchange"].isin(EXCHANGES)]

# creating a column containing the hour of extraction
daily_mat["date"] = [str(d) for d in daily_mat["date"]]
daily_mat["hour"] = daily_mat["date"].str[11:16]

daily_mat = daily_mat.loc[daily_mat.Time != 0]

daily_mat = exc_pair_cleaning(daily_mat)
daily_mat = exc_value_cleaning(daily_mat)

# ############################################################################
# ################# EXTRACTION HOURS DEFINITION ##############################

# creating 4 different df for each exctraction hour
daily_matrix_00 = daily_mat.loc[daily_mat.hour == "00:00"]
daily_matrix_12 = daily_mat.loc[daily_mat.hour == "12:00"]
daily_matrix_16 = daily_mat.loc[daily_mat.hour == "16:00"]
daily_matrix_20 = daily_mat.loc[daily_mat.hour == "20:00"]

# creating the exchange-pair couples key for the daily matrix
# for each above defined df
daily_matrix_00["key"] = daily_matrix_00["Exchange"] + \
    "&" + daily_matrix_00["Pair"]
daily_matrix_12["key"] = daily_matrix_12["Exchange"] + \
    "&" + daily_matrix_12["Pair"]
daily_matrix_16["key"] = daily_matrix_16["Exchange"] + \
    "&" + daily_matrix_16["Pair"]
daily_matrix_20["key"] = daily_matrix_20["Exchange"] + \
    "&" + daily_matrix_20["Pair"]

# ########### DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

# downloading from MongoDB the matrix with the daily values and the
# matrix containing the exchange-pair logic values
logic_key = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_keys"))

# ########## adding the dead series to the daily values ##################

# selecting only the exchange-pair couples present in the historical series
key_present = logic_key.loc[logic_key.logic_value == 1]
key_present = key_present.drop(columns=["logic_value"])
# applying a left join between the prresent keys matrix and the daily
# matrix, this operation returns a matrix containing all the keys in
# "key_present" and, if some keys are missing in "daily_mat" put NaN
merged = pd.merge(key_present, daily_matrix_00, on="key", how="left")
# assigning some columns values and substituting NaN with 0
# in the "merged" df
merged["Time"] = y_TS
split_val = merged["key"].str.split("&", expand=True)
merged["Exchange"] = split_val[0]
merged["Pair"] = split_val[1]

# define a df containing only the NaN value, so the keys
# that are not present in daily_matrix_00
check_key = merged.loc[merged["Close Price"].isnull(), "key"]

# join the dataframes that contains the keys not present in daily_matrix_00
# and the daily_matrix_12
check_12 = pd.merge(check_key, daily_matrix_12, on="key", how="left")

# isolate the potential non-NaN resulting from the join, the df would
# contain the keys present in the extraction hour 12:00
present_12 = check_12.loc[check_12["Close Price"].notnull()]
# present_12 = check_12.loc[check_12["Close Price"] != "NaN"]

# if the "present_12" df is not empty, then substitute the values for each
# keys in the "merged" df, otherwise pass
if present_12.empty is False:

    for k in present_12["key"]:

        price = present_12.loc[present_12["key"] == k, "Close Price"]
        p_vol = present_12.loc[present_12["key"] == k, "Pair Volume"]
        c_vol = present_12.loc[present_12["key"] == k, "Crypto Volume"]
        merged.loc[merged["key"] == k, "Close Price"] = price
        merged.loc[merged["key"] == k, "Pair Volume"] = p_vol
        merged.loc[merged["key"] == k, "Crypto Volume"] = c_vol
else:
    pass

# giving 0 to all the remainig values
merged.fillna(0, inplace=True)
print(merged)
# ########## checking potential new exchange-pair couple ##################

# define a subset of keys that has not been present in the past
key_absent = logic_key.loc[logic_key.logic_value == 0]
key_absent.drop(columns=["logic_value"])

# merging the two dataframe (left join) in order to find potential
# new keys in the data of the day
merg_absent = pd.merge(key_absent, daily_matrix_00, on="key", how="left")
merg_absent.fillna("NaN", inplace=True)
new_key = merg_absent.loc[merg_absent["Close Price"] != "NaN"]

if new_key.empty is False:

    print("Message: New exchange-pair couple(s) found.")
    new_key_list = new_key["key"]
    print(new_key_list)

    for key in new_key_list:

        # updating the logic matrix of exchange-pair couple
        logic_key.loc[logic_key.key == key, "logic value"] = 1

        # create the historical series of the new couple(s)
        # composed by zeros
        splited_key = key.split("&")
        key_hist_df = pd.DataFrame(date_tot, columns="Time")
        key_hist_df["Close Price"] = 0
        key_hist_df["Pair Volume"] = 0
        key_hist_df["Crypto Volume"] = 0
        key_hist_df["Exchange"] = splited_key[0]
        key_hist_df["Pair"] = splited_key[1]

        # inserting the today value of the new couple(s)
        new_price = new_key.loc[new_key.key == key, "Close Price"]
        new_p_vol = new_key.loc[new_key.key == key, "Pair Volume"]
        new_c_vol = new_key.loc[new_key.key == key, "Crypto Volume"]
        key_hist_df.loc[key_hist_df.Time == y_TS, "Close Price"] = new_price
        key_hist_df.loc[key_hist_df.Time == y_TS, "Pair Volume"] = new_p_vol
        key_hist_df.loc[key_hist_df.Time == y_TS, "Crypto Volume"] = new_c_vol

        # upload the new artificial historical series on MongoDB
        # collection "EXC_cleandata"
        mongo_upload(key_hist_df, collection_dict_upload.get(
            "collection_exc_clean"))

else:
    pass

# ###########################################################################
# ######################## MISSING DATA FIXING ##############################

q_dict_str: Dict[str, str] = {}
q_dict_str = {"Time": str(two_before_TS)}

# downloading from MongoDB the matrix referring to the previuos day
day_bfr_mat = query_mongo(DB_NAME, MONGO_DICT.get("coll_exc_clean"), q_dict_str)

# add the "key" column
day_bfr_mat["key"] = day_bfr_mat["Exchange"] + "&" + day_bfr_mat["Pair"]

# looping through all the daily keys looking for potential missing value
for key_val in day_bfr_mat["key"]:

    new_val = merged.loc[merged.key == key_val]
    # if the new 'Close Price' referred to a certain key is 0 the script
    # check the previous day value: if is == 0 then pass, if is != 0
    # the values related to the selected key needs to be corrected
    # ###### IF A EXC-PAIR DIES AT A CERTAIN POINT, THE SCRIPT
    # CHANGES THE VALUES. MIGHT BE WRONG #######################
    if np.array(new_val["Close Price"]) == 0.0:

        d_before_val = day_bfr_mat.loc[day_bfr_mat.key == key_val]

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
merged = merged.drop(columns=["key", "date", "hour"])
merged['Time'] = [str(t) for t in merged['Time']]
print(merged)
mongo_upload(merged, "collection_exc_clean")
