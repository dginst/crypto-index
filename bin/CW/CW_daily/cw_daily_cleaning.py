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
from datetime import datetime
from typing import Dict

# third party import
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.mongo_setup as mongo


# ############# INITIAL SETTINGS ################################

pair_array = ["gbp", "usd", "cad", "jpy", "eur", "usdt", "usdc"]
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']
Crypto_Asset = [
    "BTC",
    "ETH",
    "XRP",
    "LTC",
    "BCH",
    "EOS",
    "ETC",
    "ZEC",
    "ADA",
    "XLM",
    "XMR",
    "BSV",
]
# crypto complete [ 'BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'EOS',
# 'ETC', 'ZEC', 'ADA', 'XLM', 'XMR', 'BSV']
Exchanges = [
    "coinbase-pro",
    "poloniex",
    "bitstamp",
    "gemini",
    "bittrex",
    "kraken",
    "bitflyer",
]
# exchange complete = [ 'coinbase-pro', 'poloniex', 'bitstamp',
# 'gemini', 'bittrex', 'kraken', 'bitflyer']

# #################### setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# naming the existing collections as a variable
collection_clean = db.CW_cleandata
collect_vol = db.CW_volume_checked_data
collection_CW_k = db.CW_keys


# defining the database name and the collection name where to look for data
database = "index"
collection_raw = "CW_rawdata"
collection_clean_check = "CW_cleandata"
collect_vol_chk = "CW_volume_checked_data"

##

myquery = {'Time': 1591833600}
collect_vol.delete_many(myquery)
##
# ############################ missing days check #############################

# this section allows to check if CW_clean data contains the new values of the
# day, the check is based on a 5-days period and allows

hour_in_sec = 3600
day_in_sec = 86400
start_period = "01-01-2016"

# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - day_in_sec
two_before_TS = y_TS - day_in_sec

# defining the array containing all the date from start_period until today
date_complete_int = data_setup.date_gen(start_period)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_complete_int]

# searching only the last five days
last_five_days = date_tot[(len(date_tot) - 5): len(date_tot)]

# defining the MongoDB path where to look for the rates
query = {"Exchange": "coinbase-pro", "Pair": "ethusd"}

# retrieving data from MongoDB 'index' and 'ecb_raw' collection
matrix = mongo.query_mongo(database, collection_clean_check, query)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_add = data_setup.Diff(last_five_days, last_five_days_mongo)
print(date_to_add)

if date_to_add != []:

    if len(date_to_add) > 1:

        date_to_add.sort()
        start_date = data_setup.timestamp_to_human(
            [date_to_add[0]], date_format="%m-%d-%Y"
        )
        start_date = start_date[0]
        end_date = data_setup.timestamp_to_human(
            [date_to_add[len(date_to_add) - 1]], date_format="%m-%d-%Y"
        )
        end_date = end_date[0]

    else:

        start_date = datetime.fromtimestamp(int(date_to_add[0]))
        start_date = start_date.strftime("%m-%d-%Y")
        end_date = start_date

    rel_ref_vector = data_setup.date_gen(start_date, end_date, EoD="N")

    # creating a date array of support that allows to manage the one-day
    # missing data
    if start_date == end_date:

        day_before = int(rel_ref_vector[0]) - 86400
        sup_date_array = np.array([day_before])
        sup_date_array = np.append(sup_date_array, int(rel_ref_vector[0]))


# ################### fixing the "Pair Volume" information #################

db = "index"
collection_raw = "CW_rawdata"
q_dict: Dict[str, int] = {}
q_dict = {"Time": y_TS}

daily_mat = mongo.query_mongo(db, collection_raw, q_dict)
daily_mat = daily_mat.loc[daily_mat.Time != 0]
daily_mat = daily_mat.drop(columns=["Low", "High", "Open"])

for Crypto in Crypto_Asset:

    ccy_pair_array = []

    for i in pair_array:

        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        for cp in ccy_pair_array:

            mat = daily_mat.loc[daily_mat["Exchange"] == exchange]
            mat = mat.loc[mat["Pair"] == cp]
            # checking if the matrix is not empty
            if mat.shape[0] > 1:

                mat["Pair Volume"] = mat["Close Price"] * mat["Crypto Volume"]

            # put the manipulated data on MongoDB
            try:

                data = mat.to_dict(orient="records")
                collect_vol.insert_many(data)

            except TypeError:
                pass

# ############################################################################
# ########### DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################

collect_vol_chk = "CW_volume_checked_data"
collect_log_key = "CW_keys"
q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix with the daily values and the
# matrix containing the exchange-pair logic values
daily_mat = mongo.query_mongo(db, collect_vol_chk, q_dict)
logic_key = mongo.query_mongo(db, collect_log_key)

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

        # upload the dataframe on MongoDB collection "CW_cleandata"
        data = key_hist_df.to_dict(orient="records")
        collection_clean.insert_many(data)

    # uploading the updated keys df on the CW_keys collection
    new_k_logic = logic_key.to_dict(orient="records")
    collection_CW_k.insert_many(new_k_logic)

else:
    pass

# ###########################################################################
# ######################## MISSING DATA FIXING ##############################

database = "index"
collection_clean_check = "CW_cleandata"
q_dict_time: Dict[str, str] = {}
q_dict_time = {"Time": str(two_before_TS)}


# downloading from MongoDB the matrix referring to the previuos day
day_bfr_mat = mongo.query_mongo(db, collection_clean_check, q_dict_time)

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

            print(new_val)
            print(d_before_val)

            price_var = data_setup.daily_fix_miss(new_val, merged, day_bfr_mat)
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
data = merged.to_dict(orient="records")
collection_clean.insert_many(data)
