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

hour_in_sec = 3600
day_in_sec = 86400
start_period = "01-01-2016"

# set today
today = datetime.now().strftime("%Y-%m-%d")
today_TS = int(datetime.strptime(
    today, "%Y-%m-%d").timestamp()) + hour_in_sec * 2
y_TS = today_TS - day_in_sec
two_before_TS = y_TS - day_in_sec
print(y_TS)
# defining the array containing all the date from start_period until today
date_complete_int = data_setup.date_gen(start_period)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_complete_int]

# #################### setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# naming the existing collections as a variable
collection_clean = db.EXC_cleandata

# defining the database name and the collection name where to look for data
coll_EXC_raw = "EXC_rawdata"
coll_clean = "EXC_cleandata"
coll_CW_raw = "CW_rawdata"

# ################### fixing the "Pair Volume" information #################

db = "index"
coll_EXC_raw = "EXC_rawdata"
coll_EXC_raw = "EXC_test"
q_dict: Dict[str, str] = {}
q_dict = {"Time": str(y_TS)}

daily_mat = mongo.query_mongo(db, coll_EXC_raw, q_dict)
daily_mat = daily_mat[
    ["Pair", "Exchange", "Close Price", "Time", "Crypto Volume", "date"]
]

# selecting the exchange used in the index computation
daily_mat = daily_mat.loc[daily_mat["Exchange"].isin(Exchanges)]

# creating a column containing the hour of extraction
daily_mat["date"] = [str(d) for d in daily_mat["date"]]
daily_mat["hour"] = daily_mat["date"].str[11:16]

daily_mat = daily_mat.loc[daily_mat.Time != 0]

# changing some features in "Pair" field in order to homogeneize
daily_mat["Pair"] = [
    element.replace("USDT_BCHSV", "bsvusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_BCHSV", "bsvusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_BCHABC", "bchusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_BCHABC", "bchusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_LTC", "ltcusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_LTC", "ltcusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_XRP", "xrpusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_XRP", "xrpusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_ZEC", "zecusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_ZEC", "zecusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_EOS", "eosusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_EOS", "eosusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_ETC", "etcusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_ETC", "etcusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_STR", "xlmusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_STR", "xlmusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_BTC", "btcusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_BTC", "btcusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDC_ETH", "ethusdc") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [
    element.replace("USDT_ETH", "ethusdt") for element in daily_mat["Pair"]
]
daily_mat["Pair"] = [element.lower() for element in daily_mat["Pair"]]
daily_mat["Pair"] = [element.replace("xbt", "btc")
                     for element in daily_mat["Pair"]]

daily_mat["Crypto Volume"] = [float(v) for v in daily_mat["Crypto Volume"]]
daily_mat["Close Price"] = [float(p) for p in daily_mat["Close Price"]]
daily_mat["Pair Volume"] = daily_mat["Close Price"] * daily_mat["Crypto Volume"]

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

collect_log_key = "EXC_keys"
# q_dict: Dict[str, int] = {}
# q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix with the daily values and the
# matrix containing the exchange-pair logic values
logic_key = mongo.query_mongo(db, collect_log_key)

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
        data = key_hist_df.to_dict(orient="records")
        collection_clean.insert_many(data)

else:
    pass

# ###########################################################################
# ######################## MISSING DATA FIXING ##############################

database = "index"
coll_clean = "EXC_cleandata"
q_dict_str: Dict[str, str] = {}
q_dict_str = {"Time": str(two_before_TS)}

# downloading from MongoDB the matrix referring to the previuos day
day_bfr_mat = mongo.query_mongo(db, coll_clean, q_dict_str)

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
merged = merged.drop(columns=["key", "date", "hour"])
merged['Time'] = [str(t) for t in merged['Time']]
print(merged)
data = merged.to_dict(orient="records")
collection_clean.insert_many(data)
