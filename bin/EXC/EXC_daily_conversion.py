# standard library import
import time
from datetime import datetime

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
Start_Period = "01-01-2016"

# set today
today = datetime.now().strftime("%Y-%m-%d")
today_TS = int(datetime.strptime(today, "%Y-%m-%d").timestamp()) + hour_in_sec * 2
yesterday_TS = today_TS - day_in_sec
two_before_TS = yesterday_TS - day_in_sec

# defining the array containing all the date from start_period until today
date_complete_int = data_setup.timestamp_gen(Start_Period)
# converting the timestamp format date into string
date_complete = [str(single_date) for single_date in date_complete_int]

# #################### setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# naming the existing collections as a variable
collection_final = db.EXC_final_data


# ################# DAILY DATA CONVERSION MAIN PART ##################

# defining the database name and the collection name
db = "index"
collection_data = "EXC_cleandata"
collection_rates = "ecb_clean"
collection_stable = "stable_coin_rates"

# querying the data from mongo
query_data = {"Time": str(yesterday_TS)}
query_rate = {"Date": str(yesterday_TS)}
query_stable = {"Time": str(yesterday_TS)}
matrix_rate = mongo.query_mongo(db, collection_rates, query_rate)
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_data = mongo.query_mongo(db, collection_data, query_data)
matrix_rate_stable = mongo.query_mongo(db, collection_stable, query_stable)

# creating a column containing the fiat currency
matrix_rate["fiat"] = [x[:3].lower() for x in matrix_rate["Currency"]]
matrix_data["fiat"] = [x[3:].lower() for x in matrix_data["Pair"]]
matrix_rate_stable["fiat"] = [x[:4].lower() for x in matrix_rate_stable["Currency"]]

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
stable_merged = pd.merge(stablecoin_matrix, matrix_rate_stable, on=["Time", "fiat"])

# converting the prices in usd
stable_merged["Close Price"] = stable_merged["Close Price"] / stable_merged["Rate"]
stable_merged["Close Price"] = stable_merged["Close Price"].replace(
    [np.inf, -np.inf], np.nan
)
stable_merged["Close Price"].fillna(0, inplace=True)
stable_merged["Pair Volume"] = stable_merged["Pair Volume"] / stable_merged["Rate"]
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
data = converted_data.to_dict(orient="records")
collection_final.insert_many(data)
