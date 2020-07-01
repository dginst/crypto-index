# ############################################################################
# The script is divided in three main parts.
# 1) USDT and USDC daily rates computation
# using the weighted average value of btc/usd rates (computed using all the
# avalilable exchanges) and the average btc/usdc and btc/usdc rates, the script
# computes USDT/USD and USDC/USD rate. The daily value of rates is
# uploaded on MongoDB in the collection "stable_coin_rates"
# 2) non-USD values conversion into USD
# The file retrieves data from MongoDB collection "CW_cleandata" the values
# related to the date of interest (current day - 1) and, for each Crypto-Fiat
# converts the data into USD values using the ECB rates stored on MongoDB
# in the collection "ecb_clean" and th# computed stablecoin rates saved in
# the "stable_coin_rates" collection. Once everything has been converted,
# the script upload the dataframe on MongoDB collection "converted_data".
# 3) Uploading the converted data of the day in "CW_final_data" collection
# ###############################################################################

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


# ############# initial settings #############################

start_date = "01-01-2016"

# define today date as timestamp
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - 86400
two_before_TS = y_TS - 86400

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.date_gen(start_date)

# pair arrat without USD (no need of conversion)
pair_array = ["usd", "gbp", "eur", "cad", "jpy", "usdt", "usdc"]
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur']
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
# crypto complete ['BTC', 'ETH', 'XRP', 'LTC', 'BCH', 'EOS',
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
# exchange complete = ['coinbase-pro', 'poloniex',
#  'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

# ################ setup MongoDB connection ################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# creating the empty collection cleandata within the database index
collection_stable = db.stable_coin_rates
collection_final_data = db.CW_final_data
collection_converted = db.CW_converted_data
collection_key = db.CW_keys

# ########## USDC/USD and USDT/USD computation #####################

start = time.time()

# MongoDB index and collection names definition
database = "index"
collection = "CW_cleandata"

# taking BTC/USD pair value related to the date of interest
# ## should be add a try/except TypeError in order to manage
# the potential missing of kraken btcusd ##############
first_query = {"Pair": "btcusd", "Exchange": "kraken", "Time": str(y_TS)}
first_call = mongo.query_mongo(database, collection, first_query)


price_df = first_call[["Close Price"]]
volume_df = first_call[["Pair Volume"]]
price_df = price_df.rename(columns={"Close Price": "kraken"})
volume_df = volume_df.rename(columns={"Pair Volume": "kraken"})
Exchanges.remove("kraken")


for exchange in Exchanges:

    query = {"Pair": "btcusd", "Exchange": exchange, "Time": str(y_TS)}
    single_ex = mongo.query_mongo(database, collection, query)

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


# ############# USDT exchange rates computation ##########
# BTC/USDT is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historoical values from 01/01/2016

# POLONIEX usdt/usd exchange rate
query_usdt = {"Exchange": "poloniex", "Pair": "btcusdt", "Time": str(y_TS)}
usdt_poloniex = mongo.query_mongo(database, collection, query_usdt)

# KRAKEN usdt/usd exchange rate
query_usdt = {"Exchange": "kraken", "Pair": "btcusdt", "Time": str(y_TS)}
usdt_kraken = mongo.query_mongo(database, collection, query_usdt)

# BITTREX usdt/usd exchange rate
query_usdt = {"Exchange": "bittrex", "Pair": "btcusdt", "Time": str(y_TS)}
usdt_bittrex = mongo.query_mongo(database, collection, query_usdt)

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
usdt_rates["Standard Date"] = data_setup.timestamp_to_human(first_call["Time"])

print(usdt_rates)
# USDT mongoDB upload
usdt_data = usdt_rates.to_dict(orient="records")
collection_stable.insert_many(usdt_data)


# ############# USDC exchange rates computation ############
# BTC/USDC is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historoical values from 01/01/2016

# POLONIEX usdc/usd exchange rate
query_usdc = {"Exchange": "poloniex", "Pair": "btcusdc", "Time": str(y_TS)}
usdc_poloniex = mongo.query_mongo(database, collection, query_usdc)

# KRAKEN usdc/usd exchange rate
query_usdc = {"Exchange": "kraken", "Pair": "btcusdc", "Time": str(y_TS)}
usdc_kraken = mongo.query_mongo(database, collection, query_usdc)

# COINBASE_PRO usdc exchange rate
query_usdc_coinbase = {
    "Exchange": "coinbase-pro",
    "Pair": "btcusdc",
    "Time": str(y_TS),
}
usdc_coinbase = mongo.query_mongo(database, collection, query_usdc_coinbase)

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
usdc_rates["Standard Date"] = data_setup.timestamp_to_human(first_call["Time"])

print(usdc_rates)
# USDC mongoDB upload
usdc_data = usdc_rates.to_dict(orient="records")
collection_stable.insert_many(usdc_data)

# ########################################################################
# ################# defining the time vector for the check ###############

# defining the array containing all the date from start_period until today
date_tot = data_setup.date_gen(start_date)
# converting the timestamp format date into string
date_tot = [str(single_date) for single_date in date_tot]
# searching only the last five days
last_five_days = date_tot[(len(date_tot) - 5): len(date_tot)]

# ################# DAILY DATA CONVERSION MAIN PART ##################

# defining the database name and the collection name
db = "index"
collection_data = "CW_cleandata"
collection_rates = "ecb_clean"
collection_stable = "stable_coin_rates"

# querying the data from mongo
query_data = {"Time": str(y_TS)}
query_rate = {"Date": str(y_TS)}
query_stable = {"Time": str(y_TS)}
matrix_rate = mongo.query_mongo(db, collection_rates, query_rate)
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_data = mongo.query_mongo(db, collection_data, query_data)
matrix_rate_stable = mongo.query_mongo(db, collection_stable, query_stable)

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
data = converted_data.to_dict(orient="records")
collection_converted.insert_many(data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))


# ################ "CW_final_data" collection check ##########################

# define collections where to look up
collection_final = "CW_final_data"

# defining the MongoDB path where to look for the rates
query_dict = {"Exchange": "coinbase-pro", "Pair": "ethusd"}

# retrieving data from MongoDB 'index' and "CW_final_data" collection
matrix = mongo.query_mongo(database, collection_final, query_dict)

# checking the time column
date_list = np.array(matrix["Time"])
last_five_days_mongo = date_list[(len(date_list) - 5): len(date_list)]

# finding the date to download as difference between complete array of date and
# date now stored on MongoDB
date_to_convert = data_setup.Diff(last_five_days, last_five_days_mongo)

# ################### CW_final_data upload ##################################

collection_converted = "CW_converted_data"

if date_to_convert != []:

    for date in date_to_convert:

        query_dict = {"Time": str(date)}

        # retriving the needed information on MongoDB
        matrix = mongo.query_mongo(database, collection_converted, query_dict)

        # put the manipulated data on MongoDB
        data = matrix.to_dict(orient="records")
        collection_final_data.insert_many(data)

else:

    print(
        "Message: No new date to upload on on Mongo DB, the CW_final_data \
        collection on MongoDB is updated."
    )
