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


# ########################## initial settings #################################

# set start_period that has to correspond to the first day of exc data history
start_period = "04-17-2020"

hour_in_sec = 3600
day_in_sec = 86400

# set today
today = datetime.now().strftime("%Y-%m-%d")
today_TS = int(datetime.strptime(
    today, "%Y-%m-%d").timestamp()) + hour_in_sec * 2
y_TS = today_TS - day_in_sec

# creating the timestamp array at 12:00 AM
date_array = data_setup.date_gen(start_period)
date_array_str = [str(el) for el in date_array]

# pair arrat without USD (no need of conversion)
pair_array = ["usd", "gbp", "eur", "cad", "jpy", "usdt", "usdc"]
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur']
Crypto_Asset = [
    "ETH",
    "BTC",
    "LTC",
    "BCH",
    "XRP",
    "XLM",
    "ADA",
    "ZEC",
    "XMR",
    "EOS",
    "BSV",
    "ETC",
]
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA',
#  'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [
    "coinbase-pro",
    "poloniex",
    "bitstamp",
    "gemini",
    "bittrex",
    "kraken",
    "bitflyer",
]
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp',
#  'gemini', 'bittrex', 'kraken', 'bitflyer']

# defining the crypto-fiat pairs array
cryptofiat_array = []

for crypto in Crypto_Asset:

    for fiat in pair_array:

        cryptofiat_array.append(crypto.lower() + fiat)

# ############################ setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
db = connection.index

# creating the new EXC_clean collection
db.EXC_cleandata.create_index([("id", -1)])
db.EXC_final_data.create_index([("id", -1)])

# dropping pre-existing collections
db.EXC_final_data.drop()
db.EXC_cleandata.drop()

# naming the existing collections as a variable
collection_clean = db.EXC_cleandata
collection_final = db.EXC_final_data

# defining the database name and the collection name where to look for data
# if is needed a data correction

collection_CW_raw = "CW_rawdata"

# ################### creation of EXC_cleandata collection ##################

database = "index"
collection_raw = "EXC_test"

# querying all raw data from EXC_rawdata
all_data = mongo.query_mongo(database, collection_raw)

# defining the columns on interest
head = ["Pair", "Exchange", "Time",
        "Close Price", "Crypto Volume", "Pair Volume"]

# keeping only the columns of interest among all the
# information in rawdata
all_data = all_data.loc[:, head]

# changing the "Time" values format from integer to string
all_data["Time"] = [str(element) for element in all_data["Time"]]

# creating a column containing the hour of extraction
# all_data["hour"] = all_data["date"].str[11:16]

# selecting the date corresponding to 12:00 AM
# all_data = all_data.loc[all_data.hour == "00:00"]
all_data = all_data.loc[all_data["Time"].isin(date_array_str)]

# changing some features in "Pair" field
all_data["Pair"] = [
    element.replace("USDT_BCHSV", "bsvusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_BCHSV", "bsvusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_BCHABC", "bchusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_BCHABC", "bchusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_LTC", "ltcusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_LTC", "ltcusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_XRP", "xrpusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_XRP", "xrpusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_ZEC", "zecusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_ZEC", "zecusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_EOS", "eosusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_EOS", "eosusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_ETC", "etcusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_ETC", "etcusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_STR", "xlmusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_STR", "xlmusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_BTC", "btcusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_BTC", "btcusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDC_ETH", "ethusdc") for element in all_data["Pair"]
]
all_data["Pair"] = [
    element.replace("USDT_ETH", "ethusdt") for element in all_data["Pair"]
]
all_data["Pair"] = [element.lower() for element in all_data["Pair"]]
all_data["Pair"] = [element.replace("xbt", "btc")
                    for element in all_data["Pair"]]


# selecting the crypto-fiat pairs used in the index computation
all_data = all_data.loc[all_data["Pair"].isin(cryptofiat_array)]

# selecting the exchange used in the index computation
all_data = all_data.loc[all_data["Exchange"].isin(Exchanges)]

# correcting the "Pair Volume" field
all_data["Crypto Volume"] = [float(v) for v in all_data["Crypto Volume"]]
all_data["Close Price"] = [float(p) for p in all_data["Close Price"]]
all_data["Pair Volume"] = all_data["Crypto Volume"] * all_data["Close Price"]

# ########## DEAD AND NEW CRYPTO-FIAT MANAGEMENT ############################
db = "index"
collect_log_key = "EXC_keys"
q_dict = {"Time": y_TS}

# downloading from MongoDB the matrix containing the exchange-pair logic values
logic_key = mongo.query_mongo(db, collect_log_key)

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
data = all_data.to_dict(orient="records")
collection_clean.insert_many(data)


# ################# DATA CONVERSION MAIN PART ##################

start = time.time()
# defining the database name and the collection name
db = "index"
collection_data = "EXC_cleandata"
collection_rates = "ecb_clean"
collection_stable = "stable_coin_rates"

# querying the data from mongo
matrix_rate = mongo.query_mongo(db, collection_rates)
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_rate = matrix_rate.loc[matrix_rate.Time.isin(date_array_str)]
matrix_data = mongo.query_mongo(db, collection_data)
matrix_rate_stable = mongo.query_mongo(db, collection_stable)
matrix_rate_stable = matrix_rate_stable.loc[matrix_rate_stable.Time.isin(
    date_array_str)]

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
collection_final.insert_many(data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
