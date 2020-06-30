# ############################################################################
# The script is divided in three main parts.
# 1) USDT and USDC rates computation
# using the weighted average value of btc/usd rates (computed using all the
# avalilable exchanges) and the average btc/usdc and btc/usdc rates, the script
# computes USDT/USD and USDC/USD rate. The historical series of rates is
# uploaded on MongoDB in the collection "stable_coin_rates"
# 2) non-USD  values conversion into USD
# The file retrieves data from MongoDB collection "CW_cleandata" and, for each
# Crypto-Fiat historical series, converts the data into USD values using
# the ECB rates stored on MongoDB in the collection "ecb_clean" and the
# computed stablecoin rates saved in the "stable_coin_rates" collection.
# Once everything has been converted, the script upload the dataframe on
# MongoDB collection "converted_data".
# 3) filling the zero-volume values
# the script, if the historical series have date where the crypto-pair is
# traded but the volume displayed is zero, takes the previous day value
# and fills the missing volume.
# ###############################################################################

# standard library import
import time
from datetime import datetime, timezone

# third party import
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.mongo_setup as mongo

start = time.time()

# ################## initial settings #####################################

start_date = "01-01-2016"
day_in_sec = 86400
# define today date as timestamp
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())
y_TS = today_TS - 86400


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

# drop the pre-existing collection (if there is one)
db.CW_converted_data.drop()
db.CW_final_data.drop()
db.stable_coin_rates.drop()
db.CW_keys.drop()
db.EXC_keys.drop()

# creating the empty collection cleandata within the database index
db.CW_final_data.create_index([("id", -1)])
db.CW_converted_data.create_index([("id", -1)])
db.stable_coin_rates.create_index([("id", -1)])
db.CW_keys.create_index([("id", -1)])
db.EXC_keys.create_index([("id", -1)])
collection_stable = db.stable_coin_rates
collection_final_data = db.CW_final_data
collection_converted = db.CW_converted_data
collection_CW_key = db.CW_keys
collection_EXC_key = db.EXC_keys

# ########## USDC/USD and USDT/USD computation #####################

start = time.time()

# MongoDB index and collection names definition
database = "index"
collection = "CW_cleandata"

# taking BTC/USD pair historical
first_query = {"Pair": "btcusd", "Exchange": "kraken"}
first_call = mongo.query_mongo(database, collection, first_query)
price_df = first_call[["Close Price"]]
volume_df = first_call[["Pair Volume"]]
price_df = price_df.rename(columns={"Close Price": "kraken"})
volume_df = volume_df.rename(columns={"Pair Volume": "kraken"})
Exchanges.remove("kraken")

for exchange in Exchanges:

    query = {"Pair": "btcusd", "Exchange": exchange}
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

# first_query = {'Pair': 'btcusd', 'Exchange': 'kraken'}
# first_call = mongo.query_mongo(database, collection, first_query)
# average_df = first_call
# average_df['average usd'] = average_df['Close Price']

# ############# USDT exchange rates computation ##########
# BTC/USDT is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historical values from 01/01/2016

# POLONIEX usdt/usd exchange rate
query_usdt = {"Exchange": "poloniex", "Pair": "btcusdt"}
usdt_poloniex = mongo.query_mongo(database, collection, query_usdt)

# KRAKEN usdt/usd exchange rate
query_usdt = {"Exchange": "kraken", "Pair": "btcusdt"}
usdt_kraken = mongo.query_mongo(database, collection, query_usdt)

# BITTREX usdt/usd exchange rate
query_usdt = {"Exchange": "bittrex", "Pair": "btcusdt"}
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

# correcting the date 2016-10-02 using the previous day rate
prev_rate = np.array(usdt_rates.loc[usdt_rates.Time == '1475280000', "Rate"])
usdt_rates.loc[usdt_rates.Time == '1475366400', "Rate"] = prev_rate

# USDT mongoDB upload
usdt_data = usdt_rates.to_dict(orient="records")
collection_stable.insert_many(usdt_data)


# ############# USDC exchange rates computation ############
# BTC/USDC is traded on Poloniex, Kraken and bittrex
# Poloniex has the entire historoical values from 01/01/2016

# POLONIEX usdc/usd exchange rate
query_usdc = {"Exchange": "poloniex", "Pair": "btcusdc"}
usdc_poloniex = mongo.query_mongo(database, collection, query_usdc)

# KRAKEN usdc/usd exchange rate
query_usdc = {"Exchange": "kraken", "Pair": "btcusdc"}
usdc_kraken = mongo.query_mongo(database, collection, query_usdc)

# COINBASE_PRO usdc exchange rate
query_usdc_coinbase = {"Exchange": "coinbase-pro", "Pair": "btcusdc"}
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

# USDC mongoDB upload
usdc_data = usdc_rates.to_dict(orient="records")
collection_stable.insert_many(usdc_data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# ##############################################################

# ################# DATA CONVERSION MAIN PART ##################

start = time.time()
# defining the database name and the collection name
db = "index"
collection_data = "CW_cleandata"
collection_rates = "ecb_clean"
collection_stable = "stable_coin_rates"

# querying the data from mongo
matrix_rate = mongo.query_mongo(db, collection_rates)
matrix_rate = matrix_rate.rename({"Date": "Time"}, axis="columns")
matrix_data = mongo.query_mongo(db, collection_data)
matrix_rate_stable = mongo.query_mongo(db, collection_stable)

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

converted_data = converted_data.sort_values(by=["Time"])

data = converted_data.to_dict(orient="records")
collection_converted.insert_many(data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# #####################################################################
# ############### LOGIC MATRIX OF KEYS ################################

# define database name and collection name
db_name = "index"
collection_converted_data = "CW_converted_data"

# retriving the needed information on MongoDB
q_dict = {"Time": str(y_TS)}
matrix_last_day = mongo.query_mongo(db_name, collection_converted_data, q_dict)
old_head = matrix_last_day.columns
matrix_last_day["key"] = matrix_last_day["Exchange"] + \
    "&" + matrix_last_day["Pair"]
matrix_last_day["logic"] = 1
matrix_last_day = matrix_last_day.drop(columns=old_head)

# creating the list containing all the possible exchange-pair key
Exchanges = [
    "coinbase-pro",
    "poloniex",
    "bitstamp",
    "gemini",
    "bittrex",
    "kraken",
    "bitflyer",
]
all_key = []
for exc in Exchanges:

    for cry in Crypto_Asset:

        for i in pair_array:

            all_key.append(exc + "&" + cry.lower() + i)

# creating the logic check dataframe
header = ["key", "logic_value"]
key_df = pd.DataFrame(columns=header)
key_df["key"] = all_key
# key_df['logic_value'] = np.zeros(len(all_key) - 1)

merged = pd.merge(key_df, matrix_last_day, on="key", how="left")
merged.fillna(0, inplace=True)

key_df["logic_value"] = merged["logic"]

data = key_df.to_dict(orient="records")
collection_CW_key.insert_many(data)
collection_EXC_key.insert_many(data)

# ################ ZERO VOLUMES VALUE FILLING #####################

# define database name and collection name
db_name = "index"
collection_converted_data = "CW_converted_data"

# retriving the needed information on MongoDB
matrix = mongo.query_mongo(db_name, collection_converted_data)
matrix["Crypto"] = matrix["Pair"].str[:3]
head = [
    "Time",
    "Close Price",
    "Crypto Volume",
    "Pair Volume",
    "Exchange",
    "Pair",
    "Crypto",
]
final_matrix = pd.DataFrame(columns=head)

for Crypto in Crypto_Asset:

    cry_matrix = matrix.loc[matrix.Crypto == Crypto.lower()]
    print(Crypto)
    exc_list = list(matrix["Exchange"].unique())
    print(exc_list)

    for exchange in exc_list:
        print(exchange)
        ex_matrix = cry_matrix.loc[cry_matrix.Exchange == exchange]
        ex_matrix.drop(columns=["Crypto"])
        # finding the crypto-fiat pair in the selected exchange
        pair_list = list(ex_matrix["Pair"].unique())
        print(pair_list)

        for cp in pair_list:

            cp_matrix = ex_matrix.loc[ex_matrix.Pair == cp]
            # checking if the matrix is not empty
            try:

                if cp_matrix.shape[0] > 1:
                    print(cp)
                    print(cp_matrix.Exchange)
                    print('ciao')
                    
                    cp_matrix = data_setup.fix_zero_value(cp_matrix)

                    final_matrix = final_matrix.append(cp_matrix)

            except AttributeError:
                pass

# put the manipulated data on MongoDB
data = final_matrix.to_dict(orient="records")
collection_final_data.insert_many(data)

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
