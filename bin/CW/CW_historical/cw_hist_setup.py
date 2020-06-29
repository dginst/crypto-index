# #############################################################################
# The file completes the historical series of Cryptocurrencies market data
# stored on MongoDB
# The main rules for the manipulation of raw data are the followings:
# - if a certain Crypto-Fiat pair does not start at the beginning of the
#   period but later, the script will put a series of zeros from the start
#   period until the actual beginning of the series (homogenize_series)
# - if a certain Crypto-Fiat pair stopped to trade at a certain point, the
#   script will put a series of zeros starting from the last traded value
#   until today (homogenize_dead_series)
# - if a certain data is missing in a certain date the file will compute
#   a value to insert using all the values displayed, for the same Crypto-Fiat
#   pair, in the other exchanges.
# - if, trying to fix a series as described above, the code find out that just
#   one exchange has the values for the wanted Crypto-Fiat pair, the file will
#   put a 0-values array for all the missing date
# Once the data is manipulated and the series has been homogeneized, the script
# put the historical series on MongoDB in the collection "CW_cleandata"
# ############################################################################

# standard library import
import time
from datetime import datetime

# third party import
import numpy as np
from pymongo import MongoClient

# local import
# import cryptoindex.data_setup as data_setup
# import cryptoindex.mongo_setup as mongo
from cryptoindex.data_setup import (CW_series_fix_missing, date_gen,
                                    homogenize_dead_series, homogenize_series)
from cryptoindex.mongo_setup import query_mongo

start = time.time()
# ################ initial settings #######################

start_date = "01-01-2016"
# end_date = '03-01-2020'

# define today date as timestamp
today = datetime.now().strftime("%Y-%m-%d")
today_TS = int(datetime.strptime(today, "%Y-%m-%d").timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = date_gen(start_date)
# reference_date_vector = data_setup.timestamp_to_str(reference_date_vector)

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
# exchange complete = [ 'coinbase-pro', 'poloniex',
# 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

# #################### setup MongoDB connection ################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.CW_cleandata.drop()
db.CW_volume_checked_data.drop()

# creating the empty collection "cleandata" within the database index
db.CW_cleandata.create_index([("id", -1)])
collection_clean = db.CW_cleandata

# creating the empty collection "CW_volume_checked_data" within the database index
db.CW_volume_checked_data.create_index([("id", -1)])
collect_vol = db.CW_volume_checked_data

# defining the database name and the collection name where to look for data
db = "index"
collection_raw = "CW_rawdata"
collect_vol_chk = "CW_volume_checked_data"

# ################ fixing the "Pair Volume" information ###########

tot_matrix = query_mongo(db, collection_raw)
tot_matrix = tot_matrix.loc[tot_matrix.Time != 0]
tot_matrix = tot_matrix.drop(columns=["Low", "High", "Open"])
# tot_matrix['str_t'] = [str(t) for t in tot_matrix['Time']]
tot_matrix['key'] = tot_matrix['Exchange'] + '&' + \
    tot_matrix['Pair']
# correct the "Crypto Volume" information for the date 2017-04-28 and
# 2017-04-29 using the previous day value
m_27_04 = tot_matrix.loc[tot_matrix.Time
                         == 1493251200, ['key', 'Crypto Volume']]

m_28_04 = tot_matrix.loc[tot_matrix.Time == 1493337600]
m_29_04 = tot_matrix.loc[tot_matrix.Time == 1493424000]
for k in m_27_04['key']:

    prev_vol = np.array(m_27_04.loc[m_27_04.key == k, "Crypto Volume"])
    m_28_04.loc[m_28_04.key == k, "Crypto Volume"] = prev_vol
    m_29_04.loc[m_29_04.key == k, "Crypto Volume"] = prev_vol

tot_matrix = tot_matrix.loc[tot_matrix.Time != 1493337600]
tot_matrix = tot_matrix.loc[tot_matrix.Time != 1493424000]

tot_matrix = tot_matrix.append(m_28_04)
tot_matrix = tot_matrix.append(m_29_04)

tot_matrix = tot_matrix.drop(columns=['key'])

for Crypto in Crypto_Asset:

    ccy_pair_array = []

    for i in pair_array:

        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        for cp in ccy_pair_array:

            mat = tot_matrix.loc[tot_matrix["Exchange"] == exchange]
            mat = mat.loc[mat["Pair"] == cp]
            # checking if the matrix is not empty
            if mat.shape[0] > 1:

                if exchange == "bittrex" and cp == "btcusdt":

                    sub_vol = np.array(
                        mat.loc[mat.Time == 1544486400, "Crypto Volume"])
                    mat.loc[mat.Time == 1544572800, "Crypto Volume"] = sub_vol
                    mat.loc[mat.Time == 1544659200, "Crypto Volume"] = sub_vol

                mat["Pair Volume"] = mat["Close Price"] * mat["Crypto Volume"]

            # put the manipulated data on MongoDB
            try:

                data = mat.to_dict(orient="records")
                collect_vol.insert_many(data)

            except TypeError:
                pass

end = time.time()

print("This script took: {} seconds".format(float(end - start)))

# ############## fixing historical series main part ##############
start = time.time()

tot_matrix = query_mongo(db, collect_vol_chk)


for Crypto in Crypto_Asset:

    print(Crypto)
    ccy_pair_array = []

    for i in pair_array:

        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        ex_matrix = tot_matrix.loc[tot_matrix["Exchange"] == exchange]
        print(exchange)
        for cp in ccy_pair_array:

            print(cp)
            crypto = cp[:3]
            pair = cp[3:]

            cp_matrix = ex_matrix.loc[ex_matrix["Pair"] == cp]
            cp_matrix = cp_matrix.drop(columns=["Exchange", "Pair"])
            # checking if the matrix is not empty
            if cp_matrix.shape[0] > 1:

                # check if the historical series start at the same date as
                # the start date if not fill the dataframe with zero values
                cp_matrix = homogenize_series(
                    cp_matrix, reference_date_vector
                )

                # check if the series stopped at certain point in
                # the past, if yes fill with zero
                cp_matrix = homogenize_dead_series(
                    cp_matrix, reference_date_vector
                )

                # checking if the matrix has missing data and if ever fixing it
                if cp_matrix.shape[0] != reference_date_vector.size:

                    cp_matrix = CW_series_fix_missing(
                        cp_matrix,
                        exchange,
                        cp,
                        reference_date_vector,
                        db,
                        collect_vol_chk,
                    )

                # ######## part that transform the timestamped date into string

                new_date = np.array([])
                for element in cp_matrix["Time"]:

                    element = str(element)
                    new_date = np.append(new_date, element)

                cp_matrix["Time"] = new_date
                # ############################################################

                # add exchange and currency_pair column
                cp_matrix["Exchange"] = exchange
                cp_matrix["Pair"] = cp
                # put the manipulated data on MongoDB
                data = cp_matrix.to_dict(orient="records")
                collection_clean.insert_many(data)

# #######################################################################
db = connection.index
# db.CW_volume_checked_data.drop()

end = time.time()

print("This script took: {} seconds".format(float(end - start)))
