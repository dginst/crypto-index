# standard library import
import time
from datetime import datetime

# third party import
from pymongo import MongoClient
import pandas as pd
import numpy as np

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
import cryptoindex.mongo_setup as mongo


# ########################## initial settings #################################

# set start_period # aggiungere lo start, deve coincidere con la data di inzio
#  dei ticker
Start_Period = '04-17-2020'
# set today
today = datetime.now().strftime('%Y-%m-%d')

# creating the timestamp array at 12:00 AM
date_array = data_setup.timestamp_gen(Start_Period)
date_array_str = [str(el) for el in date_array]

# pair arrat without USD (no need of conversion)
pair_array = ['usd', 'gbp', 'eur', 'cad', 'jpy', 'usdt', 'usdc']
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur']
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA',
                'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA',
#  'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = ['coinbase-pro', 'poloniex', 'bitstamp', 'gemini',
             'bittrex', 'kraken', 'bitflyer']
# exchange complete = ['coinbase-pro', 'poloniex', 'bitstamp',
#  'gemini', 'bittrex', 'kraken', 'bitflyer']

# defining the crypto-fiat pairs array
cryptofiat_array = []

for crypto in Crypto_Asset:

    for fiat in pair_array:

        cryptofiat_array.append(crypto.lower() + fiat)

# ############################ setup mongo connection ##################

# connecting to mongo in local
connection = MongoClient('localhost', 27017)
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

collection_CW_raw = "CW_rawdata"

# ################### creation of EXC_cleandata collection ##################

database = "index"
collection_raw = "EXC_test"

# querying all raw data from EXC_rawdata
all_data = mongo.query_mongo2(database, collection_raw)

# defining the columns on interest
head = ['Pair', 'Exchange', 'Time',
        'Close Price', 'Crypto Volume', 'Pair Volume']

# kepping the columns of interest among all the information in rawdata
all_data = all_data.loc[:, head]

# changing the "Time" values format from integer to string
# all_data['Time'] = [str(element - 86400) for element in all_data['Time']] ## tolto 1 d
all_data['Time'] = [str(element) for element in all_data['Time']]  # tolto 1 d

# selecting the date corresponding to 12:00 AM
all_data = all_data.loc[all_data['Time'].isin(date_array_str)]

# changing some features in "Pair" field
all_data['Pair'] = [element.replace(
    'USDT_BCHSV', 'bsvusdt') for element in all_data['Pair']]
all_data['Pair'] = [element.replace(
    'USDC_BCHSV', 'bsvusdc') for element in all_data['Pair']]
all_data['Pair'] = [element.replace(
    'USDT_BCHABC', 'bchusdt') for element in all_data['Pair']]
all_data['Pair'] = [element.replace(
    'USDC_BCHABC', 'bchusdc') for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_LTC', 'ltcusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_LTC', 'ltcusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_XRP', 'xrpusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_XRP', 'xrpusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_ZEC', 'zecusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_ZEC', 'zecusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_EOS', 'eosusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_EOS', 'eosusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_ETC', 'etcusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_ETC', 'etcusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_STR', 'xlmusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_STR', 'xlmusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_BTC', 'btcusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_BTC', 'btcusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDC_ETH', 'ethusdc')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.replace('USDT_ETH', 'ethusdt')
                    for element in all_data['Pair']]
all_data['Pair'] = [element.lower() for element in all_data['Pair']]
all_data['Pair'] = [element.replace('xbt', 'btc')
                    for element in all_data['Pair']]


# selecting the crypto-fiat pairs used in the index computation
all_data = all_data.loc[all_data['Pair'].isin(cryptofiat_array)]

# selecting the exchange used in the index computation
all_data = all_data.loc[all_data['Exchange'].isin(Exchanges)]

# correcting the "Pair Volume" field
all_data["Pair Volume"] = all_data["Crypto Volume"] * all_data["Close Price"]

# uploading the cleaned data on MongoDB in the collection EXC_cleandata
data = all_data.to_dict(orient='records')
collection_clean.insert_many(data)

# ########### data conversion and creation of EXC_final_data collection #######

database = 'index'
collection_data = "EXC_cleandata"
collection_rates = "ecb_clean"

# field of conversion
conv_fields = ['Close Price', "Pair Volume"]

for date in date_array:

    for fiat in pair_array:

        if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

            fiat = fiat.upper()
            ex_rate = fiat + '/USD'
            # defining the dictionary for the MongoDB query
            query_dict_rate = {"Currency": ex_rate, "Date": str(date)}
            # retriving the needed information on MongoDB
            matrix_rate = mongo.query_mongo2(
                database, collection_rates, query_dict_rate)
            # finding the conversion rate
            conv_rate = np.array(matrix_rate['Rate'])

        currencypair_array = []

        for Crypto in Crypto_Asset:

            currencypair_array.append(Crypto.lower() + fiat.lower())

        for cp in currencypair_array:

            # defining the dictionary for the MongoDB query
            query_dict_data = {"Pair": cp, "Time": str(date)}

            # retriving the needed information on MongoDB
            matrix_data = mongo.query_mongo2(
                database, collection_data, query_dict_data)

            try:

                if (fiat != 'usd' and fiat != 'usdt' and fiat != 'usdc'):

                    # converting the values
                    matrix_data['Close Price'] = matrix_data['Close Price'] / conv_rate
                    matrix_data['Pair Volume'] = matrix_data['Pair Volume'] / conv_rate

                else:

                    matrix_data = matrix_data

                # adding a human-readable date format
                standard_date = np.array([])

                for element in matrix_data['Time']:

                    standard = datetime.fromtimestamp(int(element))
                    standard = standard.strftime('%d-%m-%Y')
                    standard_date = np.append(standard_date, standard)

                matrix_data['Standard Date'] = standard_date

                # put the manipulated data on MongoDB
                data = matrix_data.to_dict(orient='records')
                collection_final.insert_many(data)

            except TypeError:

                pass
