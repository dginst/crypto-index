import cryptoindex.mongo_setup as mongo
import cryptoindex.data_setup as data_setup
from pymongo import MongoClient
import time
import pandas as pd
import numpy as np
from datetime import datetime
start = time.time()
pd.set_option("display.max_rows", None, "display.max_columns", None)

# establishing mongoDB connection
connection = MongoClient('localhost', 27017)
db = connection.index

# collection for stablecoins rates
db.stable_coin_rates.create_index([("id", -1)])
collection_stable = db.stable_coin_rates

database = "index"
collection = "CW_cleandata"

######################### USDT exchange rates computation ################################

# kraken usdt exchange rate

query_usd = {'Exchange': 'kraken', 'Pair': 'btcusd'}
query_usdt = {'Exchange': 'kraken', 'Pair': 'btcusdt'}

usdt_kraken = mongo.query_mongo2(database, collection, query_usdt)
usd_kraken = mongo.query_mongo2(database, collection, query_usd)

usdt_kraken['rate'] = usdt_kraken['Close Price'] / usd_kraken['Close Price']

existence_check = np.array([])

for x in usdt_kraken['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)


usdt_kraken['tot_vol'] = (usdt_kraken['Crypto Volume'] + usd_kraken['Crypto Volume']) * existence_check

# bittrex usdt exchange rate 

query_usd = {'Exchange' : 'bittrex', 'Pair' : 'btcusd'}
query_usdt = {'Exchange' : 'bittrex','Pair' : 'btcusdt'}

usdt_bittrex = mongo.query_mongo2(database, collection, query_usdt)
usd_bittrex = mongo.query_mongo2(database, collection, query_usd)


usdt_bittrex['rate'] = usdt_bittrex['Close Price'] / usd_bittrex['Close Price']

existence_check = np.array([])

for x in usdt_bittrex['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)

usdt_bittrex.fillna(0, inplace=True)
usdt_bittrex['tot_vol'] = (usdt_bittrex['Crypto Volume'] + usd_bittrex['Crypto Volume']) * existence_check

# usdt rates weighted average computation

kraken_weighted = usdt_kraken['rate'] * usdt_kraken['tot_vol']
bittrex_weighted = usdt_bittrex['rate'] * usdt_bittrex['tot_vol']
total_weights = usdt_kraken['tot_vol'] + usdt_bittrex['tot_vol']
usdt_rates = (kraken_weighted + bittrex_weighted) / total_weights
usdt_rates = 1 / usdt_rates
usdt_rates = pd.DataFrame(usdt_rates, columns=['Rate'])
usdt_rates = usdt_rates.replace([np.inf, -np.inf], np.nan)
usdt_rates.fillna(0, inplace=True)

usdt_rates['Currency'] = np.zeros(len(usdt_rates['Rate']))
usdt_rates['Currency'] = [str(x).replace('0', 'USDT/USD') for x in usdt_rates['Currency']]
usdt_rates['Time'] = usd_bittrex['Time']
usdt_rates['Standard Date'] = data_setup.timestamp_to_human(usd_bittrex['Time'])

# df = df.rename({'Time': 'Date'}, axis='columns')

# USDT mongoDB upload
usdt_data = usdt_rates.to_dict(orient='records')  
collection_stable.insert_many(usdt_data)

######################### USDC exchange rates computation ################################

# kraken usdc exchange rate 

query_usdc = {'Exchange': 'kraken', 'Pair': 'btcusdc'}
usdc_kraken = mongo.query_mongo2(database, collection, query_usdc)

usdc_kraken['rate'] = usdc_kraken['Close Price'] / usd_kraken['Close Price']

existence_check = np.array([])

for x in usdc_kraken['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)


usdc_kraken['tot_vol'] = (usdc_kraken['Crypto Volume'] + usd_kraken['Crypto Volume']) * existence_check

# coinbase usdc exchange rate 

query_usd_coinbase = {'Exchange': 'coinbase-pro', 'Pair': 'btcusd'}
query_usdc_coinbase = {'Exchange': 'coinbase-pro', 'Pair': 'btcusdc'}

usdc_coinbase = mongo.query_mongo2(database, collection, query_usdc_coinbase)
usd_coinbase = mongo.query_mongo2(database, collection, query_usd_coinbase)


usdc_coinbase['rate'] = usdc_coinbase['Close Price'] / usd_coinbase['Close Price']

existence_check = np.array([])

for x in usdc_coinbase['rate']:

    if x == 0:

        existence_check = np.append(existence_check, 0)

    else:

        existence_check = np.append(existence_check, 1)

usdc_coinbase.fillna(0, inplace=True)
usdc_coinbase['tot_vol'] = (usdc_coinbase['Crypto Volume'] + usd_coinbase['Crypto Volume']) * existence_check

# usdc rates weighted average computation

kraken_weighted = usdc_kraken['rate'] * usdc_kraken['tot_vol']
coinbase_weighted = usdc_coinbase['rate'] * usdc_coinbase['tot_vol']
total_weights = usdc_kraken['tot_vol'] + usdc_coinbase['tot_vol']
usdc_rates = (kraken_weighted + coinbase_weighted) / total_weights
usdc_rates = 1 / usdc_rates
usdc_rates = pd.DataFrame(usdc_rates, columns=['Rate'])
usdc_rates = usdc_rates.replace([np.inf, -np.inf], np.nan)
usdc_rates.fillna(0, inplace=True)

usdc_rates['Currency'] = np.zeros(len(usdc_rates['Rate']))
usdc_rates['Currency'] = [str(x).replace('0', 'USDC/USD') for x in usdc_rates['Currency']]
usdc_rates['Time'] = usd_coinbase['Time']
usdc_rates['Standard Date'] = data_setup.timestamp_to_human(usd_coinbase['Time'])

# USDC mongoDB upload

usdc_data = usdc_rates.to_dict(orient='records')
collection_stable.insert_many(usdc_data)
