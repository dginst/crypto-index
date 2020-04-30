import utils.mongo_setup as mongo
from pymongo import MongoClient
import time
import pandas as pd
import numpy as np
from datetime import datetime
start = time.time()
pd.set_option("display.max_rows", None, "display.max_columns", None)

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
collection = db.rawdata
collection_ECB_clean = db.ecb_clean
#USDT exchange rates

########################################################kraken
database = "index"
collection = "cleandata"
query_dict = {'Exchange' : 'kraken', 'Pair' : 'btcusd'}
query_dict2 = {'Exchange' : 'kraken','Pair' : 'btcusdt'}

df = mongo.query_mongo2(database, collection, query_dict2)
df2 = mongo.query_mongo2(database, collection, query_dict)

df['rate'] = df['Close Price']/df2['Close Price']

ciao = np.array([])
for x in df['rate']:
    if x == 0:
        ciao = np.append(ciao, 0)
    else:
        ciao = np.append(ciao, 1)


df['tot_vol'] = (df['Crypto Volume'] + df2['Crypto Volume'])*ciao

########################################################bittrex

database = "index"
collection = "cleandata"
query_dict = {'Exchange' : 'bittrex', 'Pair' : 'btcusd'}
query_dict2 = {'Exchange' : 'bittrex','Pair' : 'btcusdt'}

df3 = mongo.query_mongo2(database, collection, query_dict2)
df4 = mongo.query_mongo2(database, collection, query_dict)


df3['rate'] = df3['Close Price']/df4['Close Price']

ciao = np.array([])
for x in df3['rate']:
    if x == 0:
        ciao = np.append(ciao, 0)
    else:
        ciao = np.append(ciao, 1)

df3.fillna(0, inplace = True)
df3['tot_vol'] = (df3['Crypto Volume'] + df4['Crypto Volume'])*ciao




#final rates

df['final_rates'] = ((df['rate']*df['tot_vol']) + (df3['rate'] * df3['tot_vol'])) / (df['tot_vol'] + df3['tot_vol'])

df['Rate'] = 1/df['final_rates']

df = df.replace([np.inf, -np.inf], np.nan)

df.fillna(0, inplace = True)

currency = np.array([])
for x in df['final_rates']:
    currency = np.append(currency, 'USDT/USD')

df = df.drop(columns=['Close Price', 'Pair Volume', 'Crypto Volume', 'Exchange', 'Pair', 'rate', 'tot_vol', 'final_rates' ])
df = df.rename({'Time': 'Date'}, axis='columns')
df['Currency'] = currency

new_date = np.array([])
standard_date = np.array([])

for element in df['Date']:

    standard = datetime.fromtimestamp(int(element))
    standard = standard.strftime('%Y-%m-%d')
    element = str(element)
    new_date = np.append(new_date, element)
    standard_date = np.append(standard_date, standard)

df['Date'] = new_date
df['Standard Date'] = standard_date

data = df.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)

print(df.head())

#USDC exchange rates

########################################################kraken
database = "index"
collection = "cleandata"
query_dict = {'Exchange' : 'kraken', 'Pair' : 'btcusd'}
query_dict2 = {'Exchange' : 'kraken','Pair' : 'btcusdc'}

df = mongo.query_mongo2(database, collection, query_dict2)
df2 = mongo.query_mongo2(database, collection, query_dict)

df['rate'] = df['Close Price']/df2['Close Price']

ciao = np.array([])
for x in df['rate']:
    if x == 0:
        ciao = np.append(ciao, 0)
    else:
        ciao = np.append(ciao, 1)


df['tot_vol'] = (df['Crypto Volume'] + df2['Crypto Volume'])*ciao

########################################################coinbase

database = "index"
collection = "cleandata"
query_dict = {'Exchange' : 'coinbase-pro', 'Pair' : 'btcusd'}
query_dict2 = {'Exchange' : 'coinbase-pro','Pair' : 'btcusdc'}

df3 = mongo.query_mongo2(database, collection, query_dict2)
df4 = mongo.query_mongo2(database, collection, query_dict)


df3['rate'] = df3['Close Price']/df4['Close Price']

ciao = np.array([])
for x in df3['rate']:
    if x == 0:
        ciao = np.append(ciao, 0)
    else:
        ciao = np.append(ciao, 1)

df3.fillna(0, inplace = True)
df3['tot_vol'] = (df3['Crypto Volume'] + df4['Crypto Volume'])*ciao



#final rates

df['final_rates'] = ((df['rate']*df['tot_vol']) + (df3['rate'] * df3['tot_vol'])) / (df['tot_vol'] + df3['tot_vol'])

df['Rate'] = 1/df['final_rates']

df = df.replace([np.inf, -np.inf], np.nan)

df.fillna(0, inplace = True)

currency = np.array([])
for x in df['final_rates']:
    currency = np.append(currency, 'USDC/USD')

df = df.drop(columns=['Close Price', 'Pair Volume', 'Crypto Volume', 'Exchange', 'Pair', 'rate', 'tot_vol', 'final_rates' ])
df = df.rename({'Time': 'Date'}, axis='columns')
df['Currency'] = currency

new_date = np.array([])
standard_date = np.array([])

for element in df['Date']:

    standard = datetime.fromtimestamp(int(element))
    standard = standard.strftime('%Y-%m-%d')
    element = str(element)
    new_date = np.append(new_date, element)
    standard_date = np.append(standard_date, standard)

df['Date'] = new_date
df['Standard Date'] = standard_date

data = df.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)
print(df.head())