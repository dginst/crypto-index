import utils.data_setup as data_setup
import utils.data_download as data_download
from pymongo import MongoClient
import time
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import pandas as pd
import requests
from requests import get
import mongoconn as mongo

start_date = '01-01-2019'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)


pair_array = ['gbp', 'usd','eur', 'cad', 'jpy']#, 'gbp', 'usd', 'cad', 'jpy']#, 'eur', 'cad', 'jpy'] 
Crypto_Asset = ['ETH', 'BTC', 'LTC', 'BCH', 'XRP'] #BCH, LTC

# we use all the xchanges except for Kraken that needs some more test in order to be introduced without error
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer'] #'kraken', 'bitflyer'



####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_raw = db.rawdata
collection_clean = db.cleandata
collection_ECB_raw = db.ecb_raw

def CW_raw_to_mongo(exchange, currencypair, mongo_collection, start_date = '01-01-2016', end_date = None, periods = '86400'):

    
    Crypto = currencypair[:3].upper()
    Pair = currencypair[3:].upper()
    
    # check date format
    start_date = data_setup.date_reformat(start_date)
    start_date = datetime.strptime(start_date, '%m-%d-%Y')

    # set end_date = today if empty
    if end_date == None:

        end_date = datetime.now().strftime('%m-%d-%Y')
    else:

        end_date = data_setup.date_reformat(end_date, '-')

    end_date = datetime.strptime(end_date, '%m-%d-%Y')

    # transform date into timestamps
    start_date = str(int(time.mktime(start_date.timetuple())))
    end_date = str(int(time.mktime(end_date.timetuple())))

    # API settings
    entrypoint = 'https://api.cryptowat.ch/markets/' 
    key = exchange + "/" + currencypair + "/ohlc?periods=" + periods + "&after=" + start_date + "&before=" + end_date
    request_url = entrypoint + key
    
    # API call
    response = requests.get(request_url)
    response = response.json()
    print(response)
    #print(len(response))
    
    try: 
        for i in range(len(response['result']['86400'])):
            
            r = response['result']['86400']
            #print(r)
            Exchange = exchange
            Pair = currencypair
            Time = r[i][0]
            Open  = r[i][1] 
            High = r[i][2]
            Low = r[i][3]
            Close_Price = r[i][4]
            Crypto_Volume = r[i][5]
            Pair_Volume = r[i][6]

            rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time':Time, 'Low':Low, 'High':High, 'Open':Open, 'Close Price':Close_Price, 'Crypto Volume':Crypto_Volume, 'Pair Volume': Pair_Volume}

            mongo_collection.insert_one(rawdata)

    except:
        
        r = response
        Exchange = exchange
        Pair = currencypair
        Time = 0
        Open  = 0
        High = 0
        Low = 0
        Close_Price = 0
        Crypto_Volume = 0
        Pair_Volume = 0


        rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time':Time, 'Low':Low, 'High':High, 'Open':Open, 'Close Price':Close_Price, 'Crypto Volume':Crypto_Volume, 'Pair Volume': Pair_Volume}

        mongo_collection.insert_one(rawdata)

    return  None


##########

for Crypto in Crypto_Asset:

    currencypair_array = []
    for i in pair_array:
        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
    
    
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting to CryptoWatch website
            matrix = CW_raw_to_mongo(exchange, cp, collection_raw, start_date)
            # query data from mongo. Syntax : { "field" : "value", "field": "value" }, Syntax with logic operators(LO) : { "field" : {"$LO" : "value"} }
            # for daily calculation  it is necessary to put the third condition : {"Time" : x}
            # db = "index"
            # collection = "rawdata"
            # query_dict = {"Exchange" : exchange, "Pair": cp }
            # matrix = mongo.query_mongo(db, collection, query_dict)



