
from pymongo import MongoClient
import mongoconn as mongo
import utils.data_setup as data_setup
import numpy as np
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient
import requests
from requests import get

import API_request as api

connection = MongoClient('localhost', 27017)
#creating the database called index

db = connection.index
db.rawdata.create_index([ ("id", -1) ])
coll = db.ecb_raw
# collection_bitstamptraw = db.bitstamptraw
# collection_geminiraw = db.geminiraw
# collection_bittrexraw = db.bittrexraw
collection_krakentraw = db.krakentraw

def kraken_ticker (Crypto, Fiat, collection):


    for asset in Crypto:
        

        for fiat in Fiat:
            
            asset = asset.lower()
            fiat = fiat.lower()
            entrypoint = 'https://api.kraken.com/0/public/Ticker?pair='
            key = asset+fiat
            request_url = entrypoint + key

            response = requests.get(request_url)

            response = response.json()

            try:
                asset = asset.upper()
                fiat = fiat.upper()
                pair = 'X' + asset + 'Z' + fiat
                r = response['result'][pair]
                print(r)
                
                collection.insert_one(response)
            except:
                print('none_kraken')

            pair = asset + fiat
            time = datetime.utcnow()
            price = r['c'][0]
            crypto_volume = r['v'][1]

            rawdata = {'pair' : pair, 'time':time, 'price':price, 'crypto_volume': crypto_volume}

            collection.insert_one(rawdata)
            
    return 


asset = ['xbt', 'ETH']
stbc = ['usd', 'eur']

ciao = kraken_ticker(asset, stbc, collection_krakentraw)



