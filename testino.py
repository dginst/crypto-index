
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

collection_bitstamptraw = db.bitstamptraw

def bitstamp_ticker(Crypto, Fiat, collection):

    header = ["high", "last", "timestamp", "bid", "vwap", "volume", "low", "ask", "open"]

    for asset in Crypto:

        asset = asset.lower()
        for fiat in Fiat:

            fiat = fiat.lower()
            entrypoint = 'https://www.bitstamp.net/api/v2/ticker/'
            key = asset + fiat
            request_url = entrypoint + key

            response = requests.get(request_url)

            response = response.json()

            try:
                
                r = response
                pair = asset+fiat
                time = r['timestamp']
                price = r['last']
                volume = r['volume']
                high = r['high'] 
                low = r['low']
                bid = r['bid']
                ask = r['ask']
                vwap = r['vwap']
                Open = r['open']
                

                rawdata = {'pair' : pair, 'time':time, 'price':price, 'volume':volume, 'high':high, 'low' : low, 'bid': bid, 'ask':ask, 'vwap': vwap, 'open' : Open}

                
                collection.insert_one(rawdata)        
            except:
                print('none_bitstamp')

    return 

Crypto = ['BTC', 'ETH']
Fiat = ['USD', 'EUR']

ciao = bitstamp_ticker(Crypto, Fiat, collection_bitstamptraw)