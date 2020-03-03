
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
collection_geminitraw = db.geminitraw


def gemini_ticker(Crypto, Fiat, collection):

    for asset in Crypto:

        asset = asset.lower()
        for fiat in Fiat:

            fiat = fiat.lower()
            entrypoint = 'https://api.gemini.com/v1/pubticker/'
            key = asset + fiat
            request_url = entrypoint + key

            response = requests.get(request_url)
            response = response.json()

            try:
                asset = asset.upper()
                r = response
                pair = asset + fiat
                time = r['volume']['timestamp']
                price = r['last']
                asset = asset.upper()
                volume = r['volume'][asset]
                bid = r['bid']
                ask = r['ask']
               
                rawdata = {'pair' : pair, 'time': time, 'price': price, 'volume': volume, 'bid': bid,
                            'ask' : ask}
            
                collection.insert_one(rawdata)
            except :
                print('none_gemini')
    
    return

crypto = ['btc', 'eth']
fiat = ['usd', 'eur']

ciao = gemini_ticker(crypto, fiat, collection_geminitraw)