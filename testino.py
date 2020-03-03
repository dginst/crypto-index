
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
collection_bittrextraw = db.bittrextraw


def bittrex_ticker (Crypto, Fiat, collection):


    for asset in Crypto:
        asset = asset.lower()

        for fiat in Fiat:

            fiat = fiat.lower()
            entrypoint = 'https://api.bittrex.com/api/v1.1/public/getmarketsummary?market='
            key = fiat + '-' + asset 
            request_url = entrypoint + key

            response = requests.get(request_url)

            response = response.json() 

           
            
            try:
                r = response["result"][0]
                pair = asset+fiat
                time = r['TimeStamp']
                price = r['Last']
                volume = r['Volume']
                basevolume = r['BaseVolume'] 
                high = r['High']
                low = r['Low']
                bid = r['Bid']
                ask = r['Ask']
                openbuyorders = r['OpenBuyOrders']
                opensellorders = r['OpenSellOrders']
                prevday = r['PrevDay']

                rawdata = {'pair' : pair, 'time':time, 'price':price, 'volume': volume, 
                            'basevolume': basevolume,  'high' : high, 'low': low, 
                            'bid': bid, 'ask': ask, 'openbuyorders' : openbuyorders,
                            'opensellorders' : opensellorders, 'prevday' : prevday }


                collection.insert_one(rawdata)

            except :
                
                print('none_bittrex')
            
         

    return 


crypto = ['btc', 'eth']

fiat = ['usd', 'eur']

ciao = bittrex_ticker(crypto, fiat, collection_bittrextraw )