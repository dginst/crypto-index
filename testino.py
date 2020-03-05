
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


def itbit_ticker (Crypto, Fiat, collection):

    for asset in Crypto:
        

        for fiat in Fiat:
            
            asset = asset.upper()
            fiat = fiat.upper()
            entrypoint = 'https://api.itbit.com/v1/markets/'
            key = asset + fiat + '/ticker'

            if asset == 'BTC':
                asset = 'XBT'
                request_url = entrypoint + key
                response = requests.get(request_url)
                response = response.json()

            else:

                request_url = entrypoint + key
                response = requests.get(request_url)
                response = response.json()   

            
                r = response    
                if asset == 'XBT':

                    try:

                        asset = 'BTC'
                        pair = asset + fiat
                        time = r['serverTimeUTC']
                        price = r['lastPrice']
                        volume = r['volume24h']
                        volumeToday = r['volumeToday'] 
                        high24h = r['high24h']
                        low24h = r['low24h']
                        highToday = r['highToday']
                        lowToday = r['lowToday']
                        openToday = r['openToday']
                        vwapToday = r['vwapToday']
                        vwap24h = r['vwap24h']
                    

                        rawdata = {'pair' : pair, 'time':time, 'price':price, 'volume': volume, 
                                    'volumeToday': volumeToday,  'high24h' : high24h, 'low24h': low24h, 
                                    'highToday': highToday, 'lowToday': lowToday, 'openToday' : openToday,
                                    'vwapToday' : vwapToday, 'vwap24h' : vwap24h }


                        

                        collection.insert_one(rawdata)

                    except:

                        print('none_itbit')
    
                else:
                    try:

                        pair = asset + fiat
                        time = r['serverTimeUTC']
                        price = r['lastPrice']
                        volume = r['volume24h']
                        volumeToday = r['volumeToday'] 
                        high24h = r['high24h']
                        low24h = r['low24h']
                        highToday = r['highToday']
                        lowToday = r['lowToday']
                        openToday = r['openToday']
                        vwapToday = r['vwapToday']
                        vwap24h = r['vwap24h']
                        

                        rawdata = {'pair' : pair, 'time':time, 'price':price, 'volume': volume, 
                                    'volumeToday': volumeToday,  'high24h' : high24h, 'low24h': low24h, 
                                    'highToday': highToday, 'lowToday': lowToday, 'openToday' : openToday,
                                    'vwapToday' : vwapToday, 'vwap24h' : vwap24h }



                        collection.insert_one(rawdata)

                    except :
                        
                        print('none_itbit')
                
    return  


crypto = ['btc', 'eth']

fiat = ['usd', 'eur']

ciao = itbit_ticker(crypto, fiat, collection_bittrextraw )