
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
#creating the empty collection rawdata within the database index
collection_bitstamptraw = db.bitstamptraw
collection_geminiraw = db.geminiraw
collection_bittrexraw = db.bittrexraw
collection_bitflyertraw = db.bitflyertraw

crypto = ['xbt', 'eth', 'ltc']
fiat = ['usd', 'eur']            

def itbit_ticker (crypto, fiat, db, collection):

    header = ['pair', 'bid', 'bidAmt', 'ask', 'askAmt', 'lastPrice', 'lastAmt', 'volume24h', \
            'volumeToday', 'high24h', 'low24h', 'highToday','lowToday','openToday', 'vwapToday',\
            'vwap24h', 'serverTimeUTC']

    for asset in crypto:
        asset = asset.upper()

        for fiat in fiat:

            fiat = fiat.upper()
            entrypoint = 'https://api.itbit.com/v1/markets/'
            key = asset + fiat + '/ticker'
            request_url = entrypoint + key

            response = requests.get(request_url)

            response = response.json()   
            print(response)

            try:
                collection.insert_one(response)
            except:
                print('none_itbit')
            


    return  


data = itbit_ticker(crypto, fiat, db, collection_bitflyertraw)






    

