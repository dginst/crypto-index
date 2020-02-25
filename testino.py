
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
collection_coinbasetraw = db.coinbasetraw


def poloniex_ticker ():
    
    entrypoint = 'https://poloniex.com/public?command=returnTicker'
    #key = asset + fiat + '/ticker'
    request_url = entrypoint

    response = requests.get(request_url)

    response = response.json()

    
    try:
        collection.insert_one(dict2)
    except:
        print('none_poloniex')
            
    return response

response = poloniex_ticker()

pairs = ['USDT_BTC', 'USDC_BTC', 'USDT_ETH']

dict2 = {x:response[x] for x in pairs}

print(dict2)


