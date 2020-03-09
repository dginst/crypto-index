
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

def today_ts():

    today = datetime.now().strftime('%Y-%m-%d')
    today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

    return today_TS

ciao =  midnight_ts()

print(ciao)