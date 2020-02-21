
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

cursor = db.coll.aggregate([{"$group": {"_id": "TIME_PERIOD", "unique_ids" : {"$addToSet": "$_id"}, "count": {"$sum": 1}}},{"$match": {"count": { "$gte": 2 }}}])

response = []
for doc in cursor:
    del doc["unique_ids"][0]
    for id in doc["unique_ids"]:
        response.append(id)

coll.delete_many({"_id": {"$in": response}})

# collection_bitstamptraw = db.bitstamptraw
# collection_geminiraw = db.geminiraw
# collection_bittrexraw = db.bittrexraw
# collection_bitflyertraw = db.bitflyertraw








    

