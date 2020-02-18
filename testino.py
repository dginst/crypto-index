
from pymongo import MongoClient
import mongoconn as mongo
import utils.data_setup as data_setup
import numpy as np
from datetime import datetime, timedelta
import pandas as pd


# Start_Period = '2016-01-01'
# End_Period = '2019-02-13'
# date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
# print(type(date[0]))
# print(date[0])

# date = [datetime.strptime(x, '%Y-%m-%d') for x in date ]
# print(type(date[0]))
# print(date[0])

# #connecting to mongo in local
# connection = MongoClient('localhost', 27017)
# #creating the database called index
# db = connection.index
# db.ecb_raw.create_index([ ("id", -1) ])
# #creating the empty collection rawdata within the database index
# collection_ECB_raw = db.ecb_raw

# database= 'index'
# collection = 'ecb_raw'

# query = {'CURRENCY' : "USD", 'TIME_PERIOD': date[3] }

# ecb_query = mongo.query_mongo(database, collection, query)


# #print(ecb_query['CURRENCY'].loc[0])
# print(ecb_query)

today = datetime.today()

print(type(datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')))
print(datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d'))

    

