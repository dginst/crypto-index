from pymongo import MongoClient
from datetime import *
from requests import get
import time
import requests
import os
import pandas as pd
import io
import numpy as np

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

# function that takes as arguments:
# database = database name [index_raw, index_cleaned, index_cleaned]
# collection = the name of the collection of interest
# query_dict = mongo db uses dictionary structure to do query ex:  
# {"Exchange" : "kraken", "Pair" : "btcjpy", "Time" : { "$gte": 1580774400} }, this query call all the documents 
# that contains kraken as exchange, the pair btcjpy and the time value is greater than 1580774400

def query_mongo(database, collection, query_dict):

    # defining the variable that allows to work with MongoDB
    db = connection[database]
    coll = db[collection]

    # find in the selected collection the wanted element/s
    doc = coll.find(query_dict)

    df = []
        
    for i, element in enumerate(doc):

        element.pop('_id', None)  

        if i == 0:
            df = pd.DataFrame(element, index=[i])
        
        else:

            df = df.append(element, ignore_index = True)
      
    return df
