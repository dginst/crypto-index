import utils.mongo_setup as mongo
from pymongo import MongoClient
import time
import pandas as pd
start = time.time()
pd.set_option("display.max_rows", None, "display.max_columns", None)

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
collection = db.rawdata

database = "index"
collection = "rawdata2"
collection_volume_check = "volume_checked_data"

db = connection[database]
coll = db[collection]
query_dict = {"Exchange" : 'coinbase-pro', "Pair": 'ethgbp'}

df = mongo.query_mongo(database, collection, query_dict)
print(df)

#ciao = collection.find()


# df = pd.DataFrame(list(coll.find(query_dict)))
# df = df.drop(columns= '_id')

# print(df)

# end = time.time()



# print("This script took: {} seconds".format(float(end-start)))


# def query_mongo(database, collection, query_dict = None):

#     # defining the variable that allows to work with MongoDB
#     db = connection[database]
#     coll = db[collection]

#     # find in the selected collection the wanted element/s
#       # find in the selected collection the wanted element/s
    
#     if query_dict == None:

#         df = pd.DataFrame(list(coll.find()))
#         df = df.drop(columns= '_id')
#     else:
#         df = pd.DataFrame(list(coll.find(query_dict)))
        
#         df = df.drop(columns= '_id')
    
#     print(df)
#     return df

