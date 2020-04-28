from pymongo import MongoClient
import time
import pandas as pd
start = time.time()
pd.set_option("display.max_rows", None, "display.max_columns", None)

connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
collection = db.rawdata

query_dict = {"Exchange" : 'coinbase-pro', "Pair": 'ethgbp'}

#ciao = collection.find()

df = pd.DataFrame(list(collection.find(query_dict)))
df = df.drop(columns= '_id')

print(df)

end = time.time()



print("This script took: {} seconds".format(float(end-start)))


def query_mongo(database, collection, query_dict = None):

    # defining the variable that allows to work with MongoDB
    
    coll = db.collection

    # find in the selected collection the wanted element/s
    
    if query_dict == None:

        df = pd.DataFrame(list(coll.find()))
        df = df.drop(columns= '_id')
    else:
        df = pd.DataFrame(list(coll.find(query_dict)))
        print(df)
        df = df.drop(columns= '_id')
    
    print(df)
    return df
