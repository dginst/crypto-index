# third party packages
import pandas as pd
from pymongo import MongoClient

from os import path 
# internal import
from cryptoindex.mongo_setup import query_mongo

# connecting to the database
connection = MongoClient("localhost", 27017)
db = connection.index

filename = "mongo_test.json"
data_folder = path.join(path.dirname(__file__), "test_folder", filename )

def test_query_mongo():

    # uploading test json file
    df_test = pd.read_json(data_folder)

    df_test.drop("_id", axis=1, inplace=True)

    # querying the collection from db

    database = "index"
    collection = "test"

    df_mongo = query_mongo(database, collection)

    assert df_test.equals(df_mongo)

    query_dict = {"A": {"$gt": 0}}

    df_mongo = query_mongo(database, collection, query_dict)

    assert df_test.equals(df_mongo)

    query_dict = {"A": {"$gt": 111110}}
    ll = []

    df_mongo = query_mongo(database, collection, query_dict)

    assert ll == df_mongo

    collection = "tes"

    df_mongo = query_mongo(database, collection)

    assert ll == df_mongo
