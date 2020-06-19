# third party packages
from os import path

import pandas as pd
from pymongo import MongoClient

# internal import
from cryptoindex.mongo_setup import query_mongo

# connecting to the database
connection = MongoClient("localhost", 27017)
db = connection.index

filename = "mongo_test.json"
data_folder = path.join(path.dirname(__file__), "data", filename)


def test_query_mongo():

    # uploading test json file
    df_test = pd.read_json(data_folder)

    df_test = df_test.drop(columns=["_id"])

    #df_test.drop("_id", axis=1, inplace=True)

    # querying the collection from db

    database = "index"
    collection = "test_api_request"

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
