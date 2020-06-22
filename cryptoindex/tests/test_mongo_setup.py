# third party packages
from os import path
import pandas as pd
from pymongo import MongoClient
from bson.json_util import loads

# internal import
from cryptoindex.mongo_setup import query_mongo

# connecting to the database
connection = MongoClient("localhost", 27017)
db = connection.index

# creating the collection for the test
db.test_mongo.drop()

# creating the empty collection cleandata within the database index
db.test_mongo.create_index([("id", -1)])
coll_mongo = db.test_mongo

# defining file path and naming
filename = "mongo_test.json"
data_folder = path.join(path.dirname(__file__), "data", filename)

# uploading the file in the test_mongo collection

with open(data_folder) as m:

    data = m.read()
    file_data = loads(data)

coll_mongo.insert_many(file_data)


def test_query_mongo():

    # uploading test json file
    df_test = pd.read_json(data_folder)

    df_test = df_test.drop(columns=["_id"])

    # querying the collection from db

    database = "index"
    collection = "test_mongo"

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
