# third party packages
from os import path

import pandas as pd
from bson.json_util import loads
from pymongo import MongoClient

# internal import
from cryptoindex.mongo_setup import (df_reorder, mongo_coll, mongo_coll_drop,
                                     mongo_index_conn, mongo_indexing,
                                     query_mongo, mongo_upload)

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


def test_mongo_index_conn():

    assert mongo_index_conn() == db


def test_mongo_indexing():

    mongo_indexing()


def test_mongo_coll():

    assert type(mongo_coll()) == dict
    # asserting that the dictionary is not empty
    assert bool(mongo_coll) is True


def test_mongo_coll_drop():

    ll = ['ecb_hist_d', 'ecb_hist_s', 'cw_hist_d', 'cw_hist_s', 'cw_hist_conv', 'index_hist', 'index_feed', 'exc']

    for operation in ll:

        mongo_coll_drop(operation)


def test_df_reorder():

    data = [{'Date': 1, 'ZEC' : 10, 'Time': 2, 'ETH' : 4, 'BTC' : 3, 'Exchange':50, 'Close Price':70, 'Pair': 120, 'Pair Volume':250, 'Crypto Volume':560,'XRP' : 5, 'LTC' : 6, 'BCH' : 7, 'EOS' : 8, 'ETC' : 9, 'ADA' : 11, 'XLM' : 12, 'XMR' : 13, 'BSV' : 14, 'Index Value':19, 'Divisor Value':15}] 

    df = pd.DataFrame(data) 

    check = df_reorder(df, "complete")

    check = check.columns.to_list()

    ll = ["Date","Time","BTC","ETH","XRP","LTC","BCH","EOS","ETC","ZEC","ADA","XLM","XMR","BSV"]

    assert check == ll

    check = df_reorder(df, 'divisor')
    check = check.columns.to_list()

    ll = ["Date","Time","Divisor Value"]

    assert check == ll

    check = df_reorder(df, 'index')
    check = check.columns.to_list()

    ll = ["Date","Time","Index Value"]

    assert check == ll

    check = df_reorder(df, 'conversion')
    check = check.columns.to_list()

    ll = ["Time","Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]

    assert check == ll

def test_mongo_upload():

    df = pd.DataFrame(file_data) 

    mongo_upload(df, 'test_mongo')

    