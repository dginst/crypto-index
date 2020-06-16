from cryptoindex import API_request as api

from datetime import datetime
from pymongo import MongoClient


connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index
db.rawdata.create_index([("id", -1)])
# creating the empty collection rawdata within the database index
t_api = db.t_api


def test_coinbase_ticker():

    Crypto = "BTC"
    Fiat = "USD"

    check, pair = api.coinbase_ticker(Crypto, Fiat, t_api)

    assert check == "Everything is fine", pair == "BTCUSD"

    Crypto = "BT"

    check = api.coinbase_ticker(Crypto, Fiat, t_api)

    assert check == "Some problems occurred"
