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

    # asserting if with a wrong value raise the right error
    Crypto = "BT"

    check = api.coinbase_ticker(Crypto, Fiat, t_api)

    assert check == "This key doesn't exist"


def test_kraken_ticker():

    Crypto = "BTC"
    Fiat = "USD"

    check, pair = api.kraken_ticker(Crypto, Fiat, t_api)

    # when the btc asset is passed as argument
    # the function automatically convert it into xbt
    # to make the call for cerain pair it should be added
    # a x before the asset and z before de fiat
    assert check == "Everything is fine", pair == "XXBTCZUSD"

    Crypto = "Dollar"

    check = api.kraken_ticker(Crypto, Fiat, t_api)

    assert check == "This key doesn't exist"

    Crypto = "BTC"
    Fiat = "USDT"

    check, pair = api.kraken_ticker(Crypto, Fiat, t_api)

    assert check == "Everything is fine", pair == "XBTUSDT"
    

def test_
