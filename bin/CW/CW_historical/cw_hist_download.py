# ############################################################################
# The file download from the CryotoWatch websites the market data of this set
# of Cryptocurrencies: 'ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM', 'ADA',
# 'ZEC', 'XMR', 'EOS', 'BSV' and 'ETC'
# from this set of exchanges:
# 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini',
# 'bittrex', 'kraken' and 'bitflyer'
# and for each fiat currenncies in this set:
# 'gbp', 'usd', 'eur', 'cad' and 'jpy'
# Once downloaded the file saves the raw data on MongoDB in the database
# "index" and collection "CW_rawdata".
# #############################################################################

# standard library import
from datetime import datetime

# third party import
from pymongo import MongoClient

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download

# ################### initial settings #########################

start_date = "01-01-2016"

# set end_date as today, otherwise comment and choose an end_date
end_date = datetime.now().strftime("%m-%d-%Y")


# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.date_gen(start_date)

pair_array = ["gbp", "usd", "eur", "cad", "jpy", "usdt", "usdc"]
# pair complete = ['gbp', 'usd', 'cad', 'jpy', 'eur', 'usdt', 'usdc']
Crypto_Asset = [
    "BTC",
    "ETH",
    "XRP",
    "LTC",
    "BCH",
    "EOS",
    "ETC",
    "ZEC",
    "ADA",
    "XLM",
    "XMR",
    "BSV",
]
# crypto complete ['ETH', 'BTC', 'LTC', 'BCH', 'XRP', 'XLM',
#  'ADA', 'ZEC', 'XMR', 'EOS', 'BSV', 'ETC']
Exchanges = [
    "coinbase-pro",
    "poloniex",
    "bitstamp",
    "gemini",
    "bittrex",
    "kraken",
    "bitflyer",
]
# exchange complete = [ 'coinbase-pro', 'poloniex',
# 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer']

# ################# setup MongoDB connection #####################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.CW_rawdata.drop()

# creating the empty collection rawdata within the database index
db.rawdata.create_index([("id", -1)])
collection_raw = db.CW_rawdata

# ################# downloading and storing part ################

for Crypto in Crypto_Asset:
    print(Crypto)
    ccy_pair_array = []
    for i in pair_array:
        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:

        for cp in ccy_pair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting
            # to CryptoWatch website
            data_download.CW_raw_to_mongo(
                exchange, cp, collection_raw, start_date, end_date
            )
