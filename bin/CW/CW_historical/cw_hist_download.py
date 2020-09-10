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

import pandas as pd

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.data_download as data_download
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_coll_drop, mongo_indexing, mongo_upload)
from cryptoindex.config import (
    START_DATE, PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES, CW_RAW_HEAD)

# ################### initial settings #########################


# set end_date as today, otherwise comment and choose an end_date
end_date = datetime.now().strftime("%m-%d-%Y")


# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.date_gen(START_DATE)


# ################# setup MongoDB connection #####################

mongo_coll_drop("cw_hist_d")

mongo_indexing()

collection_dict_upload = mongo_coll()

# ################# downloading and storing part ################
df = pd.DataFrame(columns=CW_RAW_HEAD)

for Crypto in CRYPTO_ASSET:
    print(Crypto)

    ccy_pair_array = []
    for i in PAIR_ARRAY:

        ccy_pair_array.append(Crypto.lower() + i)

    for exchange in EXCHANGES:

        for cp in ccy_pair_array:

            crypto = cp[:3]
            pair = cp[3:]
            # create the matrix for the single currency_pair connecting
            # to CryptoWatch website
            df = data_download.cw_raw_download(
                exchange, cp, df, START_DATE, end_date
            )

print("download finished")
mongo_upload(df, "collection_cw_raw")
