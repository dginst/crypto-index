# third party packages
import pandas as pd

from pymongo import MongoClient

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# function that takes as arguments:
# database = database name [index_raw, index_cleaned, index_cleaned]
# collection = the name of the collection of interest
# query_dict = mongo db uses dictionary structure to do query ex:
# {"Exchange" : "kraken", "Pair" : "btcjpy", "Time" : { "$gte": 1580774400} },
#  this query call all the documents that contains kraken as exchange,
#  the pair btcjpy and the time value is greater than 1580774400


def query_mongo(database, collection, query_dict=None):

    # defining the variable that allows to work with MongoDB
    db = connection[database]
    coll = db[collection]

    #     # find in the selected collection the wanted element/s

    if query_dict is None:

        df = pd.DataFrame(list(coll.find()))

        try:

            df = df.drop(columns="_id")

        except AttributeError:

            df = []

        except KeyError:

            df = []

    else:

        df = pd.DataFrame(list(coll.find(query_dict)))

        try:

            df = df.drop(columns="_id")

        except AttributeError:

            df = []

        except KeyError:

            df = []

    return df


def mongo_index_conn():

    # connecting to mongo in local
    connection = MongoClient("localhost", 27017)

    db = connection.index

    return db


def mongo_indexing():

    db = mongo_index_conn()

    # CW related collections
    db.CW_rawdata.create_index([("id", -1)])
    db.CW_cleandata.create_index([("id", -1)])
    db.CW_volume_checked_data.create_index([("id", -1)])
    db.CW_converted_data.create_index([("id", -1)])
    db.CW_final_data.create_index([("id", -1)])
    db.CW_keys.create_index([("id", -1)])

    # EXC related collections
    db.EXC_cleandata.create_index([("id", -1)])
    db.EXC_final_data.create_index([("id", -1)])
    db.EXC_keys.create_index([("id", -1)])

    # ECB and stable coin rates collections
    db.ecb_raw.create_index([("id", -1)])
    db.ecb_clean.create_index([("id", -1)])
    db.stable_coin_rates.create_index([("id", -1)])

    # index computation collections
    db.index_data_feed.create_index([("id", -1)])
    db.all_exc_volume.create_index([("id", -1)])
    db.crypto_price.create_index([("id", -1)])
    db.crypto_volume.create_index([("id", -1)])
    db.crypto_price_return.create_index([("id", -1)])
    db.index_EWMA.create_index([("id", -1)])
    db.index_logic_matrix_one.create_index([("id", -1)])
    db.index_logic_matrix_two.create_index([("id", -1)])
    db.index_EWMA_logic_checked.create_index([("id", -1)])
    db.index_weights.create_index([("id", -1)])
    db.index_divisor.create_index([("id", -1)])
    db.index_divisor_reshaped.create_index([("id", -1)])
    db.index_synth_matrix.create_index([("id", -1)])
    db.index_rel_synth_matrix.create_index([("id", -1)])
    db.index_level_1000.create_index([("id", -1)])
    db.index_level_raw.create_index([("id", -1)])

    return None


def mongo_coll():

    db = mongo_index_conn()

    dict_of_coll = {

        # CW related collections
        "collection_cw_raw": db.CW_rawdata,
        "collection_cw_clean": db.CW_cleandata,
        "collection_cw_final_data": db.CW_final_data,
        "collection_cw_converted": db.CW_converted_data,
        "collection_cw_vol_check": db.CW_volume_checked_data,
        "collection_CW_key": db.CW_keys,
        # EXC related collections
        "collection_exc_clean": db.EXC_cleandata,
        "collection_exc_final_data": db.EXC_final_data,
        "collection_EXC_key": db.EXC_keys,
        # ECB and stable coin rates collections
        "collection_ecb_raw": db.ecb_raw,
        "collection_ecb_clean": db.ecb_clean,
        "collection_stable_rate": db.stable_coin_rates,
        # index computation collections
        "collection_data_feed": db.index_data_feed,
        "collection_all_exc_vol": db.all_exc_volume,
        "collection_price": db.crypto_price,
        "collection_volume": db.crypto_volume,
        "collection_price_ret": db.crypto_price_return,
        "collection_EWMA": db.index_EWMA,
        "collection_logic_one": db.index_logic_matrix_one,
        "collection_logic_two": db.index_logic_matrix_two,
        "collection_EWMA_check": db.index_EWMA_logic_checked,
        "collection_divisor": db.index_divisor,
        "collection_divisor_reshaped": db.index_divisor_reshaped,
        "collection_synth": db.index_synth_matrix,
        "collection_relative_synth": db.index_rel_synth_matrix,
        "collection_weights": db.index_weights,
        "collection_index_level_1000": db.index_level_1000,
        "collection_index_level_raw": db.index_level_raw,
        # test

        'test_mongo': db.test_mongo

    }

    return dict_of_coll


def mongo_coll_drop(operation):

    db = mongo_index_conn()

    if operation == "ecb_hist_d":

        db.ecb_raw.drop()

    elif operation == "ecb_hist_s":

        db.ecb_clean.drop()

    elif operation == "cw_hist_down":

        db.CW_rawdata.drop()

    elif operation == "cw_hist_clean":

        db.CW_cleandata.drop()
        db.CW_volume_checked_data.drop()

    elif operation == "cw_hist_conv":

        db.CW_converted_data.drop()
        db.CW_final_data.drop()
        db.stable_coin_rates.drop()
        db.CW_keys.drop()
        db.EXC_keys.drop()

    elif operation == "index_hist":

        db.crypto_price.drop()
        db.crypto_volume.drop()
        db.all_exc_volume.drop()
        db.crypto_price_return.drop()
        db.index_weights.drop()
        db.index_level_1000.drop()
        db.index_level_raw.drop()
        db.index_EWMA.drop()
        db.index_logic_matrix_one.drop()
        db.index_logic_matrix_two.drop()
        db.index_EWMA_logic_checked.drop()
        db.index_divisor.drop()
        db.index_divisor_reshaped.drop()
        db.index_synth_matrix.drop()
        db.index_rel_synth_matrix.drop()

    elif operation == "index_feed":

        db.index_data_feed.drop()

    elif operation == "exc":

        db.EXC_final_data.drop()
        db.EXC_cleandata.drop()

    return None


def df_reorder(df_to_reorder, column_set):

    if column_set == "complete":

        reordered_df = df_to_reorder[
            [
                "Date",
                "Time",
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
        ]

    elif column_set == "divisor":

        reordered_df = df_to_reorder[
            [
                "Date",
                "Time",
                "Divisor Value"
            ]
        ]

    elif column_set == "index":

        reordered_df = df_to_reorder[
            [
                "Date",
                "Time",
                "Index Value"
            ]
        ]

    elif column_set == "conversion":

        reordered_df = df_to_reorder[
            [
                "Time",
                "Close Price",
                "Crypto Volume",
                "Pair Volume",
                "Exchange",
                "Pair"
            ]
        ]

    return reordered_df


def mongo_upload(data_to_upload, where_to_upload, reorder="N", column_set_val=None):

    collection_dict = mongo_coll()

    if reorder == "Y":

        data_to_upload = df_reorder(
            data_to_upload, column_set=column_set_val)

    data_to_dict = data_to_upload.to_dict(orient="records")
    collection_dict.get(where_to_upload).insert_many(data_to_dict)

    return None
