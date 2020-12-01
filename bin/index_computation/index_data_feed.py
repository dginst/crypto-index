# standard library import
from datetime import datetime, timezone

# third party import
from pymongo import MongoClient

# local import
import cryptoindex.data_setup as data_setup
import cryptoindex.mongo_setup as mongo
from cryptoindex.config import (DAY_IN_SEC, PAIR_ARRAY, CRYPTO_ASSET, EXCHANGES,
                                START_DATE, EXC_START_DATE)


# ############# INITIAL SETTINGS ################################

# set today
today_str = datetime.now().strftime("%Y-%m-%d")
today = datetime.strptime(today_str, "%Y-%m-%d")
today_TS = int(today.replace(tzinfo=timezone.utc).timestamp())


# define the array containing the date where the index uses CW feed data
CW_date_arr = data_setup.date_gen(START_DATE, EXC_START_DATE)
CW_date_str = [str(date) for date in CW_date_arr]

# ######################## setup MongoDB connection ###########################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
# creating the database called index
db = connection.index

# drop the pre-existing collection (if there is one)
db.index_data_feed.drop()

# creating the empty collection cleandata within the database index
db.index_data_feed.create_index([("id", -1)])
collection_feed = db.index_data_feed

# ############################## CW and EXC series union ###################

# defining the database name and the collection name
database = "index"
collection_CW = "CW_final_data"
collection_EXC = "EXC_final_data"

# downloading the EXC series from MongoDB
EXC_series = mongo.query_mongo(database, collection_EXC)
EXC_series = EXC_series[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]

# downloading the CW series from MongoDB and selecting only the date
# from 2016-01-01 to 2020-04-17
CW_series = mongo.query_mongo(database, collection_CW)
CW_sub_series = CW_series.loc[CW_series.Time.isin(CW_date_str)]
CW_sub_series = CW_sub_series[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]

# creting an unique dataframe containing the two different data source
data_feed = CW_sub_series.append(EXC_series, sort=True)
data_feed = data_feed[
    ["Time", "Close Price", "Crypto Volume", "Pair Volume", "Exchange", "Pair"]]
# put the converted data on MongoDB
data = data_feed.to_dict(orient="records")
collection_feed.insert_many(data)
