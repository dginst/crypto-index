# third party import
from pymongo import MongoClient

# local import
import cryptoindex.data_setup as data_setup


# ################# setup mongo connection ##########################

# connecting to mongo in local
connection = MongoClient("localhost", 27017)
db = connection.index

# connecting to the collection
collection_ECB_clean = db.ecb_clean

# ################## ECB rates manipulation ############################

# define the array with all the currencies
key_curr_vector = ["USD", "GBP", "CAD", "JPY"]

cleaned_ecb = data_setup.ECB_daily_setup(key_curr_vector)

# ############# upload the manipulated data in MongoDB ##################

data = cleaned_ecb.to_dict(orient="records")
collection_ECB_clean.insert_many(data)
