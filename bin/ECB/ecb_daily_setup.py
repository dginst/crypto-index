
from cryptoindex.data_setup import ECB_daily_setup
from cryptoindex.mongo_setup import (
    mongo_coll, mongo_upload, mongo_indexing
)
from cryptoindex.config import ECB_FIAT

# ################ setup MongoDB connection ################

# creating the empty collection cleandata within the database index
mongo_indexing()

collection_dict_upload = mongo_coll()

# ################## ECB rates cleaning ############################

cleaned_ecb = ECB_daily_setup(ECB_FIAT)

# ############# upload data on MongoDB ##################

mongo_upload(cleaned_ecb, "collection_ecb_clean")
