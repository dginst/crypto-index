from cryptoindex.mongo_setup import (
    mongo_coll, mongo_indexing)

mongo_indexing()
collection_dict_upload = mongo_coll()

### just for testing ####
my = {'Time': "1595462400"}
my2 = {"Exchange": "coinbase-pro", "Pair": "xrpgbp"}
collection_dict_upload.get("collection_cw_converted").delete_many(my)
collection_dict_upload.get("collection_cw_final_data").delete_many(my)
