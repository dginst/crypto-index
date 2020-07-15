from cryptoindex.config import MONGO_DICT

from cryptoindex.mongo_setup import *

print(MONGO_DICT)

mongo_indexing()

a = mongo_coll()

print(a.get("collection_weights"))
