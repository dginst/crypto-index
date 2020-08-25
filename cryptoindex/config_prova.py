from cryptoindex.mongo_setup import query_mongo

db_name = "index"
# coll_data = "index_data_feed"
coll_data = "CW_final_dat"

a = query_mongo(db_name, coll_data)

