connection = MongoClient('localhost', 27017)
db = connection.index
db.tweets.ensure_index("id", unique=True, dropDups=True)
collection = db.rawdata

 collection.save(x)
