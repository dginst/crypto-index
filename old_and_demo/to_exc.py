from pymongo import MongoClient
import cryptoindex.mongo_setup as mongo

connection = MongoClient('localhost', 27017)
# creating the database called index
db = connection.index
db.EXC_rawdata.create_index([("id", -1)])
# creating the empty collection rawdata within the database index

collection = db.EXC_rawdata
db = 'index'

# coinbasetraw

try:

    coll = 'coinbasetraw'

    df = mongo.query_mongo(db, coll)

    df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                            'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

    df['Time'] = df['Time'].apply(str)

    df['Pair'] = df['Pair'].str.lower()

    df = df.to_dict(orient='records')

    collection.insert_many(df)
except:
    print('none_coinbase')


# bitflyertraw


coll = 'bitflyertraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)


# krakentraw

coll = 'krakentraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)


# bitstamptraw

coll = 'bitstamptraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)

# geminitraw

coll = 'geminitraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)

# itbittraw

coll = 'itbittraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)

# bittrextraw

coll = 'bittrextraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)

# poloniextraw

coll = 'poloniextraw'

df = mongo.query_mongo(db, coll)

df = df.rename(columns={'pair': 'Pair', 'exchange': 'Exchange',
                        'time': 'Time', 'price': 'Close Price', 'volume': 'Crypto Volume'})

df['Time'] = df['Time'].apply(str)

df['Pair'] = df['Pair'].str.lower()

df = df.to_dict(orient='records')

collection.insert_many(df)
