#this code block has to be put before all the download functions.
import API_request
import utils.API_request as api
from pymongo import MongoClient

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_raw = db.rawdata
collection_clean = db.cleandata

#-----------------------------------------------------------------------------------------------------------

# i don't know why, it stops after first cicle BTCUSD
    
def Coinbase_API(Start_Date='01-01-2017', End_Date='12-01-2019', Crypto = ['ETH', 'BTC'], Fiat=['USD','EUR'], granularity = '86400', ):

    date_object = api.date_gen_isoformat(Start_Date,End_Date,49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}

    for asset in Crypto:
        print(asset)
        for fiat in Fiat:
            print(fiat)    
            for start,stop in date_object:
                print(start,stop)
                
                entrypoint = 'https://api.pro.coinbase.com/products/'
                key = asset+"-"+fiat+"/candles?start="+start+"&end="+stop+"&granularity="+granularity
                
                request_url = entrypoint + key
                print(request_url)
                response = requests.get(request_url)
                sleep(0.3)
                response = response.json()
                
                #print(response)
                for i in range(len(response)):
                    
                    r = response
                    Exchange = 'Coinbase - Pro'
                    Pair = asset+fiat
                    Time = r[i][0]
                    Low  = r[i][1] 
                    High = r[i][2]
                    Open = r[i][3]
                    Close_Price = r[i][4]
                    Crypto_Volume = r[i][5]


                    rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time':Time, 'Low':Low, 'High':High, 'Open':Open, 'Close Price':Close_Price, 'Crypto Volume':Crypto_Volume}

                    collection.insert_one(rawdata)

            
    return response 


#function to easily query on mongo:
#db_name = name of the database
#coll_name = nome of the collection
#field = field of the search ( ex. time, pair etc)
#request = specific parameter for the search in the field
#all the inserted value must be string
#result will be a dictionary

def query_mongo(db_name, coll_name, field, request ):

    mydb = connection[db_name]
    mycol = mydb[coll_name]

    myquery = { field : request }

    mydoc = mycol.find(myquery)

    return mydoc

    




#function to import rawdata downloaded from CryptoWatch directly to MongoDB
#saves the data downloaded from mongo

def CW_mongoraw(exchange, response, asset, fiat ):

    for i in range(len(response)):
        r = response
        Exchange = Exchange
        Pair = asset+fiat
        Time = r[i][0]
        Open = r[i][1] 
        High = r[i][2]
        Low = r[i][3]
        Close_Price = r[i][4]
        Crypto_Volume = r[i][5]
        Quote_Volume = r[i][6]

        rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time' : Time, 'Low' : Low,
           'High' : High, 'Open' : Open, 'Close Price' : Close_Price,
           'Crypto Volume' : Crypto_Volume}
        
        collection.insert_one(rawdata)




#this function takes the pd dataframes, turns them in dictionary
#then it store the data in mongo db day-by-day
#dataframe = the dataframe that we want to insert in mongodb
#collection = the collection where we want to save the dataframe data

def CW_mongoclean(dataframe, collection ):

    #first transform the pandas dataframe to a dictionary
    data = dataframe.to_dict(orient='records')  # Here's our added param..
    collection.insert_many(data)

    return