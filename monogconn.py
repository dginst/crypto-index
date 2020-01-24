#this code block has to be put before all the download functions.
import API_request

from pymongo import MongoClient
connection = MongoClient('localhost', 27017)
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
collection = db.rawdata

#-----------------------------------------------------------------------------------------------------------

# i don't know why, it stops after first cicle BTCUSD

def Coinbase_API(Start_Date='01-01-2019', End_Date='12-01-2019', Crypto = ['BTC', 'ETH'], Fiat=['USD'], granularity = '86400', ):

    date_object = API_request.date_gen_isoformat('01-01-2019','12-01-2019',49)
    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}
    
    for asset in Crypto:

        for fiat in Fiat:
            
            for start,stop in date_object:

                entrypoint = 'https://api.pro.coinbase.com/products/'
                key = assets+"-"+fiat+"/candles?start="+start+"&end="+stop+"&granularity="+granularity
                request_url = entrypoint + key

                response = requests.get(request_url)
                sleep(0.25)

                response = response.json()
               
               # it saves each day in mongo. To do that a for loop is utilized.  
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