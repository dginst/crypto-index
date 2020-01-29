#this code block has to be put before all the download functions.
import API_request
import utils.API_request as api
from pymongo import MongoClient
import utils.data_download as data_download

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_raw = db.rawdata
collection_clean = db.cleandata
collection_ECB_raw = db.ecb_raw

#-----------------------------------------------------------------------------------------------------------
#####################################################################################################
##########################  FUNCTIONS TO SAVE RAW DATA IN MONGO  ####################################
#####################################################################################################
    
def Coinbase_API(Start_Date='01-01-2017', End_Date='12-01-2019', Crypto = ['ETH', 'BTC'], Fiat=['USD','EUR'], granularity = '86400', ):

    df = np.array([])
    header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
    d = {}

    for asset in Crypto:
        print(asset)
        for fiat in Fiat:
            date_object = api.date_gen_isoformat(Start_Date,End_Date,49)
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

                    collection_raw.insert_one(rawdata)

            
    return response 



#function to import rawdata downloaded from CryptoWatch directly to MongoDB
#saves the data downloaded from mongo

def CW_mongoraw(exchange, response, asset, fiat ):

    for i in range(len(response)):
        r = response
        Exchange = exchange
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
        'Crypto Volume' : Crypto_Volume, 'Quote_Volume' = Quote_Volume}
        
        collection_raw.insert_one(rawdata)






#this function adds to the def ecb rates exctrator stored in data download to code lines
#to add all the raw downloaded data from ecb to mongo

def ECB_rates_extractor_with_mongo(key_curr_vector, Start_Period, End_Period = None, freq = 'D', 
                        curr_den = 'EUR', type_rates = 'SP00', series_var = 'A'):
    
    Start_Period = data_setup.date_reformat(Start_Period, '-', 'YYYY-MM-DD')
    # set end_period = start_period if empty
    if End_Period == None:
        End_Period = Start_Period
    else:
        End_Period = data_setup.date_reformat(End_Period, '-', 'YYYY-MM-DD')

    # API settings
    entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
    resource = 'data'           
    flow_ref = 'EXR'
    param = {
        'startPeriod': Start_Period, 
        'endPeriod': End_Period    
    }

    Exchange_Rate_List = pd.DataFrame()
    pd.options.mode.chained_assignment = None

    for i, currency in enumerate(key_curr_vector):
        key = freq + '.' + currency + '.' + curr_den + '.' + type_rates + '.' + series_var
        request_url = entrypoint + resource + '/' + flow_ref + '/' + key
        
        # API call
        response = requests.get(request_url, params = param, headers = {'Accept': 'text/csv'})
        
        # if data is empty, it is an holiday, therefore exit
        try:
            Data_Frame = pd.read_csv(io.StringIO(response.text))
        except:
            break
        
        Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE', 'CURRENCY', 'CURRENCY_DENOM'], axis=1)

        if currency == 'USD':
            cambio_USD_EUR = float(Main_Data_Frame['OBS_VALUE'])

        # 'TIME_PERIOD' was of type 'object' (as seen in Data_Frame.info). Convert it to datetime first
        Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])
        data = Main_Data_Frame.to_dict(orient='records')  
        collection_ECB_raw.insert_many(data)

        # Set 'TIME_PERIOD' to be the index
        Main_Data_Frame = Main_Data_Frame.set_index('TIME_PERIOD')
        
        if Exchange_Rate_List.size == 0:
            Exchange_Rate_List = Main_Data_Frame
            Exchange_Rate_List['USD based rate'] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR
        else:
            Exchange_Rate_List = Exchange_Rate_List.append(Main_Data_Frame, sort=True)
            Exchange_Rate_List['USD based rate'][i] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR

    return Exchange_Rate_List




#####################################################################################################
########################  FUNCTIONS TO SAVE MANIPULATED DATA IN MONGO  ##############################
#####################################################################################################



#this function takes the pd dataframes, turns them in dictionary
#then it store the data in mongo db day-by-day
#dataframe = the dataframe that we want to insert in mongodb
#collection = the collection where we want to save the dataframe data

def CW_mongoclean(dataframe, collection ):

    #first transform the pandas dataframe to a dictionary
    data = dataframe.to_dict(orient='records')  # Here's our added param..
    collection_clean.insert_many(data)

    return


#####################################################################################################
########################  FUNCTIONS TO QUERY RAW DATA IN MONGO  #####################################
#####################################################################################################

#this function query the ecbraw data stored in mongo.
# in this case the function query all the data that in index.ecb_raw, respect
# time period = 2016-01-04 and the currency selected from key_curr_vector

def query_ecb_mongo(key_curr_vector):

    for curr in key_curr_vector:

        db = connection["index"]
        coll = db["ecb_raw"]

        myquery = { "TIME_PERIOD": "2016-01-04", "CURRENCY": curr }

        doc = coll.find(myquery)
        
        dataframe = pd.DataFrame(mydoc)

    return dataframe

#####################################################################################################
########################  FUNCTIONS TO QUERY MANIPULATED DATA IN MONGO  #############################
#####################################################################################################