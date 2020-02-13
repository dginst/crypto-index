#this code block has to be put before all the download functions.
import API_request as api
from pymongo import MongoClient
from datetime import *
from requests import get
import time
import requests
import os
import pandas as pd
import io
import numpy as np
import utils.data_setup as data_setup

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_raw = db.rawdata
collection_clean = db.cleandata
collection_ECB_raw = db.ecb_raw
collection_ECB_clean = db.ecb_clean

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

###########################################################################################################################################################################################

def CW_dato_reader(exchange, currencypair, start_date = '01-01-2016', end_date = None, periods='86400'):

    
    Crypto = currencypair[:3].upper()
    Pair = currencypair[3:].upper()
    
    # check date format
    start_date = data_setup.date_reformat(start_date)
    start_date = datetime.strptime(start_date, '%m-%d-%Y')

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')
    else:
        end_date = data_setup.date_reformat(end_date, '-')
    end_date = datetime.strptime(end_date, '%m-%d-%Y')

    # transform date into timestamps
    start_date = str(int(time.mktime(start_date.timetuple())))
    end_date = str(int(time.mktime(end_date.timetuple())))

    # API settings
    entrypoint = 'https://api.cryptowat.ch/markets/' 
    key = exchange + "/" + currencypair + "/ohlc?periods=" + periods + "&after=" + start_date + "&before=" + end_date
    request_url = entrypoint + key
    
    # API call
    response = requests.get(request_url)
    response = response.json()
    #print(len(response))
    
    try: 
        for i in range(len(response['result']['86400'])):
            
            r = response['result']['86400']
            #print(r)
            Exchange = exchange
            Pair = currencypair
            Time = r[i][0]
            Open  = r[i][1] 
            High = r[i][2]
            Low = r[i][3]
            Close_Price = r[i][4]
            Crypto_Volume = r[i][5]
            Pair_Volume = r[i][6]

            rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time':Time, 'Low':Low, 'High':High, 'Open':Open, 'Close Price':Close_Price, 'Crypto Volume':Crypto_Volume, 'Pair Volume': Pair_Volume}

            collection_raw.insert_one(rawdata)

    except:
        
        r = response
        Exchange = exchange
        Pair = currencypair
        Time = 0
        Open  = 0
        High = 0
        Low = 0
        Close_Price = 0
        Crypto_Volume = 0
        Pair_Volume = 0


        rawdata = { 'Exchange' : Exchange, 'Pair' : Pair, 'Time':Time, 'Low':Low, 'High':High, 'Open':Open, 'Close Price':Close_Price, 'Crypto Volume':Crypto_Volume, 'Pair Volume': Pair_Volume}

        collection_raw.insert_one(rawdata)

    return  

###################################################################################################################à
# function that takes as arguments:
# database = database name [index_raw, index_cleaned, index_cleaned]
# collection = the name of the collection of interest
# query_dict = mongo db uses dictionary structure to do query ex:  
# {"Exchange" : "kraken", "Pair" : "btcjpy", "Time" : { "$gte": 1580774400} }, this query call all the documents 
# that contains kraken as exchange, the pair btcjpy and the time value is greater than 1580774400

def query_mongo(database, collection, query_dict):

    db = connection[database]
    coll = db[collection]
    
    myquery = query_dict
    doc = coll.find(myquery)
    

    matrix= pd.DataFrame.from_records(doc)
    matrix = matrix.drop(columns = ['_id','Exchange', 'Pair','Open', 'High', 'Low'])

    
    return matrix


####################################################################################################################################################################################################


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
            #print("pinsoglio")
            #print(Data_Frame)
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



def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])
    for i, single_date in enumerate(date):
        
        # retrieving data from ECB website
        single_date_ex_matrix = ECB_rates_extractor_with_mongo(key_curr_vector, date[i])
        
        # check if the API actually returns values 
        if data_setup.Check_null(single_date_ex_matrix) == False:

            date_arr = np.full(len(key_curr_vector),single_date)
            # creating the array with 'XXX/USD' format
            curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
            # creating the array with rate values USD based
            # since ECB displays rate EUR based some changes needs to be done
            rate_arr = single_date_ex_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)

            # stacking the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            # filling the return matrix
            if Exchange_Matrix.size == 0:
                Exchange_Matrix = array
            else:
                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

        # if the first API call returns an empty matrix, function will takes values of the
        # last useful day        
        else:

            exception_date = datetime.strptime(date[i], '%Y-%m-%d') - timedelta(days = 1)
            print('pirandello')
            print (exception_date)
            date_str = exception_date.strftime('%Y-%m-%d')            
            exception_matrix = ECB_rates_extractor_with_mongo(key_curr_vector, date_str)
            print('la maschera')
            print(exception_matrix)
            while data_setup.Check_null(exception_matrix) != False:

                exception_date = exception_date - timedelta(days = 1)
                date_str = exception_date.strftime('%Y-%m-%d') 
                exception_matrix = ECB_rates_extractor_with_mongo(key_curr_vector, date_str)

            date_arr = np.full(len(key_curr_vector),single_date)
            curr_arr = exception_matrix['CURRENCY'] + '/USD'
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
            rate_arr = exception_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/exception_matrix['OBS_VALUE'][0], rate_arr)
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            if Exchange_Matrix.size == 0:
                Exchange_Matrix = array
            else:
                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))
    
    if timeST != 'N':

        for j, element in enumerate(Exchange_Matrix[:,0]):

            to_date = datetime.strptime(element, '%Y-%m-%d')
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j,0] = int(time_stamp)
            
            fin_matrix = pd.DataFrame(Exchange_Matrix, columns = header)

            data = fin_matrix.to_dict(orient='records')  
            collection_ECB_clean.insert_many(data)


    return fin_matrix


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

#def query_ecb_mongo(key_curr_vector):
#
 #   for curr in key_curr_vector:

#        db = connection["index"]
 #       coll = db["ecb_raw"]

#        myquery = { "TIME_PERIOD": "2016-01-04", "CURRENCY": curr }

##        doc = coll.find(myquery)
        
#        dataframe = pd.DataFrame(mydoc)

 #   return dataframe

#####################################################################################################
########################  FUNCTIONS TO QUERY MANIPULATED DATA IN MONGO  #############################
#####################################################################################################

def ECB_rates_extractor(key_curr_vector, Start_Period, End_Period = None, freq = 'D', 
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
        response = get(request_url, params = param, headers = {'Accept': 'text/csv'})
        
        # if data is empty, it is an holiday, therefore exit
        try:
            Data_Frame = pd.read_csv(io.StringIO(response.text))
        except:
            break
        
        Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE', 'CURRENCY', 'CURRENCY_DENOM'], axis=1)
    
        Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])

        if Exchange_Rate_List.size == 0:
            Exchange_Rate_List = Main_Data_Frame
        else:
            Exchange_Rate_List = Exchange_Rate_List.append(Main_Data_Frame, sort=True)


        
    return Exchange_Rate_List
