import utils.data_setup as data_setup
import utils.data_download as data_download
from pymongo import MongoClient
import time
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import pandas as pd
import requests
from requests import get
import mongoconn as mongo

start = time.time()


#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
db.rawdata.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_ECB_raw = db.ecb_raw
collection_ECB_clean = db.ecb_clean



def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
    print(date)
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']
    Exchange_Rate_List = pd.DataFrame()
    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])
    for i, single_date in enumerate(date):
        time.sleep(0.5)
        # retrieving data from ECB website
        single_date_ex_matrix = mongo.ECB_rates_extractor(key_curr_vector, date[i])

        if Exchange_Rate_List.size == 0:
            Exchange_Rate_List = single_date_ex_matrix
        else:
            Exchange_Rate_List = Exchange_Rate_List.append(single_date_ex_matrix, sort=True)

    return Exchange_Rate_List

########################## import the raw ecb data to mongo
key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']
rates = ECB_setup(key_curr_vector, '2016-01-02', '2020-02-12')
data = rates.to_dict(orient='records')  
collection_ECB_raw.insert_many(data)

######################## query 
query_dict = {}
query = mongo.query_mongo(index, ecb_raw,query_dict)


  

# end = time.time()

# print("This script took: {} minutes".format(int((end-start)/60)))



###



# def ECB_rates_extractor(key_curr_vector, Start_Period, End_Period = None, freq = 'D', 
#                         curr_den = 'EUR', type_rates = 'SP00', series_var = 'A'):
    
#     Start_Period = data_setup.date_reformat(Start_Period, '-', 'YYYY-MM-DD')
#     # set end_period = start_period if empty
#     if End_Period == None:
#         End_Period = Start_Period
#     else:
#         End_Period = data_setup.date_reformat(End_Period, '-', 'YYYY-MM-DD')

#     # API settings
#     entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
#     resource = 'data'           
#     flow_ref = 'EXR'
#     param = {
#         'startPeriod': Start_Period, 
#         'endPeriod': End_Period    
#     }

#     Exchange_Rate_List = pd.DataFrame()
#     pd.options.mode.chained_assignment = None

#     for i, currency in enumerate(key_curr_vector):
#         key = freq + '.' + currency + '.' + curr_den + '.' + type_rates + '.' + series_var
#         request_url = entrypoint + resource + '/' + flow_ref + '/' + key
        
#         # API call
#         response = requests.get(request_url, params = param, headers = {'Accept': 'text/csv'})
        
#         # if data is empty, it is an holiday, therefore exit
#         try:
#             Data_Frame = pd.read_csv(io.StringIO(response.text))
#         except:
#             break
        
#         Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE', 'CURRENCY', 'CURRENCY_DENOM'], axis=1)
#         print(Main_Data_Frame)
#         if currency == 'USD':
#             cambio_USD_EUR = float(Main_Data_Frame['OBS_VALUE'])

#         # 'TIME_PERIOD' was of type 'object' (as seen in Data_Frame.info). Convert it to datetime first
#         Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])
        
#         # Set 'TIME_PERIOD' to be the index
#         Main_Data_Frame = Main_Data_Frame.set_index('TIME_PERIOD')
        
#         if Exchange_Rate_List.size == 0:
#             Exchange_Rate_List = Main_Data_Frame
#             Exchange_Rate_List['USD based rate'] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR
#         else:
#             Exchange_Rate_List = Exchange_Rate_List.append(Main_Data_Frame, sort=True)
#             Exchange_Rate_List['USD based rate'][i] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR

#     return Main_Data_Frame



#     key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']


#     rates = ECB_rates_extractor(key_curr_vector, '2020-01-01', '2020-01-04')

#     print(rates)

   