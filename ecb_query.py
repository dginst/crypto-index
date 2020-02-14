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

###### function that manipulate ecb data querying them from mongo

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
#index
db.ecb_raw.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_ECB_clean = db.ecb_clean


def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']
    database = 'index'
    collection = 'ecb_raw'
    Exchange_Rate_List = pd.DataFrame()
    
    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    for i, single_date in enumerate(date):
        for currency in key_curr_vector:
            #ripartire dalla query
            query = {"CURRENCY": currency, 'TIME_PERIOD': date[i]}
            # retrieving data from ECB website
            ecb_query = mongo.query_mongo(database, collection, query)
            print(ecb_query)
            if ecb_query['CURRENCY'].loc[0] == 'USD':
                cambio_USD_EUR = float(ecb_query['OBS_VALUE'])

            # Set 'TIME_PERIOD' to be the index
            Main_Data_Frame = ecb_query.set_index('TIME_PERIOD')
            
            if Exchange_Rate_List.size == 0:
                Exchange_Rate_List = Main_Data_Frame
                Exchange_Rate_List['USD based rate'] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR
            else:
                Exchange_Rate_List = Exchange_Rate_List.append(Main_Data_Frame, sort=True)
                Exchange_Rate_List['USD based rate'][i] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR

    
            # check if the API actually returns values 
            if Check_null(Exchange_Rate_List) == False:

                date_arr = np.full(len(key_curr_vector),single_date)
                # creating the array with 'XXX/USD' format
                curr_arr = Exchange_Rate_List['CURRENCY'] + '/USD'
                curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
                # creating the array with rate values USD based
                # since ECB displays rate EUR based some changes needs to be done
                rate_arr = Exchange_Rate_List['USD based rate']
                rate_arr = np.where(rate_arr == 1.000000, 1/Exchange_Rate_List['OBS_VALUE'][0], rate_arr)

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
                date_str = exception_date.strftime('%Y-%m-%d')            
                exception_matrix = mongo.query_mongo(database, collection, query)

                while Check_null(exception_matrix) != False:

                    exception_date = exception_date - timedelta(days = 1)
                    date_str = exception_date.strftime('%Y-%m-%d') 
                    exception_matrix = mongo.query_mongo(database, collection, query)

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


    return pd.DataFrame(Exchange_Matrix, columns = header)

Start_Period = '2016-01-01'
End_Period = '2019-02-13'
key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

mongo_clean = ECB_setup(key_curr_vector, Start_Period, End_Period)
data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)


