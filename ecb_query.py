import utils.data_setup as data_setup
import utils.data_download as data_download
from pymongo import MongoClient
import time
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import *
import time
import pandas as pd
import requests
from requests import get
import mongoconn as mongo

#manca solo trasformare la data in timestamp

###### function that manipulate ecb data querying them from mongo

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index
#index
db.ecb_raw.create_index([ ("id", -1) ])
#creating the empty collection rawdata within the database index
collection_ECB_clean = db.ecb_clean

#for historical ecb

def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = data_setup.date_array_gen(Start_Period, End_Period, timeST = 'N')
    date = [datetime.strptime(x, '%Y-%m-%d') for x in date ]
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    for i, single_date in enumerate(date):

        database= 'index'
        collection = 'ecb_raw'
        query = {'TIME_PERIOD': date[i] } 
       
        # retrieving data from ECB website
        single_date_ex_matrix = ecb_query = mongo.query_mongo(database, collection, query)
        print(single_date_ex_matrix)

        try:
            cambio_USD_EUR = float(single_date_ex_matrix['OBS_VALUE'][0])
            single_date_ex_matrix = single_date_ex_matrix.set_index('TIME_PERIOD')
            single_date_ex_matrix['USD based rate'] = (single_date_ex_matrix['OBS_VALUE']) / cambio_USD_EUR
        except:
            print('ciao2')
        
        if data_setup.Check_null(single_date_ex_matrix) == False:

            date_arr = np.full(len(key_curr_vector),single_date)
            print(date_arr)
            # creating the array with 'XXX/USD' format
            curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
            
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
            print(curr_arr)
            # creating the array with rate values USD based
            # since ECB displays rate EUR based some changes needs to be done
            rate_arr = single_date_ex_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)
            print(curr_arr)

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

            exception_date = date[i] - timedelta(days = 1)
            query = {'TIME_PERIOD': exception_date }
            exception_matrix = mongo.query_mongo(database, collection, query)

            try:
                cambio_USD_EUR = float(exception_matrix['OBS_VALUE'][0])
                exception_matrix = exception_matrix.set_index('TIME_PERIOD')
                exception_matrix['USD based rate'] = (exception_matrix['OBS_VALUE']) / cambio_USD_EUR
            except:
                print('ciao3')

            while data_setup.Check_null(exception_matrix) != False:

                exception_date = exception_date - timedelta(days = 1)
                query = {'TIME_PERIOD': exception_date } 
                exception_matrix = mongo.query_mongo(database, collection, query)

                try:
                    cambio_USD_EUR = float(exception_matrix['OBS_VALUE'][0])
                    exception_matrix = exception_matrix.set_index('TIME_PERIOD')
                    exception_matrix['USD based rate'] = (exception_matrix['OBS_VALUE']) / cambio_USD_EUR
                
                except:
                    print('ciao4')

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

Start_Period = '2020-02-17'
End_Period = '2020-02-18'
key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

mongo_clean = ECB_setup(key_curr_vector, Start_Period, End_Period)
print(type(mongo_clean['Date'].loc[0]))
print(type(mongo_clean['Date'].loc[0]))
print(mongo_clean)
data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)

#for daily ecb

Start_Period = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
End_Period = datetime.today().strftime('%Y-%m-%d')
key_curr_vector = ['USD', 'GBP', 'CAD', 'JPY']

mongo_clean = ECB_setup(key_curr_vector, Start_Period, End_Period)
print(type(mongo_clean['Date'].loc[0]))
print(type(mongo_clean['Date'].loc[0]))
print(mongo_clean)
data = mongo_clean.to_dict(orient='records')  
collection_ECB_clean.insert_many(data)