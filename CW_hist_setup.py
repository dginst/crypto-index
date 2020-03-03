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

start_date = '01-01-2019'

# define today date as timestamp
today = datetime.now().strftime('%Y-%m-%d')
today_TS = int(datetime.strptime(today,'%Y-%m-%d').timestamp()) + 3600

# define the variable containing all the date from start_date to today.
# the date are displayed as timestamp and each day refers to 12:00 am UTC
reference_date_vector = data_setup.timestamp_gen(start_date)


pair_array = ['gbp', 'usd','eur', 'cad']#, 'gbp', 'usd', 'cad', 'jpy']#, 'eur', 'cad', 'jpy'] 
Crypto_Asset = ['ETH', 'BTC', 'BCH', 'LTC', 'XRP'] #BCH, LTC

# we use all the xchanges except for Kraken that needs some more test in order to be introduced without error
Exchanges = [ 'coinbase-pro', 'poloniex', 'bitstamp', 'gemini', 'bittrex', 'kraken', 'bitflyer'] 


####################################### setup mongo connection ###################################

#connecting to mongo in local
connection = MongoClient('localhost', 27017)
#creating the database called index
db = connection.index

#creating the empty collection cleandata within the database index
db.cleandata.create_index([ ("id", -1) ])

collection_raw = db.rawdata
collection_clean = db.cleandata
collection_ECB_raw = db.ecb_raw

##############################################################
# function takes as input a matrix with missing values referred to specific exchange, crypto and pair
# reference_array is the array of date of the period of interest, info_position can be: 
#(4=close price, 5= volume in crypto, 6=volume in pair)
# based on the info_pos choice the function returns a fixed vector that contain also the values obtained as volume weighted average
# (of close price, volume crypto or volume in pair) of the daily variations of every exchange in the crypto+pair 

def fix_missing(broken_matrix, exchange, cryptocurrency, pair, start_date, end_date = None):

    print('START FIX')
    # define DataFrame header
    header = ['Time', 'Close Price', 'Crypto Volume', 'Pair Volume']

    # set the index name and collection name to retrieve data from MongoDB
    db = "index"
    collection = "rawdata"

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    # creating the reference date array from start date to end date
    reference_array = data_setup.timestamp_gen(start_date, end_date)
    # select just the date on broken_matrix
    broken_array = broken_matrix['Time']
    ccy_pair = cryptocurrency + pair

    # set the list af all exchanges and then pop out the one in subject
    exchange_list = ['bitflyer', 'poloniex', 'bitstamp','bittrex','coinbase-pro','gemini','kraken']#aggungere itbit
    exchange_list.remove(exchange)
    print(exchange_list)

    # iteratively find the missing value in all the exchanges
    fixing_price = np.array([])
    fixing_cry_vol = np.array([])
    fixing_pair_vol = np.array([])
    fixing_volume = np.array([])

    # variable that count how many exchanges actually have values for the selected crypto+pair 
    count_exchange = 0

    for element in exchange_list:
        print(element)
        
        query_dict = {"Exchange" : element, "Pair": cp }
        matrix = mongo.query_mongo(db, collection, query_dict)
        matrix = matrix.drop(columns = ['Exchange', 'Pair', 'Low', 'High','Open'])
        print(matrix)

        # checking if data frame is empty: if not then the ccy_pair exists in the exchange
        # then add to the count variable 
        if matrix.shape[0] > 1:

            count_exchange = count_exchange + 1
    
            # if the matrix is not null, find variation and volume of the selected exchange
            # and assign them to the related matrix
            variations_price, volumes = substitute_finder(broken_array, reference_array, matrix, 'Close Price')
            variations_cry_vol, volumes = substitute_finder(broken_array, reference_array, matrix, 'Crypto Volume')
            variations_pair_vol, volumes = substitute_finder(broken_array, reference_array, matrix, 'Pair Volume')
            variation_time = variations_price[:,0]
            print(variations_price)
            if fixing_price.size == 0:

                fixing_price = variations_price[:,1]
                fixing_cry_vol = variations_cry_vol[:,1]
                fixing_pair_vol = variations_pair_vol[:,1]
                fixing_volume = volumes[:,1]

            else:

                fixing_price = np.column_stack((fixing_price, variations_price[:,1]))
                fixing_cry_vol = np.column_stack((fixing_cry_vol, variations_cry_vol[:,1]))
                fixing_pair_vol = np.column_stack((fixing_pair_vol, variations_pair_vol[:,1]))
                fixing_volume = np.column_stack((fixing_volume, volumes[:,1]))
    
    # find the volume weighted variation for all the variables
    weighted_var_price = []
    weighted_cry_vol = []
    weighted_pair_vol = []
    for i in range(len(fixing_price)): 
        count_none = 0 

        for j in range(count_exchange):

            try:

                if fixing_price[i,j] == 0:

                    count_none = count_none + 1

            except IndexError:
                pass


        # checking if single date is missing in ALL the exchanges
        # if yes assign zero variation (the previous day value will be taken)
        if count_none == count_exchange:

            weighted_var_price.append(0)
            weighted_cry_vol.append(0)
            weighted_pair_vol.append(0)

        # condition that assure: 1) not all values are 0, 2) there is more than 1 exchange (= more than 1 columns)
        # 3) if true, there is just an element to fix, so fixing variation is a 1d array
        elif count_none != count_exchange and count_exchange > 1 and fixing_price.size == count_exchange:

            price = fixing_price[i,:].sum() / fixing_volume[i,:].sum()
            cry_vol = fixing_cry_vol[i,:].sum() / fixing_volume[i,:].sum()
            pair_vol = fixing_pair_vol[i,:].sum() / fixing_volume[i,:].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)

        elif count_none != count_exchange and count_exchange == 1:

            price = fixing_price[i].sum() / fixing_volume[i].sum()
            cry_vol = fixing_cry_vol[i].sum() / fixing_volume[i].sum()
            pair_vol = fixing_pair_vol[i].sum() / fixing_volume[i].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)   
        # 
        elif count_none != count_exchange and count_exchange > 1 and fixing_price.size > count_exchange:

            price = fixing_price[i,:].sum() / fixing_volume[i,:].sum()
            cry_vol = fixing_cry_vol[i,:].sum() / fixing_volume[i,:].sum()
            pair_vol = fixing_pair_vol[i,:].sum() / fixing_volume[i,:].sum()
            weighted_var_price.append(price)
            weighted_cry_vol.append(cry_vol)
            weighted_pair_vol.append(pair_vol)


    # create a matrix with columns: timestamp date, weighted variatons of prices, weightes variations of volume both crypto and pair
    try:
        variation_matrix = np.column_stack((variation_time, weighted_var_price, weighted_cry_vol, weighted_pair_vol))
        print('variation matrix')
        print(variation_matrix)
        # sostituire variation matrix nan con 0
        for i, row in enumerate(variation_matrix[:,0]):

            previous_values = broken_matrix[broken_matrix['Time'] == row - 86400].iloc[:,1:4]
            print('previuos value')
            print(previous_values)

            new_values = previous_values * (1 + variation_matrix[i, 1:])

            new_values = pd.DataFrame(np.column_stack((row, new_values)), columns = header)
            broken_matrix = broken_matrix.append(new_values)
            broken_matrix = broken_matrix.sort_values(by = ['Time'])
            broken_matrix = broken_matrix.reset_index(drop = True)


        fixed_matrix = broken_matrix
        int_date = np.array([])
    
        for date in fixed_matrix['Time']:

            new_date = int(date)
            new_date = str(new_date)
            int_date = np.append(int_date, new_date)
        
        fixed_matrix['Time'] = int_date

    except UnboundLocalError:

        fixed_matrix = np.array([])

    return fixed_matrix



# given a matrix (where_to_lookup), a date reference array and, broken date array with missing date
# function returns two matrices:
# the first one is about the "position" information and can be "Close Price", "Crypto Volume" or "Pair Volume"
# where the first column contains the list of date that broken array misses and
# the second column contains the variations of the "position" info between T and T-1
# the second one contains the volume variations as seconda column and date as first


def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    print('FIXING...')

    # find the elements of ref array not included in broken array (the one to check)
    missing_item = data_setup.Diff(reference_array, broken_array)
    # print(reference_array)
    # print(broken_array)

    print(missing_item)
    variations = [] 
    volumes = []
    for element in missing_item:
        # for each missing element try to find it in where to look up, if KeyError occurred 
        # meaning the searched item is not found, then append zero
        try:
            today_alt = where_to_lookup[where_to_lookup['Time'] == element][position]
            today_value = float(where_to_lookup[where_to_lookup['Time'] == element][position])
            yesterday_value = float(where_to_lookup[where_to_lookup['Time'] == element - 86400][position])
            variation = (today_value - yesterday_value) / yesterday_value
            volume = float(where_to_lookup[where_to_lookup['Time'] == element]['Pair Volume']) ##consider crytpo vol
            variation = variation * volume
            variations.append(variation)
            volumes.append(volume)

        except KeyError:

            variations.append(0) 
            volumes.append(0)

        except TypeError:

            variations.append(0) 
            volumes.append(0)

    volumes = np.array(volumes)
    variations = np.array(variations)
    variation_matrix = np.column_stack((missing_item, variations))
    volume_matrix = np.column_stack((missing_item, volumes))

    return variation_matrix, volume_matrix

########################################################
for Crypto in Crypto_Asset:

    currencypair_array = []
    for i in pair_array:
        currencypair_array.append(Crypto.lower() + i)

    for exchange in Exchanges:
    
    
        for cp in currencypair_array:

            crypto = cp[:3]
            pair = cp[3:]


            # query data from mongo. Syntax : { "field" : "value", "field": "value" }, Syntax with logic operators(LO) : { "field" : {"$LO" : "value"} }
            # for daily calculation  it is necessary to put the third condition : {"Time" : x}
            db = "index"
            collection = "rawdata"
            query_dict = {"Exchange" : exchange, "Pair": cp }
            matrix = mongo.query_mongo(db, collection, query_dict)
            print(matrix)
            matrix = matrix.drop(columns = ['Exchange', 'Pair', 'Low', 'High','Open'])
            print(matrix.shape[0])

            # checking if the matrix is not empty
            if matrix.shape[0] > 1:

                # check if the historical series start at the same date as the stert date
                # if not fill the dataframe with zero values
                matrix = data_setup.homogenize_series(matrix, reference_date_vector)

                # checking if the matrix has missing data and if ever fixing it
                if matrix.shape[0] != reference_date_vector.size:
                    
                    matrix = fix_missing(matrix, exchange, Crypto, pair, start_date)
                    
                    # add exchange and currency_pair column
                    matrix['Exchange'] = exchange
                    matrix['Pair'] = cp

            data = matrix.to_dict(orient='records')  
            collection_clean.insert_many(data)



###################################################



