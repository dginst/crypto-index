import pandas as pd
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import utils.data_download as data_download
import mongoconn as mongo
from pymongo import MongoClient



# function that generate an array of date in timstamp format starting from start_date to end_date
# given in mm-dd-yyyy forma; if not specified end_date = today() 
# function only returns value of timestamp in second since epoch where every day is in the exact 12:00 am UTC
# function considers End of Day price series so, if not otherwise specified,
# the returned array of date will be from start to today - 1 (EoD = 'Y')

def timestamp_gen(start_date, end_date = None,  EoD = 'Y'):

    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    end_date = datetime.strptime(end_date,'%m-%d-%Y')
    end = int(time.mktime(end_date.timetuple()))
    end = end + 3600
    start = datetime.strptime(start_date,'%m-%d-%Y')
    start = int(time.mktime(start.timetuple()))
    start = start + 3600

    array = np.array([start])
    date = start
   
    while date < end:
        date = date + 86400
        array = np.append(array, date)
    
    array = array[:len(array) - 1]

    return array


# 
def timestamp_convert(date_array):

    new_array = np.array = ([])

    for date in date_array:

        new_date = datetime.fromtimestamp(date)
        new_date = new_date.strftime('%Y-%m-%d')
        new_array = np.append(new_array, new_date)

    return new_array




def timestamp_vector(start, stop, lag = 86400):

    array = np.array([start])

    single_date = start + lag
    while single_date != stop:

        array = np.append(array, single_date)
        single_date = single_date + lag

    return array




# function that generate an array of date starting from start_date to end_date
# if not specified end_date = today() 
# default format is in second since the epoch (timeST = 'Y'), type timeST='N' for date in format YY-mm-dd
# function considers End of Day price series so, if not otherwise specified,
# the returned array of date will be from start to today - 1
# write all date in MM/DD/YYYY format

def date_array_gen(start_date, end_date = None, timeST = 'Y', EoD = 'Y'):

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    date_index = pd.date_range(start_date, end_date)
    
    DateList = date_list(date_index, timeST)

    if EoD == 'Y':
        DateList = DateList[:len(DateList)-1]

    return DateList




# given a start date and a period (number of days) the function returns an array containing
# the "period" date going back from the start date (default) or starting from the start date 
# (direction='forward') the output can be both in timestamp since epoch (default) or in date 
# MM/DD/YYYY (timeST='N')

def period_array(start_date, period, direction = 'backward', timeST='Y'):

    start_date = date_reformat(start_date, '-')

    if direction == 'backward':
        end_date = datetime.strptime(start_date,'%m-%d-%Y') - timedelta(days = period) 
        date_index = pd.date_range(end_date, start_date)

    else:
        end_date = datetime.strptime(start_date,'%m-%d-%Y') + timedelta(days = period) 
        date_index = pd.date_range(start_date, end_date)
    
    DateList = date_list(date_index, timeST)
    
    return DateList



# TODO: add description 
def date_list(date_index, timeST = 'Y', lag_adj = 3600):
    
    DateList = []
    
    for date in date_index:
        val = int(time.mktime(date.timetuple()))
        val = val + lag_adj
        DateList.append(val)
   
    NoStamp = []
    if timeST =='N':
        for string in DateList:
            value = int(string)
            NoStamp.append(datetime.utcfromtimestamp(value).strftime('%Y-%m-%d'))
        return NoStamp
    else:
        return DateList

####################### DATA FIXING FUNCTIONS ###################################


# return a list containing the elements of list_1 (bigger one) non included in list_2 (smaller one)

def Diff(list_1, list_2): 
    
    return (list(set(list_1) - set(list_2))) 



# return a sorted array of the size of reference_array.
# if there are more elements in ref array, broken_array is filled with the missing elements
# broken_array HAS TO BE smaller than reference array
# default sorting is in ascending way, if descending is needed specify versus='desc'

def fill_time_array(broken_array, ref_array, versus = 'asc'):
    
    difference = Diff(ref_array, broken_array)
    
    for element in difference:
        broken_array.add(element)
    broken_array = list(broken_array)
    
    if versus == 'desc':
        broken_array.sort(reverse = True)
    
    else:
        broken_array.sort()
    
    return broken_array



# function that given a list of items, find the items and relative indexes in another list/vector
# if one or more items in list_to_find are not included in where_to_find the function return None as position
# the return matrix have items as first column and index as second column

def find_index(list_to_find, where_to_find):

    list_to_find = np.array(list_to_find)
    where_to_find = np.array(where_to_find)
    index = []
    item = []
    for element in list_to_find:

        if element in where_to_find:
            i, = np.where(where_to_find == element)
            index.append(i)
            item.append(element)
        else:
            index.append(None)
            item.append(element)

    index = np.array(index)
    item = np.array(item)
    indexed_item = np.column_stack((item, index))
    indexed_item = indexed_item[indexed_item[:,0].argsort()]

    return indexed_item


# function that aims to homogenize the crypto-fiat series

def homogenize_series(series_to_check, reference_date_array_TS, days_to_check = 30):

    reference_date_array_TS = np.array(reference_date_array_TS)
    header = list(series_to_check.columns)
    test_matrix = series_to_check.loc[series_to_check.Time.between(reference_date_array_TS[0], reference_date_array_TS[days_to_check], inclusive = True), header]

    if test_matrix.empty == True:

        first_date = np.array(series_to_check['Time'].iloc[0])
        last_missing_date = (int(first_date) - 86400)
        first_missing_date = reference_date_array_TS[0]

        missing_date_array = timestamp_vector(first_missing_date, first_date)

        new_series = pd.DataFrame(missing_date_array, columns = ['Time'])

        header.remove('Time')
        for element in header:

            new_series[element] = np.zeros(len(missing_date_array))


        complete_series = new_series.append(series_to_check)
        
    else:

        complete_series = series_to_check
    
    complete_series = complete_series.reset_index(drop = True)
    #print(complete_series)
    return complete_series



# given a matrix (where_to_lookup), a date reference array and, broken date array with missing date
# function returns two matrices:
# the first one is about the "position" information and can be "Close Price", "Crypto Volume" or "Pair Volume"
# where the first column contains the list of date that broken array misses and
# the second column contains the variations of the "position" info between T and T-1
# the second one contains the volume variations as seconda column and date as first


def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    print('FIXING...')
    # find the elements of ref array not included in broken array (the one to check)
    missing_item = Diff(reference_array, broken_array)
    # print(reference_array)
    # print(broken_array)
    # print(missing_item)
    # print(len(missing_item))
    variations = [] 
    volumes = []
    for element in missing_item:
        
        # for each missing element try to find it in where to look up, if KeyError occurred 
        # meaning the searched item is not found, then append zero
        try:
            today_alt = where_to_lookup[where_to_lookup['Time'] == element][position]
            # print(today_alt)
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

    volumes = np.array(volumes)
    variations = np.array(variations)
    variation_matrix = np.column_stack((missing_item, variations))
    volume_matrix = np.column_stack((missing_item, volumes))

    return variation_matrix, volume_matrix



# function takes as input a matrix with missing values referred to specific exchange, crypto and pair
# reference_array is the array of date of the period of interest, info_position can be: 
#(4=close price, 5= volume in crypto, 6=volume in pair)
# based on the info_pos choice the function returns a fixed vector that contain also the values obtained as volume weighted average
# (of close price, volume crypto or volume in pair) of the daily variations of every exchange in the crypto+pair 

def fix_missing(broken_matrix, exchange, cryptocurrency, pair, start_date, end_date = None):

    print('START FIX')
    # define DataFrame header
    header = ['Time', 'Close Price', 'Crypto Volume', 'Pair Volume']

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    # creating the reference date array from start date to end date
    reference_array = timestamp_gen(start_date, end_date)
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

    for elements in exchange_list:
        
        # create a data frame connecting to CryptoWatch API
        matrix = data_download.CW_data_reader(elements, ccy_pair, start_date, end_date)
        # print(matrix)
        # checking if data frame is empty: if not then the ccy_pair exists in the exchange
        # then add to the count variable 
        if Check_null(matrix) == False:
            count_exchange = count_exchange + 1
            print(elements)
            # if the matrix is not null, find variation and volume of the selected exchange
            # and assign them to the related matrix
            variations_price, volumes = substitute_finder(broken_array, reference_array, matrix, 'Close Price')
            variations_cry_vol, volumes = substitute_finder(broken_array, reference_array, matrix, 'Crypto Volume')
            variations_pair_vol, volumes = substitute_finder(broken_array, reference_array, matrix, 'Pair Volume')
            variation_time = variations_price[:,0]
            # print(variations_price)
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
    # print(count_exchange)
    # print(fixing_price.size)
    # print(fixing_price)
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


        # checking if single date is missing in all the exchanges
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

        for i, row in enumerate(variation_matrix[:,0]):

            previous_values = broken_matrix[broken_matrix['Time'] == row - 86400].iloc[:,1:4]

            new_values = previous_values * (1 + variation_matrix[i, 1:])

            new_values = pd.DataFrame(np.column_stack((row, new_values)), columns = header)
            broken_matrix = broken_matrix.append(new_values)
            broken_matrix = broken_matrix.sort_values(by = ['Time'])
            broken_matrix = broken_matrix.reset_index(drop = True)


    # # finds all the previous prices, crypto volume and pair volume
    # previous_price = find_previous(index_list[:,1], broken_matrix['Close Price'])
    # previous_cry_vol = find_previous(index_list[:,1], broken_matrix['Crypto Volume'])
    # previous_pair_vol = find_previous(index_list[:,1], broken_matrix['Pair Volume']) ###########

    # # compute the values to insert
    # price_to_insert = (weighted_var_price + 1) * previous_price
    # cryvol_to_insert = (weighted_cry_vol + 1) * previous_cry_vol
    # pairvol_to_insert = (weighted_pair_vol + 1) * previous_pair_vol 

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



#function takes a .json file from Cryptowatch API and transforms it into a matrix
# the matrix has the headers : ['Time' ,'Open',	'High',	'Low',	'Close',""+Crypto+" Volume" , ""+Pair+" Volume"]
#if the downloaded file does not have results the function returns an empty array
#note that the "time" column contains value in timestamp format
#86400 is the daily frequency in seconds

def json_to_matrix(file_path, Crypto='',Pair=''):
    raw_json=pd.read_json(file_path)
    if Crypto == '':
        Crypto="Crypto"
    if Pair=='':
        Pair="Pair"
    header=['Time' ,'Open',	'High',	'Low',	'Close Price',""+Crypto+" Volume" , ""+Pair+" Volume"]
    if "result" in raw_json.keys(): #testing if the file has the 'result' list of value
        matrix= pd.DataFrame(raw_json['result']['86400'], columns=header)
        # for i, element in enumerate(matrix['time']):
        #     matrix['time'][i]=datetime.strptime(str(datetime.fromtimestamp(matrix['time'][i]))[:10], '%Y-%m-%d').strftime('%d/%m/%y')
    else:
        matrix=np.array([])
    
    return matrix

# function that checks if a item(array, matrix, string,...) is null

def Check_null(item): 
    try:
        return len(item) == 0 
    except TypeError: 
        pass 
    return False 

# function that reforms the inserted date according to the choosen separator
# function takes as input date with "/" seprator and without separator for 
# both the YY ans YYYY format; works on default with MM-DD_YYYY format and 
# if different has to be specified ('YYYY-DD-MM' or 'YYYY-MM-DD')

def date_reformat(date_to_check, separator = '-', order = 'MM-DD-YYYY'):
    if ("/" in date_to_check and len(date_to_check) == 10):
        return_date = date_to_check.replace("/", separator)  
    elif ("/" in date_to_check and len(date_to_check) == 8):
        return_date = date_to_check.replace("/", separator)
    elif ("/" not in date_to_check and (len(date_to_check) == 8 or len(date_to_check) == 6)):
        if (order == 'YYYY-DD-MM' or order == 'YYYY-MM-DD'):
            return_date = date_to_check[:4] + separator + date_to_check[4:6] + separator + date_to_check[6:]
        else:
            return_date = date_to_check[:2] + separator + date_to_check[2:4] + separator + date_to_check[4:]
    else:
        return_date = date_to_check

    return return_date


###################### DAILY AND HISTORICAL ECB RATES SETUP FUNCTIONS #############

# function returns a matrix of exchange rates USD based that contains
# Date, Exchange indicator (ex. USD/GBP) and rate of a defined period
# retrieving data from the website of European Central Bank
# the function, if data is missing (holiday and weekends), finds the first previous
# day with data and takes its values
# inputs are:
# key_curr_vector that passes the list of currencies of interest
# start_Period and End_Period

def ECB_setup(key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date_TS = timestamp_gen(Start_Period, End_Period, EoD = 'N')
    date = timestamp_convert(date_TS)
    date = [datetime.strptime(x, '%Y-%m-%d') for x in date]

    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    
    # Exchange_Matrix = np.array([])
 

    for i, single_date in enumerate(date):

        database= 'index'
        collection = 'ecb_raw'
        query = {'TIME_PERIOD': date[i] } 

       
        # retrieving data from MongoDB 'index' and 'ecb_raw' collection
        single_date_ex_matrix = mongo.query_mongo(database, collection, query)

        # check if rates exist in the specified date
        if Check_null(single_date_ex_matrix) == False:
            
            # find the USD/EUR rates useful for conversions
            cambio_USD_EUR = float(np.array(single_date_ex_matrix.loc[single_date_ex_matrix.CURRENCY == 'USD', 'OBS_VALUE']))
            
            # add a column to DF with the USD based rates
            single_date_ex_matrix['USD based rate'] = (single_date_ex_matrix['OBS_VALUE']) / cambio_USD_EUR

            # creat date array
            date_arr = np.full(len(key_curr_vector),single_date)
          
            # creating the array with 'XXX/USD' format
            curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
            curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
          
            # creating the array with rate values USD based
            rate_arr = single_date_ex_matrix['USD based rate']
            rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)
    
            # stacking the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))
         
            # filling the return matrix
            if Exchange_Matrix.size == 0:

                Exchange_Matrix = array

            else:

                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

        # if the query returns an empty matrix, function will takes values of the
        # last useful day        
        else:

            date_arr = np.full(len(key_curr_vector),single_date)

            # take the curr_arr values of the previous day
            curr_arr = curr_arr

            # take the rate_arr values of the pevious day
            rate_arr = rate_arr

            # stack the array together
            array = np.column_stack((date_arr, curr_arr, rate_arr))

            if Exchange_Matrix.size == 0:
                
                Exchange_Matrix = array

            else:

                Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    if timeST != 'N':

        for j, element in enumerate(Exchange_Matrix[:,0]):

            to_date = datetime.strptime(element, '%Y-%m-%d')
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j ,0] = int(time_stamp)


    return pd.DataFrame(Exchange_Matrix, columns = header)



# function returns a matrix of exchange rates USD based that contains
# Date, Exchange indicator (ex. USD/GBP) and rate for TODAY
# retrieving data from the website of European Central Bank
# the function, if data is missing (holiday and weekends), finds the first previous
# day with data and takes its values
# input:
# key_curr_vector that passes the list of currencies of interest

def ECB_daily_setup (key_curr_vector, timeST = 'N'):

    # defining the array of date to be used
    today = datetime.now().strftime('%m-%d-%Y')
    yesterday = (datetime.now() - timedelta(days = 1)).strftime('%m-%d-%Y')
    date = date_array_gen(yesterday, today, timeST = 'N', EoD = 'N')
    date = [datetime.strptime(x, '%Y-%m-%d') for x in date]

    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])

    # defining the MongoDB path where to look for the rates
    database= 'index'
    collection = 'ecb_raw'
    query = {'TIME_PERIOD': date[1]} 
    
    # retrieving data from MongoDB 'index' and 'ecb_raw' collection
    single_date_ex_matrix = mongo.query_mongo(database, collection, query)

    # check if rates exist in the specified date
    if Check_null(single_date_ex_matrix) == False:
        
        # find the USD/EUR rates useful for conversions
        cambio_USD_EUR = float(np.array(single_date_ex_matrix.loc[single_date_ex_matrix.CURRENCY == 'USD', 'OBS_VALUE']))
        
        # add a column to DF with the USD based rates
        single_date_ex_matrix['USD based rate'] = (single_date_ex_matrix['OBS_VALUE']) / cambio_USD_EUR

        # creat date array
        date_arr = np.full(len(key_curr_vector), date[1])
        
        # creating the array with 'XXX/USD' format
        curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
        curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
        
        # creating the array with rate values USD based
        rate_arr = single_date_ex_matrix['USD based rate']
        rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)

        # stacking the array together
        array = np.column_stack((date_arr, curr_arr, rate_arr))
        
        # filling the return matrix
        if Exchange_Matrix.size == 0:

            Exchange_Matrix = array

        else:

            Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    # if the query returns an empty matrix, function will takes values of the
    # last useful day        
    else:

        # set the MongoDB query to yestarday
        query = {'TIME_PERIOD': date[0]} 
    
        # retrieving data from MongoDB 'index' and 'ecb_raw' collection 
        single_date_ex_matrix = mongo.query_mongo(database, collection, query)

        date_arr = np.full(len(key_curr_vector), date[1])

        # creating the array with 'XXX/USD' format
        curr_arr = single_date_ex_matrix['CURRENCY'] + '/USD'
        curr_arr = np.where(curr_arr == 'USD/USD', 'EUR/USD', curr_arr)
        
        # creating the array with rate values USD based
        rate_arr = single_date_ex_matrix['USD based rate']
        rate_arr = np.where(rate_arr == 1.000000, 1/single_date_ex_matrix['OBS_VALUE'][0], rate_arr)

        # stack the array together
        array = np.column_stack((date_arr, curr_arr, rate_arr))

        if Exchange_Matrix.size == 0:
            
            Exchange_Matrix = array

        else:

            Exchange_Matrix = np.row_stack((Exchange_Matrix, array))

    if timeST != 'N':

        for j, element in enumerate(Exchange_Matrix[:,0]):

            to_date = datetime.strptime(element, '%Y-%m-%d')
            time_stamp = datetime.timestamp(to_date) + 3600
            Exchange_Matrix[j, 0] = int(time_stamp)


    return pd.DataFrame(Exchange_Matrix, columns = header)

#################################################################################


# function returns the Data Frame relative to a specified exchange/crypto/pair
# with the "pair" value converted in USD, more specifically converts the columns
# 'Close Price' and 'Pair Volume' into USD
# function takes as input:
# CW_matrix = CryptoWatch dataframe to be changed
# Ex_Rate_matrix = data frame of ECB exchange rates
# currency = string that specify the currency of CW_matrix (EUR, CAD, GBP,...)

def CW_data_setup (CW_matrix, currency):

    currency = currency.upper()

    if currency != 'USD':
            
        ex_curr = currency + '/USD'

    
        #connecting to mongo in local
        connection = MongoClient('localhost', 27017)
        db = connection.index

        # defining the MongoDB path where to look for the rates
       
        for i in range (len(CW_matrix['Time'])):
            
            date = str(CW_matrix['Time'][i])

           # defining the MongoDB path where to look for the rates 
            database= 'index'
            collection = 'ecb_clean'
            query = {'Date': date, 'Currency' : ex_curr} 
        
            # retrieving data from MongoDB 'index' and 'ecb_raw' collection
            single_date_rate = mongo.query_mongo(database, collection, query)
            print(single_date_rate)
            if single_date_rate.shape[0] == 1:

                rate = single_date_rate['Rate']

            else:

                single_date_rate = single_date_rate.iloc[0]

            
            print(single_date_rate.shape[0])
            print(single_date_rate.iloc[0])
            rate = single_date_rate['Rate']

            CW_matrix['Close Price'][i] = float(CW_matrix['Close Price'][i] / rate)
            CW_matrix['Pair Volume'][i] = float(CW_matrix['Pair Volume'][i] / rate)
    
    else:

        CW_matrix = CW_matrix

    return CW_matrix
        

##############################




