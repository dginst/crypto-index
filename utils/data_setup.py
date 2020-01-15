import pandas as pd
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import utils.data_download as data_download



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



# given a matrix (where_to_lookup), a date reference array and, broken date array with missing date
# function returns a matrix where the first column contains the list of date that broken array miss
# the second column contains the relative weighted variations between T and T-1, the third column contains
# the T volume specified by "position"(4=close price, 5= volume in crypto, 6=volume in pair)

def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    # find the elements of ref array not included in broken array (the one to check)
    missing_item = Diff(reference_array, broken_array)
    # find the position in a matrix (row index) of each elements missing in broken array
    indexed_list = find_index(missing_item, where_to_lookup['Time'])

    # setting position as column name
    header = ['Time', 'Open', 'High', 'Low', 'Close Price', "Crypto Volume", "Pair Volume"]
    position = header[position]

    weighted_variations = [] #####se sono zero tutti? se più di uno è zero? deve completare al 100% dei casi #####
    volumes = []
    for element in indexed_list[:,1]:

        if element != None:
            variation = (where_to_lookup(element, position) - where_to_lookup(element - 1, position)) / where_to_lookup(element -1 , position)
            volume = where_to_lookup(element, 6)
            weighted_variation = variation * volume
            weighted_variations.append(weighted_variation)
            volumes.append(volume)
        else:
            weighted_variations.append(None) ## 0 or None?
            volumes.append(None)

    volumes = np.array(volumes)
    weighted_variations = np.array(weighted_variations)
    variation_matrix = np.column_stack((indexed_list[:,0], weighted_variations))
    volume_matrix = np.column_stack((indexed_list[:,0], volumes))

    return variation_matrix, volume_matrix


# function that fills a vector with specified elelments in specified positions
# function takes as input:
# where_to_insert = list where insert the item included in what_to_insert
# what_to_insert = list of items to be inserted
# index_list = the index position that the items should have in where_to_insert
# the function returns where_to_insert updated with the missing items 

def insert_items(what_to_insert, where_to_insert, index_list):

    index_list = np.array(index_list)
    what_to_insert = np.array(what_to_insert)
    where_to_insert = np.array(where_to_insert)

    for i,index in enumerate(index_list):
        where_to_insert = np.insert(where_to_insert,
        index,what_to_insert[i])

    return where_to_insert



# function returns an array of elements in position i-1, as founded in where_to_insert, according
# to the position contained in index_list
# function takes as input:
# index_list = a list/array of indexes (position) related to elelments of interest
# where_to_insert = a vector where search the previous elements according to the index given

def find_previous(index_list, where_to_insert): 

    # turn vectors into numpy array
    index_list = np.array(index_list)
    where_to_insert = np.array(where_to_insert)
    previous_list = []

    for index in index_list:
        previous_list.append(where_to_insert[index - 1])

    return np.array(previous_list)



# function takes as input a matrix with missing values referred to specific exchange, crypto and pair
# reference_array is the array of date of the period of interest, info_position can be: 
#(4=close price, 5= volume in crypto, 6=volume in pair)
# based on the info_pos choice the function returns a fixed vector that contain also the values obtained as volume weighted average
# (of close price, volume crypto or volume in pair) of the daily variations of every exchange in the crypto+pair 

def fix_missing(broken_matrix, exchange, cryptocurrency, pair, info_position, start_date, end_date = None):

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    # creating the reference date array from start date to end date
    reference_array = date_array_gen(start_date, end_date)
    # select just the date on broken_matrix
   ### broken_array = broken_matrix[:,0]
    broken_array = broken_matrix['Time']
    ccy_pair = cryptocurrency + pair

    # set the list af all exchanges and then pop out the one in subject
    exchange_list = ['bitflyer', 'bitfinex', 'poloniex', 'bitstamp','bittrex','coinbase-pro','gemini','kraken']#aggungere itbit
    exchange_list.remove(exchange)

    # iteratively find the missing value in all the exchanges
    fixing_variation = np.array([])
    fixing_volume = np.array([])
    weighted_variation_value = np.array([])
    count_exchange = 0
    for elements in exchange_list:

        # create a data frame connecting to CryptoWatch API
        matrix = data_download.CW_data_reader(elements, ccy_pair, start_date, end_date)
        # checking if data frame is empty: if not then the ccy_pair exists in the exchange
        if Check_null(matrix) == False:
            count_exchange = count_exchange + 1

            # if the matrix is not null, find variation and volume of the selected exchange
            # and assign them to the related matrix
            variations, volumes = substitute_finder(broken_array, reference_array, matrix, info_position)
            if fixing_variation.size == 0:
                fixing_variation = variations[:,1]
                fixing_volume = volumes[:,1]
            else:
                fixing_variation = np.column_stack((fixing_variation, variations[:,1]))
                fixing_volume = np.column_stack((fixing_volume, volumes[:,1]))

    # find the volume weighted variation and then the value to insert multiplying
    # the average variation with the day_lag value
    print(fixing_variation)
    for i in range(len(reference_array)):
        count_none = 0
        for j in range(len(exchange_list)):
            if fixing_variation[i,j] == None:
                count_none = count_none + 1
                fixing_variation[i,j] = 0
                fixing_volume[i,j] = 0

        # checking if single date is missing in all the exchanges
        # if yes assign zero variation (the prevoius day value will be taken)
        # if no compute the weighted variation
        if count_none == count_exchange:
            weighted_variation_value[i] = 0
        else:
            weighted_variation_value[i] = fixing_variation[i].sum(axis = 1) / fixing_volume[i].sum(axis = 1)

    # find related index, day_lag value and compute the value to insert 
    index_list = find_index(variations[:,0], broken_matrix[:,0])
    previous_values = find_previous(index_list[:,1], broken_matrix[:, info_position])
    value_to_insert = (weighted_variation_value + 1) * previous_values

    # inserting the computed value into the vector
    fixed_column = insert_items(index_list, value_to_insert, broken_matrix[:, info_position])

    return fixed_column



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


# function returns a matrix of exchange rates USD based that contains
# Date, Exchange indicator (ex. USD/GBP) and rate of a defined period
# retrieving data from the website of European Central Bank
# the function, if data is missing (holiday and weekends), finds the first previous
# day with data and takes its values
# inputs are:
# key_curr_vector that passes the list of currencies of interest
# start_Period and End_Period

def ECB_setup (key_curr_vector, Start_Period, End_Period, timeST = 'N'):

    # defining the array of date to be used
    date = date_array_gen(Start_Period, End_Period, timeST = 'N')
    # defining the headers of the returning data frame
    header = ['Date', 'Currency', 'Rate']

    # for each date in "date" array the funcion retrieves data from ECB website
    # and append the result in the returning matrix
    Exchange_Matrix = np.array([])
    for i, single_date in enumerate(date):
        
        # retrieving data from ECB website
        single_date_ex_matrix = data_download.ECB_rates_extractor(key_curr_vector, date[i])
        
        # check if the API actually returns values 
        if Check_null(single_date_ex_matrix) == False:

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
            date_str = exception_date.strftime('%Y-%m-%d')            
            exception_matrix = data_download.ECB_rates_extractor(key_curr_vector, date_str)

            while Check_null(exception_matrix) != False:

                exception_date = exception_date - timedelta(days = 1)
                date_str = exception_date.strftime('%Y-%m-%d') 
                exception_matrix = data_download.ECB_rates_extractor(key_curr_vector, date_str)

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



# function returns the Data Frame relative to a specified exchange/crypto/pair
# with the "pair" value converted in USD, more specifically converts the columns
# 'Close Price' and 'Pair Volume' into USD
# function takes as input:
# CW_matrix = CryptoWatch dataframe to be changed
# Ex_Rate_matrix = data frame of ECB exchange rates
# currency = string that specify the currency of CW_matrix (EUR, CAD, GBP,...)

def CW_data_setup (CW_matrix, Ex_Rate_matrix, currency):

    currency = currency.upper()

    if currency != 'USD':
            
        ex_curr = currency + '/USD'

        for i in range ((CW_matrix.shape[0])):
            
            date = CW_matrix['Time'][i]
            rate = Ex_Rate_matrix[(Ex_Rate_matrix['Date'] == date) & (Ex_Rate_matrix['Currency'] == ex_curr)]
            new_close = CW_matrix['Close Price'][i] / rate['Rate']
            CW_matrix['Close Price'][i] = int(CW_matrix['Close Price'][i] / rate['Rate'])
            CW_matrix['Pair Volume'][i] = int(CW_matrix['Pair Volume'][i] / rate['Rate'])
    
    else:
        CW_matrix = CW_matrix

    return CW_matrix
        
