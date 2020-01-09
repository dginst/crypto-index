import pandas as pd
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time
import data_download



# function that generate an array of date starting from start_date to end_date
# if not specified end_date = today() 
# default format is in second since the epoch, type timeST='N' for date in format YY-mm-dd
# write all date in MM/DD/YYYY format

def date_array_gen(start_date, end_date = None, timeST = 'Y'):

    # set end_date = today if empty
    if end_date == None:
        end_date = datetime.now().strftime('%m-%d-%Y')

    date_index = pd.date_range(start_date, end_date)
    
    DateList = date_list(date_index, timeST)
    
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
def date_list(date_index, timeST = 'Y'):
    
    DateList=[]
    
    for date in date_index:
        DateList.append(str(int(time.mktime(date.timetuple()))))
    
    if timeST=='N':
        for string in DateList:
            value = int(string)
            DateList.append(datetime.utcfromtimestamp(value).strftime('%Y-%m-%d'))

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
# the second column contains the relative weighted variations between T and T-1, the third column contains the T volume
# specified by "position"(4=close price, 5= volume in crypto, 6=volume in pair)

def substitute_finder(broken_array, reference_array, where_to_lookup, position):

    # find the elements of ref array not included in broken array (the one to check)
    missing_item = Diff(reference_array, broken_array)
    # find the position in a matrix (row index) of each elements missing in broken array
    indexed_list = find_index(missing_item, where_to_lookup[:,1])

    weighted_variations = [] #####se sono zero tutti? se più di uno è zero? deve completrae al 100% dei casi #####
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



#given an index list containing the index value of certain items as founded in where_to_insert
#the function return an array of element in position i-1, searched in where_to_insert

def find_previous(index_list, where_to_insert): 

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
    broken_array=broken_matrix[:,0]
    ccy_pair=cryptocurrency+pair

    # set the list af all exchanges and then pop out the one in subject
    exchange_list = ['bitflyer', 'bitfinex', 'poloniex', 'bitstamp','bittrex','coinbase-pro','gemini','kraken']#aggungere itbit
    exchange_list.remove(exchange)

    # iteratively find the missing value in all the exchanges
    fixing_variation = np.array([])
    fixing_volume = np.array([])
    for elements in exchange_list:

        matrix = data_download.CW_data_reader(elements, ccy_pair, start_date, end_date)
        # TODO : add exception if API does not work
        variations, volumes = substitute_finder(broken_array, reference_array, matrix, info_position)

        if fixing_variation.size == 0:
            fixing_variation = variations[:,1]
            fixing_volume = volumes[:,1]
        else:
            fixing_variation = np.column_stack((fixing_variation, variations[:,1]))
            fixing_volume = np.column_stack((fixing_volume, volumes[:,1]))

    # find the volume weighted variation and then the value to insert multiplying
    # the average variation with the previuos value
    for i in range(len(reference_array)):
        count_none = 0
        ####to be finished

    weighted_variation_value = fixing_variation.sum(axis = 1) / fixing_volume.sum(axis = 1)
    index_list = find_index(variations[:,0], broken_matrix[:,0])
    previous_values = find_previous(index_list[:,1], broken_matrix[:, info_position])
    value_to_insert  = weighted_variation_value * previous_values

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

def date_reformat(date_to_check, separator, order = 'MM-DD-YYYY'):
    if ("/" in date_to_check and len(date_to_check) == 10):
        return_date = date_to_check.replace("/", separator)  
    elif ("/" in date_to_check and len(date_to_check) == 8):
        return_date = date_to_check.replace("/", separator)
    elif ("/" not in date_to_check and (len(date_to_check) == 8 or len(date_to_check) == 6)):
        if (order == 'YYYY-DD-MM' or order == 'YYYY-MM-DD'):
            return_date = date_to_check[:4] + separator + date_to_check[4:6] + separator + date_to_check[6:]
        else:
            return_date = date_to_check[:2] + separator + date_to_check[2:4] + separator + date_to_check[4:]
    return return_date