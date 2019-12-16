import pandas as pd
import numpy as np
import json
import os.path
from pathlib import Path
from datetime import datetime
from datetime import *
import time

#function that generate an array of date starting from start_date to end_date
# if not specified end_date = today() 
# default format is in second since the epoch, type timeST='N' for date in format YY-mm-dd
#write all date in MM/DD/YYYY format
def date_array_gen(start_date, end_date ='',timeST='Y'):
    if end_date == '':
        today=datetime.now().strftime('%m-%d-%Y')
        dateIndex=pd.date_range(start_date,today)
    else:
        dateIndex=pd.date_range(start_date,end_date)
    DateTSlist=[]
    DateList=[]
    for date in dateIndex:
        DateTSlist.append(str(int(time.mktime(date.timetuple()))))
    if timeST=='N':
        for string in DateTSlist:
            value=int(string)
            DateList.append(datetime.utcfromtimestamp(value).strftime('%Y-%m-%d'))
        return DateList
    return DateTSlist

#given a start date and a period (number of days) the function returns an array containing
#the "period" date going back from the start date (default) or starting from the start date (direction='forward')
#the output can be both in timestamp since epoch (default) or in date MM/DD/YYYY (timeST='N')
def period_array(start_date, period, direction = 'backward', timeST='Y'):
    if "/" in start_date:
        start_date=start_date.replace("/","-")  
    if direction=='forward':
        end_date=datetime.strptime(start_date,'%m-%d-%Y')+ timedelta(days=period) 
        dateIndex=pd.date_range(start_date,end_date)
    else:
        end_date=datetime.strptime(start_date,'%m-%d-%Y')- timedelta(days=period) 
        dateIndex = pd.date_range(end_date,start_date)
    DateTSlist=[]
    DateList=[]
    for date in dateIndex:
        DateTSlist.append(str(int(time.mktime(date.timetuple()))))
    if timeST=='N':
        for string in DateTSlist:
            value=int(string)
            DateList.append(datetime.utcfromtimestamp(value).strftime('%Y-%m-%d'))
        return DateList
    return DateTSlist


#return a list containing the elements of list_1 (bigger one) non included in list_2 (smaller one)
def Diff(list_1, list_2): 
    return (list(set(list_1) - set(list_2))) 

#return a sorted array of the size of reference_array.
# if there are more element in ref array, broken_array is filled with the missing elements
# broken_array HAS TO BE smaller than reference vector
# default sorting is in ascending way, if descending is needed specify versus='desc'
def fill_time_array(broken_array, reference_array,versus='asc'):
    difference=Diff(reference_array,broken_array)
    for element in difference:
        broken_array.add(element)
    broken_array=list(broken_array)
    if versus=='desc':
        broken_array.sort(reverse=True)
    else:
        broken_array.sort()
    return broken_array

# def find_index(broken_array, reference_array):
#     difference=Diff(reference_array,broken_array)
#     broken_array=np.array(broken_array)
#     reference_array=np.array(reference_array)
#     index=[]
#     for element in difference:
#         i, = np.where(reference_array==element)
#         index.append(i)
#     return np.array(index)

# function that given a list of item, find the items and relative indexes in another list/vector
# if one or more items in list_to_find are not included in where_to_find the fucntion simply go ahead
# the return matrix have items as first column and index as second column
def find_index(list_to_find, where_to_find):
    list_to_find=np.array(list_to_find)
    where_to_find=np.array(where_to_find)
    index=[]
    item=[]
    for element in list_to_find:
        if element in where_to_find:
            i, = np.where(where_to_find==element)
            index.append(i)
            item.append(element)
    
    index=np.array(index)
    item=np.array(item)
    indexed_item=np.column_stack((item,index))
    indexed_item=indexed_item[indexed_item[:,0].argsort()]
    return indexed_item

#given a matrix (where_to_lookup), a date reference array and, broken date array with missing date
#function returns a matrix where the first column contains the list of date that broken array miss
#the second column contains the relative weighted variations between T and T-1, the third column contains the T volume
# specified by "position"(4=close price, 5= volume in crypto, 6=volume in pair)
def substitute_finder(broken_array, reference_array, where_to_lookup,position):
    missing_item=Diff(reference_array,broken_array)
    indexed_list=find_index(missing_item, where_to_lookup[:,1])
    weighted_variations=[]
    volumes=[]
    for element in indexed_list[:,1]:
        variation=(where_to_lookup(element,position)-where_to_lookup(element-1,position))/where_to_lookup(element-1,position)
        volume=where_to_lookup(element,6)
        weighted_variation=variation*volume
        weighted_variations.append(weighted_variation)
        volumes.append(volume)
    volumes=np.array(volumes)
    weighted_variations=np.array(weighted_variations)
    variation_matrix=np.column_stack((indexed_list[:,0],weighted_variations))
    volume_matrix=np.column_stack((indexed_list[:,0],volumes))
    return variation_matrix, volume_matrix

# given a list of items (what_to_insert) and the index position that the items should have in where_to_insert
#the function returns where_to_insert updated with the missing items
def insert_items(index_list, what_to_insert, where_to_insert):
    index_list=np.array(index_list)
    what_to_insert=np.array(what_to_insert)
    where_to_insert=np.array(where_to_insert)
    for i,index in enumerate(index_list):
        where_to_insert=np.insert(where_to_insert,index,what_to_insert[i])
    return where_to_insert


#given an index list containing the index value of certain items as founded in where_to_insert
#the function return an array of element in position i-1, searched in where_to_insert
def find_previous(index_list, where_to_insert):
    index_list=np.array(index_list)
    where_to_insert=np.array(where_to_insert)
    previous_list=[]
    for index in index_list:
        previous_list.append(where_to_insert[index-1])
    return np.array(previous_list)




# function takes as input a matrix with missing values referred to specific exchange, crypto and pair
# reference_array is the array of date of the period of interest, info_position can be: 
#(4=close price, 5= volume in crypto, 6=volume in pair)
# based on the info_pos choice the function returns a fixed vector that contain also the values obtained as volume weighted average
# (of close price, volume crypto or volume in pair) of the daily variations of every exchange in the crypto+pair 
def fix_missing(broken_matrix, reference_array, cryptocurrency, exchange, pair, info_position):
    broken_array=broken_matrix[:,0]
    ccy_pair=cryptocurrency+pair
    exchange_list = ['bitflyer', 'bitfinex', 'poloniex', 'bitstamp','bittrex','coinbase-pro','gemini','kraken']#aggungere itbit
    exchange_list.remove(exchange)
    fixing_variation=np.array([])
    fixing_volume=np.array([])
    for elements in exchange_list:
        path_name=os.path.join("C:\\","Users","fcodega","hello")
        file_name=""+elements+"_"+ccy_pair+".json"
        path=os.path.join(path_name, str(file_name))
        json_matrix=np.array(json_to_matrix(path))
        variations, volumes =substitute_finder(broken_array,reference_array,json_matrix, info_position)
        if fixing_variation.size==0:
            fixing_variation=variations[:,1]
            fixing_volume=volumes[:,1]
        else:
            fixing_variation=np.column_stack((fixing_variation,variations[:,1]))
            fixing_volume=np.column_stack((fixing_volume,volumes[:,1]))
    weighted_variation_value=fixing_variation.sum(axis=1)/fixing_volume.sum(axis=1)
    index_list=find_index(variations[:,0],broken_matrix[:,0])
    previous_values=find_previous(index_list[:,1],broken_matrix[:,info_position])
    value_to_insert=weighted_variation_value*previous_values
    fixed_column=insert_items(index_list,value_to_insert,broken_matrix[:,info_position])
    return fixed_column





# for i, element in enumerate(eur):
#     eur[i]=datetime.strptime(str(datetime.fromtimestamp(eur[i]))[:10], '%Y-%m-%d').strftime('%d/%m/%y')


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


def Check_null(item): 
    try:
        return len(item)==0 
    except TypeError: 
        pass 
    return False 
