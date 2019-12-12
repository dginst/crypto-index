import pandas as pd
import numpy as np
import json
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


#return a list containing the elements of list_1 non included in list_2
def Diff(list_1, list_2): 
    return (list(set(list_1) - set(list_2))) 

#return a sorted array of the size of reference_array.
# if there are more element in ref array, small_arr is filled with the missing elements
# small_arr HAS TO BE smaller than reference vector
# default sorting is in ascending way, if descending is needed specify versus='desc'
def fill_time_array(small_arr, reference_array,versus='asc'):
    difference=Diff(reference_array,small_arr)
    for element in difference:
        small_arr.add(element)
    small_arr=list(small_arr)
    if versus=='desc':
        small_arr.sort(reverse=True)
    else:
        small_arr.sort()
    return small_arr




#function takes a .json file from Cryptowatch API and transforms it into a matrix
# the matrix has the headers : ['Time' ,'Open',	'High',	'Low',	'Close',""+Crypto+" Volume" , ""+Pair+" Volume"]
#if the downloaded file does not have results the function returns an empty array
#note that the "time" column contains value in timestamp format
def json_to_matrix(file_path, Crypto='',Pair=''):
    raw_json=pd.read_json(file_path)
    if Crypto == '':
        Crypto="Crypto"
    if Pair=='':
        Pair="Pair"
    header=['Time' ,'Open',	'High',	'Low',	'Close',""+Crypto+" Volume" , ""+Pair+" Volume"]
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