from datetime import *
from requests import get
import time
import requests
import os
import pandas as pd
import io
import numpy as np
import data_setup

# function that creates json files downloading the info from Cryptowatch website
# specifically, given a list of exchange (exchange_array) and a list of currency pair (currencypair_array),	
# the function creates exchange_arrayXcurrencypair json files with the info retrieved from the websites in the
# date range specified by start_date and end_date (end_date default is today())
# the default frequency is daily (86400 seconds)
# start_date ed end_date has to be inserted in MM-DD-YYYY format

# def CW_retrive_json(exchange_array,currencypair_array,start_date, end_date = None, periods='86400'):
#     if "/" in start_date:
#         start_date=start_date.replace("/","-") 
#     start_date = datetime.strptime(start_date, '%m-%d-%Y')
#     if end_date == None:
#         end_date=datetime.now().strftime('%m-%d-%Y')
#     end_date = datetime.strptime(end_date, '%m-%d-%Y')
#     start_date=str(int(time.mktime(start_date.timetuple())))
#     end_date=str(int(time.mktime(end_date.timetuple())))
#     result = ""
#     for exchange in exchange_array :
#         for cp in currencypair_array:
#             request_URL = "https://api.cryptowat.ch/markets/"+exchange+"/"+cp+"/ohlc?periods="+periods+"&after="+start_date+"&before="+end_date
#             result = get(request_URL).content.decode()
#             file_json = open(""+exchange+"_"+cp+".json", "w")
#             file_json.write(result)



# function that retrives data from th Cryptowatch websites and returns a Data Frame with the following
# headers ['Time' ,'Open',	'High',	'Low',	'Close Price',Crypto+ " Volume" , Pair+" Volume"]
# the exchange and currencypair inputs have to be unique value and NOT list
# date range specified by start_date and end_date (end_date default is today())
# the default frequency is daily (86400 seconds)
# start_date ed end_date has to be inserted in MM-DD-YYYY format


def CW_data_reader(exchange, currencypair, start_date, end_date = None, periods='86400'):

    Crypto = currencypair[:3].upper()
    Pair = currencypair[3:].upper()
    
    # check date format
    start_date = data_setup.date_reformat(start_date, '-')
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
    header = ['Time', 'Open', 'High', 'Low', 'Close Price', Crypto + " Volume", Pair + " Volume"]
    
    # do not show unuseful messages
    pd.options.mode.chained_assignment = None
    
    Data_Frame = pd.DataFrame(response['result']['86400'], columns = header)
    
    return Data_Frame 



# function that downloads the exchange rates from the ECB web page and returns a matrix (pd.DataFrame) that indicates:
# on the first column the date, on the second tha exchange rate vakue eutro based, on the third the currency, on the fourth the
# currency of denomination (always 'EUR') and on the fifth the exchange rate USD based (the one of interest)
# key_curr_vector expects a list of currency in International Currency Formatting (ex. USD, GBP, JPY, CAD,...)
# the functions diplays the information better for a single day data retrival, however can works with multiple date
# regarding the other default variables consult the ECB api web page
# Start_Period has to be in YYYY-MM-DD format

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
        response = requests.get(request_url, params = param, headers = {'Accept': 'text/csv'})
        
        # if data is empty, it is an holiday, therefore exit
        try:
            Data_Frame = pd.read_csv(io.StringIO(response.text))
        except:
            break
        
        Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE', 'CURRENCY', 'CURRENCY_DENOM'], axis=1)

        if currency == 'USD':
            cambio_USD_EUR = float(Main_Data_Frame['OBS_VALUE'])

        # 'TIME_PERIOD' was of type 'object' (as seen in Data_Frame.info). Convert it to datetime first
        Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])
        
        # Set 'TIME_PERIOD' to be the index
        Main_Data_Frame = Main_Data_Frame.set_index('TIME_PERIOD')
        
        if Exchange_Rate_List.size == 0:
            Exchange_Rate_List = Main_Data_Frame
            Exchange_Rate_List['USD based rate'] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR
        else:
            Exchange_Rate_List = Exchange_Rate_List.append(Main_Data_Frame, sort=True)
            Exchange_Rate_List['USD based rate'][i] = float(Main_Data_Frame['OBS_VALUE']) / cambio_USD_EUR

    return Exchange_Rate_List


# def itBit_extractor():
