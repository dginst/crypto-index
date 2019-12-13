from datetime import *
from requests import get
import time
import requests
import os
import pandas as pd
import io
import numpy as np

# function that creates json files downloading the info from Cryptowatch website
# specifically, given a list of exchange (exchange_array) and a list of currency pair (currencypair_array),	
# the function creates exchange_arrayXcurrencypair json files with the info retrieved from the websites in the
# date range specified by start_date and end_date (end_date default is today())
# the default frequency is daily (86400 seconds)
# start_date ed end_date has to be inserted in MM-DD-YYYY format		
def CW_retrive_json(exchange_array,currencypair_array,start_date, end_date = None, periods='86400'):
    if "/" in start_date:
        start_date=start_date.replace("/","-") 
    start_date = datetime.strptime(start_date, '%m-%d-%Y')
    if end_date == None:
        end_date=datetime.now().strftime('%m-%d-%Y')
    end_date = datetime.strptime(end_date, '%m-%d-%Y')
    start_date=str(int(time.mktime(start_date.timetuple())))
    end_date=str(int(time.mktime(end_date.timetuple())))
    result = ""
    for exchange in exchange_array :
        for cp in currencypair_array:
            request_URL = "https://api.cryptowat.ch/markets/"+exchange+"/"+cp+"/ohlc?periods="+periods+"&after="+start_date+"&before="+end_date
            result = get(request_URL).content.decode()
            file_json = open(""+exchange+"_"+cp+".json", "w")
            file_json.write(result)

# function that retrives data from th Cryptowatch websites and returns a Data Frame with the following
# headers ['Time' ,'Open',	'High',	'Low',	'Close Price',Crypto+ " Volume" , Pair+" Volume"]
# the exchange and currencypair inputs have to be unique value and NOT list
# date range specified by start_date and end_date (end_date default is today())
# the default frequency is daily (86400 seconds)
# start_date ed end_date has to be inserted in MM-DD-YYYY format
def CW_data_reader(exchange,currencypair,start_date, end_date = None, periods='86400'):
    Crypto=currencypair[:3].upper()
    Pair=currencypair[3:].upper()
    if "/" in start_date:
        start_date=start_date.replace("/","-") 
    start_date = datetime.strptime(start_date, '%m-%d-%Y')
    if end_date == None:
        end_date=datetime.now().strftime('%m-%d-%Y')
    end_date = datetime.strptime(end_date, '%m-%d-%Y')
    start_date=str(int(time.mktime(start_date.timetuple())))
    end_date=str(int(time.mktime(end_date.timetuple())))
    entrypoint = 'https://api.cryptowat.ch/markets/' 
    key=exchange+"/"+currencypair+"/ohlc?periods="+periods+"&after="+start_date+"&before="+end_date
    pd.options.mode.chained_assignment = None
    request_url = entrypoint + key
    response = requests.get(request_url)
    response= response.json()
    header=['Time' ,'Open',	'High',	'Low',	'Close Price',Crypto+" Volume" , Pair+" Volume"]
    Data_Frame=pd.DataFrame(response['result']['86400'],columns=header)
    return Data_Frame 

#function that downloads the exchange rates from the ECB web page and returns a matrix (pd.DataFrame) that indicates:
#on the first column the date, on the second tha exchange rate vakue eutro based, on the third the currency, on the fourth the
#currency of denomination (always 'EUR') and on the fifth the exchange rate USD based (the one of interest)
# key_currency_vector expects a list of currency in International Currency Formatting (ex. USD, GBP, JPY, CAD,...)
# the functions diplays the information better for a single day data retrival, however can works with multiple date
# regarding the other default variables consult the ECB api web page
def ECB_rates_extractor(key_currency_vector,Start_Period,End_Period=None,frequency='D', currency_denominat='EUR',typeRates='SP00',series_variation='A'):
    if End_Period == None:
        End_Period=Start_Period
    entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
    resource = 'data'           
    flowRef ='EXR'
    parameters = {
        'startPeriod': Start_Period, 
        'endPeriod': End_Period    
    }
    Exchange_Rate_List=pd.DataFrame()
    pd.options.mode.chained_assignment = None
    for i,currency in enumerate(key_currency_vector):
        key= frequency+'.'+currency+'.'+currency_denominat+'.'+typeRates+'.'+series_variation
        request_url = entrypoint + resource + '/'+ flowRef + '/' + key
        response = requests.get(request_url, params=parameters, headers={'Accept': 'text/csv'})
        Data_Frame = pd.read_csv(io.StringIO(response.text))
        Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE','CURRENCY','CURRENCY_DENOM'], axis=1)
        if currency == 'USD':
            cambio_USD_EUR=float(Main_Data_Frame['OBS_VALUE'])
        # 'TIME_PERIOD' was of type 'object' (as seen in Data_Frame.info). Convert it to datetime first
        Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])
        # Set 'TIME_PERIOD' to be the index
        Main_Data_Frame = Main_Data_Frame.set_index('TIME_PERIOD')
        if Exchange_Rate_List.size==0:
            Exchange_Rate_List=Main_Data_Frame
            Exchange_Rate_List['USD based rate']=float(Main_Data_Frame['OBS_VALUE'])/cambio_USD_EUR
        else:
            Exchange_Rate_List=Exchange_Rate_List.append(Main_Data_Frame, sort=True)
            Exchange_Rate_List['USD based rate'][i]=float(Main_Data_Frame['OBS_VALUE'])/cambio_USD_EUR
        return Exchange_Rate_List


# def itBit_extractor():
