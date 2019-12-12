from datetime import *
from requests import get
import time
import requests
import os
import pandas as pd
import io
import numpy as np
after = datetime(2019, 5, 26)
end = datetime(2019, 10, 10)

# def date_gen(start_date, end_date = ''):
#     after = start_date
#     if end_date=='':
#         end_date=datetime.today()
#     end= end_date
# 	before = end
# 	step50 = timedelta(days=2000)
# 	while(after < end):
# 		before = after + step50
# 		yield (str(int(time.mktime(after.timetuple()))), str(int(time.mktime(before.timetuple()))))
# 		after = before


# for pair in ['btc']:
# 	currencypair_array = [""+pair+"usd", ""+pair+"eur", ""+pair+"cad", ""+pair+"gbp", ""+pair+"usdt", ""+pair+"usdc", ""+pair+"jpy"]

# exchanges = ['bitflyer','bitfinex']
# d_gen = date_gen()

			
def CW_extractor(exchange_array,currencypair_array,periods):
    d_gen = date_gen()
    result = ""
    periods = "86400"
    i = 1
    for after, before in d_gen:
	    for exchange in exchange_array :
		    for cp in currencypair_array:
			    r = "https://api.cryptowat.ch/markets/"+exchange+"/"+cp+"/ohlc?periods="+periods+"&after="+after+"&before="+before
			    result = get(r).content.decode()
			    f = open(""+exchange+"_"+cp+".json", "w")
			    f.write(result)
			    print("done..." +before)
			    i += 1
    
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