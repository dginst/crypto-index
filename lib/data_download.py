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
    

# Key (dimensions) explained:
# the frequency of the measurement: D for daily
# the currency being measured: CHF for Swiss Francs
# the currency against which a currency is being measured: EUR for Euros
# the type of exchange rates: foreign exchange reference rates have code SP00
# the series variation (such as average or standardised measure for given frequency): code A

def ECB_rates_extractor(key_currency_vector,Start_Period,End_Period=None,frequency='D', currency_denominat='EUR',typeRates='SP00',series_variation='A'):
    if End_Period == None:
        End_Period=Start_Period
    entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
    resource = 'data'           
    flowRef ='EXR'              # Dataflow describing the data that needs to be returned, exchange rates in this case
    parameters = {
        'startPeriod': Start_Period,  # Start date of the time series
        'endPeriod': End_Period    # End of the time series
    }
    Exchange_Rate_List=pd.DataFrame()
    for currency in key_currency_vector:
        key= frequency+'.'+currency+'.'+currency_denominat+'.'+typeRates+'.'+series_variation
        request_url = entrypoint + resource + '/'+ flowRef + '/' + key
        response = requests.get(request_url, params=parameters, headers={'Accept': 'text/csv'})
        Data_Frame = pd.read_csv(io.StringIO(response.text))
        Main_Data_Frame = Data_Frame.filter(['TIME_PERIOD', 'OBS_VALUE','CURRENCY','CURRENCY_DENOM'], axis=1)
        # 'TIME_PERIOD' was of type 'object' (as seen in Data_Frame.info). Convert it to datetime first
        Main_Data_Frame['TIME_PERIOD'] = pd.to_datetime(Main_Data_Frame['TIME_PERIOD'])
        # Set 'TIME_PERIOD' to be the index
        Main_Data_Frame = Main_Data_Frame.set_index('TIME_PERIOD')
        if Exchange_Rate_List.size == []:
            Exchange_Rate_List=Main_Data_Frame
        else:
            Exchange_Rate_List.append(Main_Data_Frame,ignore_index=True)
    return Exchange_Rate_List