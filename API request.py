#####################################################################################################
################################ COINBASE - PRO #####################################################
#####################################################################################################
#REST API request for coinbase-pro exchange. 
#It requires:
#crypto pair = crypto-fiat (ex: BTC - USD)
#start date = ISO 8601
#end date = ISO 8601
#granularity = in seconds. Ex 86400 = 1 day
#this api gives back 300 responses max for each request.
##for the moment it is just downloading one cryptocurr.

import requests
from requests import get
from datetime import *
import pandas as pd
import numpy as np
from time import sleep

def date_gen():
    start = datetime(2016, 1, 1)
    stop = datetime(2019, 12, 1)
    pace = stop
    delta = timedelta(days=50)
    while(start < stop):
        end = start + delta
        yield (str(start.isoformat()), str(end.isoformat()))
        start = end
        
d_gen = date_gen()
#print(d_gen)
crypto = 'BTC'#, 'XRP', 'LTC', '']
fiat = 'USD'#], 'GBP', 'USDC', 'USD']

granularity = '86400'

arr = np.array([])
header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']

for start,pace in d_gen:
    print(start)
    print(pace)
    entrypoint = 'https://api.pro.coinbase.com/products/'
    key = crypto+'-'+fiat+'/candles?start='+start+'&end='+pace+'&granularity='+granularity
    request_url = entrypoint + key
    response = requests.get(request_url)
    response= response.json()
    response = np.array(response)
    print(response)

    if arr.size == 0 :
        dataframe = pd.DataFrame(response, columns = header)
        arr = np.append(arr,response)
    else:
        dataframe = dataframe.append(pd.DataFrame(response, columns = header)) 
    
    # coinbase-pro allows 4 calls per seconds maximum so we use a sleep 
    sleep(0.25)

Coinbase = dataframe.drop(columns = ['open', 'high', 'low'])          
    
