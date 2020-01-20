#REST API request for coinbase-pro exchange. 
#It requires:
#crypto pair = crypto-fiat (ex: BTC - USD)
#start date = ISO 8601
#end date = ISO 8601
#granularity = in seconds. Ex 86400 = 1 day
#this api gives back 300 responses max for each request.

import requests
from requests import get
from datetime import datetime
import pandas as pd
import numpy as np

def date_gen():
    start = datetime(2016, 1, 1)
    end = datetime(2019, 12, 1)
    pace = end
    delta = timedelta(days=50)
    while(start < end):
        pace = start + delta
        yield (str(start.isoformat()), str(pace.isoformat()))
        start = pace
        
d_gen = date_gen()
#print(d_gen)
crypto = 'BTC'#, 'XRP', 'LTC', '']
fiat = 'USD'#], 'GBP', 'USDC', 'USD']

granularity = '86400'

alo = np.array([])
ciao = np.array([])

for start,pace in d_gen:
    print(start)
    print(pace)
    entrypoint = 'https://api.pro.coinbase.com/products/'
    key = crypto+'-'+fiat+'/candles?start='+start+'&end='+pace+'&granularity='+granularity
    request_url = entrypoint + key
    response = requests.get(request_url)
    response= response.json()
    
header = ['Time', 'low', 'high', 'open', 'Close Price', 'Crypto Volume']
dataframe = pd.DataFrame(alo, columns = header)
dataframe = dataframe.drop(columns = ['open', 'high', 'low'])