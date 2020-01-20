#REST API request for coinbase-pro exchange. 
#It requires:
#crypto pair = crypto-fiat (ex: BTC - USD)
#start date = ISO 8601
#end date = ISO 8601
#granularity = in seconds. Ex 86400 = 1 day
#this api give back 300 responses max each request.
import requests
from requests import get
from datetime import datetime
import pandas as pd

date = datetime.strptime( '11-11-2017','%m-%d-%Y')
start_date = date.isoformat()

date2 = datetime.strptime( '11-13-2017','%m-%d-%Y')
end_date = date2.isoformat()

crypto = 'BTC'#, 'XRP', 'LTC', '']
fiat = 'EUR'#], 'GBP', 'USDC', 'USD']

granularity = '86400'


entrypoint = 'https://api.pro.coinbase.com/products/'
key = crypto+'-'+fiat+'/candles?start='+start_date+'&end='+end_date+'&granularity='+granularity
request_url = entrypoint + key

response = requests.get(request_url)
response = response.json()
header = ['time', 'low', 'high', 'open', 'close', 'volume']
dataframe = pd.DataFrame(response, columns = header)