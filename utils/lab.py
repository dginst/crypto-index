from datetime import *
import requests
import time
import os
import pandas as pd
import io
import data_download

entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/' 
resource = 'data'           
flowRef ='EXR'              # Dataflow describing the data that needs to be returned, exchange rates in this case
key = 'D.USD.EUR.SP00.A'  
key2= 'D.GBP.EUR.SP00.A' # Defining the dimension values, explained below
# Key (dimensions) explained:
# the frequency of the measurement: D for daily
# the currency being measured: CHF for Swiss Francs
# the currency against which a currency is being measured: EUR for Euros
# the type of exchange rates: foreign exchange reference rates have code SP00
# the series variation (such as average or standardised measure for given frequency): code A
    # Define the parameters
parameters = {
    'startPeriod': '2019-12-11',  # Start date of the time series
    'endPeriod': '2019-12-11'     # End of the time series
}

# Construct the URL: https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D.CHF.EUR.SP00.A
request_url = entrypoint + resource + '/'+ flowRef + '/' + key
request_url2 = entrypoint + resource + '/'+ flowRef + '/' + key2
# Make the HTTP request


response = requests.get(request_url, params=parameters, headers={'Accept': 'text/csv'})
response2 = requests.get(request_url2, params=parameters, headers={'Accept': 'text/csv'})
# Response succesful? (Response code 200)
print(response)
# Check if the response returns succesfully with response code 200


# Print the full URL
print(response.url)
# Read the response as a file into a Pandas DataFrame
df = pd.read_csv(io.StringIO(response.text))
df2 = pd.read_csv(io.StringIO(response2.text))
# Check the DataFrame's information
# df.info()
# df['OBS_VALUE'].describe()

# Create a new DataFrame called 'ts'
ts = df.filter(['TIME_PERIOD', 'OBS_VALUE','CURRENCY','CURRENCY_DENOM'], axis=1)
ts2 = df2.filter(['TIME_PERIOD', 'OBS_VALUE','CURRENCY','CURRENCY_DENOM'], axis=1)
# 'TIME_PERIOD' was of type 'object' (as seen in df.info). Convert it to datetime first

ts['TIME_PERIOD'] = pd.to_datetime(ts['TIME_PERIOD'])
ts2['TIME_PERIOD'] = pd.to_datetime(ts2['TIME_PERIOD'])
# Set 'TIME_PERIOD' to be the index
ts = ts.set_index('TIME_PERIOD')
ts2 = ts2.set_index('TIME_PERIOD')
# Print the last 5 rows to screen
print(ts)
print(ts2)
mm=ts.append(ts2)
print(mm)
# ts['Ciao']=ts['OBS_VALUE']+4
# ps=[]
# ps=ts
# print(ts)
# print(ps)

r=data_download.ECB_rates_extractor(['USD','GBP','JPY'],'2019-12-11')
print(r)