import utils.data_download as dw
from datetime import datetime
import utils.data_setup as ds
import numpy as np
import datetime
import pandas as pd
from datetime import *
import json
import requests



# def date_gen(Start_Date, End_Date, delta):
#     start = datetime.strptime(Start_Date,'%m-%d-%Y') 
#     stop = datetime.strptime(End_Date,'%m-%d-%Y') 
#     delta = timedelta(days=delta)
#     pace = start
#     if Start_Date != End_Date:

#         while (pace < stop):

#             end = pace + delta

#             if end > stop:
#                 end = stop
#             yield (str(pace.isoformat()), str(end.isoformat()))
#             pace = end + timedelta(days = 1)

#     else:
#         yield (str(start.isoformat()), str(stop.isoformat()))

# for pace, end in date_gen('01-01-2019','01-01-2019',49):
#     print (pace)
#     print(end)

a= dw.CW_data_reader('bitfinex','btcusd','01-01-2020')
header = ['Close Price', "Crypto Volume", "Pair Volume"]
start = 1577923200 
stop = 1578528000
b = a[header][a['Time'].between(start, stop, inclusive = True)]
c= b.sum()
d = c.sum()
e= c/c.sum()
f= np.array(e)
print(a)
print(b)
print(c)
print(c/c.sum())
print(f)
print(d)
head = ['Time']
head.extend(header)
print(head)
# c=1578096000
# d = a[a['Time'] == c]['Close Price']
# print(float(d))
# f = a[a['Time'] == c].iloc[:,1:4]
# print(f)
# g= f*f
# print(g)
# t = np.column_stack((c,g))
# print(t)
# t = pd.DataFrame(t, columns = header 
# )
# a = a.append(t)
# a = a.sort_values(by=['Time'])
# a = a.reset_index(drop=True)
# a['ciao'] = np.zeros(20)
# print(a)
# today = datetime.now().strftime('%Y-%m-%d')
# key= ['USD', 'GBP', 'CAD', 'JPY']
# rates = ds.ECB_setup(key, '2020-01-15', today, timeST='Y')
# print(rates)
# file_json = open(""+today+"_"".json", "w")
# file_json.write(rates.to_json())
# print(file_json)
# a = json.loads(r'C:\Projects\dginst\crypto-index\2020-01-22_.json')
# # df = pd.io.json.json_normalize(file_json)
# # t= pd.read_json('2020-01-22_.json')
# # print(t)

# entrypoint = 'https://api.itbit.com/v1/markets/XBTUSD/ticker' 
# key = exchange + "/" + currencypair + "/ohlc?periods=" + periods + "&after=" + start_date + "&before=" + end_date
# request_url = entrypoint + key
    
# # API call
# response = requests.get(request_url)
# response = response.json()
# #header = ['Time', 'Open', 'High', 'Low', 'Close Price', Crypto + " Volume", Pair + " Volume"]
# header = ['Time', 'Open', 'High', 'Low', 'Close Price', "Crypto Volume", "Pair Volume"]
# # do not show unuseful messages
# pd.options.mode.chained_assignment = None
